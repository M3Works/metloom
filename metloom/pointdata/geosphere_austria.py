from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import logging

from geopandas import GeoDataFrame

from .base import PointData
from ..variables import CdecStationVariables, SensorDescription
from ..dataframe_utils import append_df, merge_df, resample_whole_df

LOG = logging.getLogger("metloom.pointdata.geosphere_austria")

M_TO_FT = 3.28084


class GeoSphere(PointData):
    """
    Implement PointData methods for GeoSphere Austria data source
    API documentation here
    https://dataset.api.hub.geosphere.at/v1/docs/index.html
    https://dataset.api.hub.geosphere.at/v1/docs/user-guide/resource.html

    Datasets available here https://data.hub.geosphere.at/dataset/

    We could either use the verified klima-v1 data or the
    raw tawes-v1 data. Kilma has hourly and daily, taws is 10minute
    and most current
    """

    ALLOWED_VARIABLES = CdecStationVariables
    URL = "https://dataset.api.hub.geosphere.at"
    DATASOURCE = "GEOSPHERE"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(GeoSphere, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None
        self._tzinfo = None

    @classmethod
    def _retrieve_all_metadata(cls):
        """
        Get the metadata we can search through for stations. The assumption
        is that we ONLY WANT TAWES stations
        """
        url = cls.URL + "/v1/station/current/tawes-v1-10min/metadata"
        # url = cls.URL + "/v1/station/historical/klima-v1-1d/metadata"
        resp = requests.get(url)
        resp.raise_for_status()
        # obj = resp.json()["parameters"]  # variables
        obj = resp.json()["stations"]
        df = pd.DataFrame.from_dict(obj)
        return df

    def _get_all_metadata(self):
        """
        Get all the raw metadata for a station. This is a list of sensor
        descriptions for the station
        Returns:
            A list of dictionaries describing the sensors at a station
        """
        if self._raw_metadata is None:
            all_meta = self._retrieve_all_metadata()
            meta_df = all_meta[all_meta["id"] == self.id]
            if len(meta_df) == 0:
                raise RuntimeError(f"No matching metadata for {self.id}")
            self._raw_metadata = meta_df.to_dict(orient="rows")[0]
        return self._raw_metadata

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        data = self._get_all_metadata()

        # TODO: gridded coords are EPSG:4325, are these also?
        return gpd.points_from_xy(
            [data["lon"]],
            [data["lat"]],
            # Convert elevation to feet
            z=[data["altitude"] * M_TO_FT],
        )[0]

    def _data_request(self, params):
        """
        https://dataset.api.hub.geosphere.at/v1/station/historical/tawes-v1-10min?parameters=TL&station_ids=11035&start=2021-01-01T00%3A00%3A00&end=2021-01-01T02%3A00%3A00
        Make get request and return JSON
        Args:
            params: dictionary of request parameters
        Returns:
            dictionary of response values
        """
        url = self.URL + "/v1/station/historical/tawes-v1-10min"
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _handle_df_tz(self, val):
        """
        Covert one entry from a df from cls.TZINFO to UTC
        """
        if pd.isna(val):
            return val
        else:
            return val.tz_convert(self.desired_tzinfo)

    def _sensor_response_to_df(self, response_data, sensor, final_columns,
                               resample_duration=None):
        """
        Convert the response data from the API to a GeoDataFrame
        Format and map columns in the dataframe
        Args:
            response_data: JSON list response from CDEC API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
            resample_duration: duration to resample to
        Returns:
            GeoDataFrame
        """
        dt_values = response_data["timestamps"]
        params = response_data["features"][0]["properties"]["parameters"]
        values = params[sensor.code]["data"]
        unit = params[sensor.code]["unit"]
        # Build the dataframe
        sensor_df = gpd.GeoDataFrame.from_dict(
            {
                "datetime": dt_values,
                sensor.name: values,
                f"{sensor.name}_units":  [unit] * len(values),
                "site": [self.id] * len(values)
            },
            geometry=[self.metadata] * len(values),
        )
        sensor_df.loc[sensor_df[sensor.name] == None] = np.nan

        final_columns += [sensor.name, f"{sensor.name}_units"]
        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])

        # resample if necessary
        if resample_duration:
            sensor_df = resample_whole_df(
                sensor_df.set_index("datetime"), sensor,
                interval=resample_duration
            ).reset_index()
            sensor_df = GeoDataFrame(sensor_df, geometry=sensor_df["geometry"])

        sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)
        # set index so joining works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        sensor_df = sensor_df.loc[pd.notna(sensor_df[sensor.name])]
        return sensor_df

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        desired_duration: str,
    ):
        """
        https://dataset.api.hub.geosphere.at/v1/station/current/tawes-v1-10min?parameters=TL&station_ids=11035


        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            desired_duration: duration code ['D', 'H', 'E']
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """
        # TODO: Make this fallback for hourly
        params = {
            "parameters": ",".join([v.code for v in variables]),
            "station_ids": self.id,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        }
        df = None
        final_columns = ["geometry", "site"]
        response_data = self._data_request(params)

        if response_data:
            for sensor in variables:
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns,
                    resample_duration=desired_duration
                )
                df = merge_df(df, sensor_df)

        if df is not None:
            if len(df.index) > 0:
                # Set the datasource
                df["datasource"] = [self.DATASOURCE] * len(df.index)
                df.reset_index(inplace=True)
                df.set_index(keys=["datetime", "site"], inplace=True)
                df.index.set_names(["datetime", "site"], inplace=True)
            else:
                df = None
        self.validate_sensor_df(df)
        return df

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data
        Example query:
        https://dataset.api.hub.geosphere.at/v1/station/current/
        tawes-v1-10min?parameters=TL&station_ids=11035
        """
        return self._get_data(start_date, end_date, variables, "D")

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        return self._get_data(start_date, end_date, variables, "H")

    @staticmethod
    def _station_sensor_search(
        bounds, sensor: SensorDescription, dur=None, collect=None,
        buffer=0.0
    ):
        """
        Station search form https://cdec.water.ca.gov/dynamicapp/staSearch?
        Search for stations using the CDEC station search utility
        Args:
            bounds: dictionary of Longitude and Latitidue bounds with keys
                minx, maxx, miny, maxy
            sensor: SensorDescription object
            dur: optional CDEC duration code ['M', 'H', 'D']
            collect: optional CDEC collection type string i.e. 'MANUAL+ENTRY'
        Returns:
            Pandas Dataframe of table result or None if no table found

        """
        pass

    @classmethod
    def points_from_geometry(
        cls,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        **kwargs
    ):
        """
        See docstring for PointData.points_from_geometry

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription
            snow_courses: Boolean for including only snowcourse data or no
            snowcourse data
            within_geometry: filter the points to within the shapefile
            instead of just the extents. Default True
            buffer: buffer added to search box

        Returns:
            PointDataCollection
        """
        # assign defaults
        kwargs = cls._add_default_kwargs(kwargs)

        # Assume station search result is in 4326
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        search_df = None
        station_search_kwargs = {}

        # Filter to manual, monthly measurements if looking for snow courses
        for variable in variables:
            result_df = cls._station_sensor_search(
                bounds, variable, buffer=kwargs["buffer"],
                **station_search_kwargs
            )
            if result_df is not None:
                result_df["index_id"] = result_df["ID"]
                result_df.set_index("index_id", inplace=True)
                search_df = append_df(
                    search_df, result_df
                ).drop_duplicates(subset=['ID'])
        # return empty collection if we didn't find any points
        if search_df is None:
            return cls.ITERATOR_CLASS([])
        clms = search_df.columns.values
        if "ElevationFeet" in clms:
            elev_key = "ElevationFeet"
        elif "Elevation Feet" in clms:
            elev_key = "Elevation Feet"
        else:
            raise RuntimeError("No key for elevation")
        gdf = gpd.GeoDataFrame(
            search_df,
            geometry=gpd.points_from_xy(
                search_df["Longitude"],
                search_df["Latitude"],
                z=search_df[elev_key],
            ),
        )
        # filter to points within shapefile
        if kwargs['within_geometry']:
            filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        else:
            filtered_gdf = gdf

        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf.index,
                filtered_gdf["Station Name"],
                filtered_gdf["geometry"],
            )
        ]
        return cls.ITERATOR_CLASS(points)

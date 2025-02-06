from datetime import datetime, date
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import logging

from geopandas import GeoDataFrame

from .base import PointData
from ..variables import (
    GeoSphereCurrentVariables, SensorDescription, GeoSphereHistVariables
)
from ..dataframe_utils import merge_df, resample_whole_df, \
    shp_to_box

LOG = logging.getLogger("metloom.pointdata.geosphere_austria")

M_TO_FT = 3.28084


class GeoSpherePointDataBase(PointData):
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

    ALLOWED_VARIABLES = None
    URL = "https://dataset.api.hub.geosphere.at"
    DATASOURCE = "GEOSPHERE"
    META_EXTENSION = None

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(GeoSpherePointDataBase, self).__init__(
            station_id, name, metadata=metadata
        )
        self._raw_metadata = None
        self._tzinfo = None

    @classmethod
    def _retrieve_all_metadata(cls):
        """
        Get the metadata we can search through for stations. The assumption
        is that we ONLY WANT TAWES stations

        The endpoint returns a json object with both `parameters` and
        `stations`. Parameters maps to ALL VARIABLES and stations
        maps to ALL STATIONS
        """
        url = cls.URL + cls.META_EXTENSION
        resp = requests.get(url)
        resp.raise_for_status()
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
            self._raw_metadata = meta_df.to_dict(orient="records")[0]
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
        Make get request and return JSON
        Args:
            params: dictionary of request parameters
        Returns:
            dictionary of response values
        """
        raise NotImplementedError("Need to implement")

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
            response_data: JSON list response from API
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
        sensor_df = pd.DataFrame.from_dict(
            {
                "datetime": dt_values,
                sensor.name: values,
                f"{sensor.name}_units": [unit] * len(values),
                "site": [self.id] * len(values)
            }
        )
        if all(pd.isna(sensor_df[sensor.name].values)):
            return None
        sensor_df.loc[pd.isna(sensor_df[sensor.name])] = np.nan
        sensor_df = gpd.GeoDataFrame(
            sensor_df,
            geometry=[self.metadata] * len(values),
        )

        final_columns = [sensor.name, f"{sensor.name}_units"] + final_columns
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

        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            desired_duration: duration code ['D', 'h']
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """
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

    @classmethod
    def points_from_geometry(
        cls,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        **kwargs
    ):
        """
        See docstring for PointData.points_from_geometry

        The Austria Geosphere API does not allow filtering by variable.
        As a result, we do not filter according to which points have specific
        variables. The function arguments allow variables to be passed in
        to keep consistency with the same function from other classes.

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription. NOT USED FOR THIS CLASS
            within_geometry: filter the points to within the shapefile
            instead of just the extents. Default True
            buffer: buffer added to search box,
            filter_to_active: filter to active stations

        Returns:
            PointDataCollection
        """
        # assign defaults
        kwargs = cls._add_default_kwargs(kwargs)

        # Assume station search result is in 4326
        projected_geom = geometry.to_crs("EPSG:4326")

        # add buffer to geometry
        search_geom = projected_geom.buffer(kwargs["buffer"])

        # get metadata for all stations
        all_meta = cls._retrieve_all_metadata()

        # return empty collection if we didn't find any points
        if all_meta is None:
            return cls.ITERATOR_CLASS([])

        # convert to a geodataframe
        gdf = gpd.GeoDataFrame(
            all_meta,
            geometry=gpd.points_from_xy(
                all_meta["lon"], all_meta["lat"],
                all_meta["altitude"] * M_TO_FT
            )
        )
        # TODO: is this correct?
        gdf = gdf.set_crs("EPSG:4326")

        # filter to points within shapefile
        if kwargs['within_geometry']:
            filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        # filter to the overall bounding box
        else:
            box_df = shp_to_box(search_geom)
            filtered_gdf = gdf[gdf.within(box_df.iloc[0]["geometry"])]

        # filter to active stations
        if kwargs["filter_to_active"]:
            filtered_gdf = filtered_gdf.loc[filtered_gdf["is_active"] == "true"]

        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf["id"],
                filtered_gdf["name"],
                filtered_gdf["geometry"],
            )
        ]
        return cls.ITERATOR_CLASS(points)


class GeoSphereCurrentPointData(GeoSpherePointDataBase):
    """
    Implement PointData methods for GeoSphere Austria data source
    API documentation here
    https://dataset.api.hub.geosphere.at/v1/docs/index.html
    https://dataset.api.hub.geosphere.at/v1/docs/user-guide/resource.html

    Datasets available here https://data.hub.geosphere.at/dataset/

    We use tawes-v1 data which consists of data from the last 3 months
    in 10 minute increment
    """

    ALLOWED_VARIABLES = GeoSphereCurrentVariables
    URL = "https://dataset.api.hub.geosphere.at"
    DATASOURCE = "GEOSPHERE"
    META_EXTENSION = "/v1/station/current/tawes-v1-10min/metadata"

    def _data_request(self, params):
        """
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

    @staticmethod
    def _back_3_months(dt):
        start_month = dt.month - 3
        if start_month <= 0:
            data_valid_start = dt.replace(
                month=start_month + 12, year=dt.year - 1
            )
        else:
            data_valid_start = dt.replace(month=start_month)
        return data_valid_start

    def _validate_dates(self, end_date):
        """
        Validate that the dates will work

        Args:
            end_date: datetime object for the end of the request
        """
        data_valid_start = self._back_3_months(date.today())
        if pd.to_datetime(end_date) < pd.to_datetime(data_valid_start):
            raise ValueError(
                f"This datasource does not have data older than 3 months. We "
                f"cannot fetch data for dates before"
                f" {data_valid_start.isoformat()}"
            )

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
        self._validate_dates(end_date)
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
        self._validate_dates(end_date)
        return self._get_data(start_date, end_date, variables, "h")


class GeoSphereHistPointData(GeoSpherePointDataBase):
    """
    Implement PointData methods for GeoSphere Austria data source
    API documentation here
    https://dataset.api.hub.geosphere.at/v1/docs/index.html
    https://dataset.api.hub.geosphere.at/v1/docs/user-guide/resource.html

    Datasets available here https://data.hub.geosphere.at/dataset/

    We use klima-v1-1d data which consists of historical daily data.
    There is historical hourly data, but the parameter names are different
    and as such this has not bee implemented
    """

    ALLOWED_VARIABLES = GeoSphereHistVariables
    URL = "https://dataset.api.hub.geosphere.at"
    DATASOURCE = "GEOSPHERE"
    META_EXTENSION = "/v1/station/historical/klima-v1-1d/metadata"

    def _data_request(self, params):
        """
        Make get request and return JSON
        Args:
            params: dictionary of request parameters
        Returns:
            dictionary of response values
        """
        url = self.URL + "/v1/station/historical/klima-v1-1d"
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data
        Example query:
        https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v1-1d
        ?station_ids=11401&start=2023-04-12&end=2023-04-14&parameters=schnee
        """
        return self._get_data(start_date, end_date, variables, None)

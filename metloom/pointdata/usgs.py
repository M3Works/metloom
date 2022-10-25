from datetime import datetime, timezone, timedelta
from typing import List
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import logging
from io import StringIO

from .base import PointData
from ..variables import USGSVariables, SensorDescription
from ..dataframe_utils import merge_df, append_df

LOG = logging.getLogger(__name__)


class USGSPointData(PointData):
    """
    Implement PointData methods for USGS data source.

    APIs and help
        https://waterservices.usgs.gov/rest/DV-Service.html#Service
        https://waterservices.usgs.gov/rest/IV-Service.html#Service
        https://waterservices.usgs.gov/rest/Site-Test-Tool.html

    """

    ALLOWED_VARIABLES = USGSVariables
    USGS_URL = "https://waterservices.usgs.gov/nwis/"
    META_URL = USGS_URL + "site/"
    DATASOURCE = "USGS"

    def __init__(self, station_id, name, metadata=None, duration=None):
        """
        See docstring for PointData.__init__
        """
        super(USGSPointData, self).__init__(station_id, name, metadata=metadata)
        self._tzinfo = None
        self._raw_metadata = None
        self.duration = duration

    def _get_all_metadata(self):
        if self._raw_metadata is None:
            params = {
                "format": "rdb",
                "sites": self.id,
                "siteOutput": "expanded",
                "siteStatus": "all"
            }
            resp = requests.get(self.META_URL, params=params)
            resp.raise_for_status()
            data = resp.text
            try:
                df = pd.read_csv(
                    StringIO(data), delimiter="\t", skip_blank_lines=True,
                    comment="#"
                )
            except ValueError:
                LOG.error("Could not convert data to dataFrame")
                return None
            df.drop(df[df['agency_cd'] != "USGS"].index, inplace=True)
            self._raw_metadata = df

        return self._raw_metadata

    def _get_metadata(self):
        """
        Get metadata
        """
        raw_meta = self._get_all_metadata()
        data = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(
                raw_meta["dec_long_va"],
                raw_meta["dec_lat_va"],
                z=raw_meta["alt_va"],
            )
        )
        data = data.set_crs("EPSG:4269").to_crs("EPSG:4326")
        return data.iloc[0]["geometry"]

    @property
    def tzinfo(self):
        if self._tzinfo is None:
            self._tzinfo = self._get_tzinfo()
        return self._tzinfo

    def _get_tzinfo(self):
        raw_meta = self._get_all_metadata()
        tz_abbrev = raw_meta.iloc[0]["tz_cd"]
        tz_map = {
            "PST": timedelta(hours=-8.0),
            "PDT": timedelta(hours=-7.0),
            "MST": timedelta(hours=-7.0),
            "MDT": timedelta(hours=-6.0),
            "CST": timedelta(hours=-6.0),
            "CDT": timedelta(hours=-5.0),
            "EST": timedelta(hours=-5.0),
            "EDT": timedelta(hours=-4.0),
        }
        default = timedelta(hours=0)
        return timezone(tz_map.get(tz_abbrev, default))

    def _data_request(self, params, duration="dv"):
        """
        Make request to USGS and return JSON

        Args:
            params: dictionary of request parameters
            duration: daily ("dv") or instantaneous ("iv") values
        Returns:
            data: dict of response values
        """

        data = []
        resp = requests.get(self.USGS_URL + duration + "/", params=params)
        resp.raise_for_status()
        resp = resp.json()
        contains_data = True

        if "value" not in resp:
            LOG.warning(" Empty response from request")
            contains_data = False

        if len(resp["value"]["timeSeries"]) < 1:
            LOG.warning(f" No data for site {self.id} with given parameters")
            contains_data = False

        if contains_data:
            data = resp["value"]["timeSeries"][0]

        return data

    def _sensor_response_to_df(
        self, response_data, sensor, final_columns, site_id
    ):
        """
        Convert the response data from the API to a GeoDataFrame Format and map columns
        in the dataframe

        Args:
            response_data: JSON list response from API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
            site_id: site id
        Returns:
            GeoDataFrame
        """

        if "values" not in response_data:
            LOG.warning(" Response does not contain expected data")
            raise ValueError("Failed parsing response for data")

        no_data_value = response_data["variable"]["noDataValue"]
        units = response_data["variable"]["unit"]["unitCode"]

        sensor_df = gpd.GeoDataFrame.from_dict(
            response_data["values"][0]["value"],
            geometry=[self.metadata] * len(response_data["values"][0]["value"]),
        )

        sensor_df.replace(no_data_value, np.nan, inplace=True)
        sensor_df["site"] = site_id
        sensor_df[f"{sensor.name}_units"] = units

        final_columns += [sensor.name, f"{sensor.name}_units"]
        column_map = {"dateTime": "datetime", "value": sensor.name}
        sensor_df.rename(columns=column_map, inplace=True)
        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])

        if sensor_df["datetime"][0].tzinfo is None:
            sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)

        # set index so joining works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df.index = sensor_df.index.tz_convert(self.desired_tzinfo)
        sensor_df = sensor_df.filter(final_columns)
        sensor_df = sensor_df.loc[pd.notna(sensor_df[sensor.name])]
        return sensor_df

    def _get_data_fallback(self, params, duration):
        """
        Fallback for different sample times than requested not yet implemented.

        Args:
            params: request params with or without dur_code
            duration: daily ("dv") or instantaneous ("iv")
        Returns:
            response_data: dict of url response
            duration: daily ("dv") or instantaneous ("iv")
        """
        response_data = self._data_request(params, duration)
        return response_data, duration

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        duration: str
    ):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            duration: USGS duration code, "dv" or "iv"
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """

        params = {
            'startDT': start_date.date().isoformat(),
            'endDT': end_date.date().isoformat(),
            'sites': self.id,
            'format': 'json',
            'siteType': 'ST',
            'siteStatus': 'all'
        }

        df = None
        final_columns = ["geometry", "site"]

        for sensor in variables:
            params["parameterCd"] = sensor.code
            response_data, response_duration = self._get_data_fallback(
                params, duration
            )
            if response_data:
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns, self.id
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
        """
        return self._get_data(start_date, end_date, variables, "dv")

    def get_instantaneous_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        USGS 'instantaneous' data, which is generally 15 minutes.
        """
        return self._get_data(start_date, end_date, variables, "iv")

    @staticmethod
    def _station_sensor_search(
        url, bounds, sensor: SensorDescription, dur="dv", buffer=0.0
    ):
        """
        Search for USGS stations within a bounding box for the given sensor description.

        Args:
            url: base url for metadata
            bounds: dictionary of lat/long bounds with keys minx, miny, maxx, maxy
            sensor: SensorDescription object
            dur: during (currently only supporting daily values "dv")
            buffer: buffer the bounding box
        """
        bounds = bounds.round(decimals=5)

        params = {
            "format": "rdb",
            "bBox": rf"{bounds['minx'] - buffer},{bounds['miny'] - buffer},"
                    rf"{bounds['maxx'] + buffer},{bounds['maxy'] + buffer}",
            "siteStatus": "active",
            "hasDataTypeCd": dur,
            "parameterCd": sensor.code
        }

        resp = requests.get(url, params)
        if resp.status_code == 404:
            LOG.warning(
                "No sites matching request withing given points, try changing "
                "parameter or adding buffer"
            )
        resp.raise_for_status()
        data = resp.text

        try:
            df = pd.read_csv(
                StringIO(data), delimiter="\t", skip_blank_lines=True, comment="#"
            )
        except ValueError:
            LOG.error("Could not convert data to dataFrame")
            return None

        df.drop(df[df['agency_cd'] != "USGS"].index, inplace=True)

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

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription
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

        for variable in variables:
            result_df = cls._station_sensor_search(
                cls.META_URL, bounds, variable, buffer=kwargs["buffer"],
                **station_search_kwargs
            )
            if result_df is not None:
                result_df["index_id"] = result_df["site_no"]
                result_df.set_index("index_id", inplace=True)
                search_df = append_df(
                    search_df, result_df
                ).drop_duplicates(subset=['site_no'])

        # return empty collection if we didn't find any points
        if search_df is None:
            return cls.ITERATOR_CLASS([])
        gdf = gpd.GeoDataFrame(
            search_df,
            geometry=gpd.points_from_xy(
                search_df["dec_long_va"],
                search_df["dec_lat_va"],
                z=search_df["alt_va"],
            ),
        )

        if not (search_df["dec_coord_datum_cd"] == "NAD83").all():
            LOG.error("Projection assumption for USGS is incorrect."
                      " Not all points are NAD 83")
        # convert results from NAD83 to WGS 84
        gdf = gdf.set_crs("EPSG:4269").to_crs("EPSG:4326")
        # filter to points within shapefile
        if kwargs['within_geometry']:
            filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        else:
            filtered_gdf = gdf

        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf.index,
                filtered_gdf["station_nm"],
                filtered_gdf["geometry"],
            )
        ]

        return cls.ITERATOR_CLASS([p for p in points])

import datetime
import json
import logging
from os.path import abspath, expanduser, isfile
from typing import List

import numpy as np
import pandas as pd
import pytz
import requests

import geopandas as gpd

from ..dataframe_utils import merge_df, resample_df
from ..variables import MesowestVariables, SensorDescription
from .base import PointData

LOG = logging.getLogger("metloom.pointdata.mesowest")


class MesowestPointData(PointData):
    ALLOWED_VARIABLES = MesowestVariables
    MESO_URL = "https://api.synopticdata.com/v2/stations/timeseries"
    META_URL = "https://api.synopticdata.com/v2/stations/metadata"
    DATASOURCE = "Mesowest"
    POINTS_FROM_GEOM_DEFAULTS = {'within_geometry': True,
                                 'token_json': "~/.synoptic_token.json"}

    def __init__(self, station_id, name,
                 token_json="~/.synoptic_token.json", metadata=None):
        super(MesowestPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None
        self._raw_elements = None
        self._tzinfo = None
        self._token = None
        self._token_json = token_json
        # Add the token to our urls
        self._meta_url = self.META_URL + f"?token={self.token}"
        self._meso_url = self.MESO_URL + f"?token={self.token}"

    @classmethod
    def get_token(cls, token_json):
        """
        Return the token stored in the json
        """
        token_json = abspath(expanduser(token_json))
        if not isfile(token_json):
            raise FileNotFoundError(f"Token file missing. Please sign up for a token "
                                    "with Synoptic Labs and add it to a json.\n "
                                    f"Missing {token_json}!")

        with open(token_json) as fp:
            token = json.load(fp)['token']
        return token

    @property
    def token(self):
        if self._token is None:
            self._token = self.get_token(self._token_json)
        return self._token

    def _get_metadata(self):
        """
        Method to get a shapely Point object to describe the station location
        which is assigned to the metadata property

        Returns:
            shapely.point.Point object in Longitude, Latitude
        """
        shp_point = None
        if self._raw_metadata is None:
            resp = requests.get(self._meta_url, params={"stid": self.id})
            resp.raise_for_status()
            jresp = resp.json()

            self._raw_metadata = jresp["STATION"][0]

            shp_point = gpd.points_from_xy(
                [float(self._raw_metadata["LONGITUDE"])],
                [float(self._raw_metadata["LATITUDE"])],
                z=[float(self._raw_metadata["ELEVATION"])],
            )[0]

        return shp_point

    def _get_data(self,
                  start_date: datetime,
                  end_date: datetime,
                  variables: List[SensorDescription],
                  interval='H'):
        """
        Make get request to Mesowest and return JSON
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            interval: String interval the resulting data is resampled to
        Returns:
            dictionary of response values
        """
        fmt = '%Y%m%d%H%M'
        params = {
            "stid": self.id,
            "start": start_date.strftime(fmt),
            "end": end_date.strftime(fmt),
            "vars": ",".join([s.code for s in variables]),
            'units': 'metric',
        }
        resp = requests.get(self._meso_url, params=params)
        resp.raise_for_status()
        response_data = resp.json()

        final_columns = ["geometry", "site"]

        df = None
        for sensor in variables:
            if response_data:
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns)
                df = merge_df(df, sensor_df)
        df = resample_df(df, variables, interval=interval)
        df = df.dropna(axis=0)
        return df

    def _sensor_response_to_df(self, response_data, sensor, final_columns):
        """
        Convert the response data from the API to a GeoDataFrame
        Format and map columns in the dataframe
        Args:
            response_data: JSON list response from CDEC API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
        Returns:
            GeoDataFrame
        """
        timeseries_response = response_data['STATION'][0]['OBSERVATIONS']
        sensor_df = gpd.GeoDataFrame.from_dict(
            timeseries_response,
            geometry=[self.metadata] * len(timeseries_response['date_time']),
        )
        sensor_df.replace(-9999.0, np.nan, inplace=True)
        # This mapping is important.
        sensor_df.rename(
            columns={
                "date_time": "datetime",
                f"{sensor.code}_set_1": sensor.name,
            },
            inplace=True,
        )
        final_columns += [sensor.name, f"{sensor.name}_units"]
        self._tzinfo = pytz.timezone(response_data['STATION'][0]['TIMEZONE'])
        # Convert the datetime, but 1st remove the Z in the string to avoid an
        # assumed tz.
        sensor_df["datetime"] = sensor_df.apply(
            lambda row: pd.to_datetime(
                row['datetime'].replace(
                    'Z', '')), axis=1)
        sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)

        # set index so joinng works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        sensor_df[f"{sensor.name}_units"] = response_data['UNITS'][sensor.code]

        return sensor_df

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        Get hourly measurement data
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
        Returns:
            GeoDataFrame of data. The dataframe should be indexed on
            ['datetime', 'site'] and have columns
            ['geometry', 'site']. Additionally, for each
            variables, it should have column f'{variable.name}' and
            f'{variable.name}_UNITS'
            See CDECPointData._get_data for example implementation and
            TestCDECStation.tny_daily_expected for example dataframe.
            Datetimes should be in UTC
        """
        df = self._get_data(start_date, end_date, variables, interval='H')
        return df

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        Get daily measurement data
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
        Returns:
            GeoDataFrame of data. The dataframe should be indexed on
            ['datetime', 'site'] and have columns
            ['geometry', 'site']. Additionally, for each
            variables, it should have column f'{variable.name}' and
            f'{variable.name}_UNITS'
            See CDECPointData._get_data for example implementation and
            TestCDECStation.tny_daily_expected for example dataframe.
            Datetimes should be in UTC
        """
        df = self._get_data(start_date, end_date, variables, interval='D')
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
            within_geometry: filter the points to within the shapefile
                instead of just the extents. Default True
            token_json: Path to the public token for the mesowest api
                        default = "~/.synoptic_token.json"

        Returns:
            PointDataCollection
        """
        # assign defaults
        kwargs = cls._add_default_kwargs(kwargs)

        token = cls.get_token(kwargs['token_json'])
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        bbox_str = ','.join(str(bounds[k]) for k in ['minx', 'miny', 'maxx', 'maxy'])
        var_list_str = ','.join([v.code for v in variables])

        # Grab all the stations with the variables we want within a bounding box
        resp = requests.get(cls.META_URL + f"?token={token}",
                            params={'bbox': bbox_str, 'vars': var_list_str})
        resp.raise_for_status()

        points = []
        if resp:
            jdata = resp.json()
            data = jdata['STATION']

            points = []
            for sta in data:
                points.append(MesowestPointData(station_id=sta['STID'],
                                                name=sta['NAME'],
                                                token_json=kwargs['token_json']))
        # build the result geodataframe
        result_df = gpd.GeoDataFrame.from_dict({'STID': [p.id for p in points],
                                                'NAME': [p.name for p in points]},
                                               geometry=[p.metadata for p in points])

        # filter to points within shapefile
        if kwargs['within_geometry']:
            filtered_gdf = result_df[result_df.within(
                projected_geom.iloc[0]["geometry"])]
            points = [p for p in points if p.id in filtered_gdf['STID'].values]

        return cls.ITERATOR_CLASS(points)

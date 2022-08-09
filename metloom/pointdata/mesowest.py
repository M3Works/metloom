from datetime import datetime
import json
import logging
from os.path import abspath, expanduser, isfile
from typing import List

import numpy as np
import pandas as pd
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
    POINTS_FROM_GEOM_DEFAULTS = {
        'within_geometry': True,
        'token_json': "~/.synoptic_token.json",
        'buffer': 0.0
    }
    NO_DATA_MESSAGE = 'No stations found for this request.'

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
                                    "with Synoptic Labs and add it to a json using "
                                    "MesowestPointData.create_token_json(token)\n "
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
            "obtimezone": "UTC",  # this is the default
        }
        resp = requests.get(self._meso_url, params=params)
        resp.raise_for_status()
        response_data = resp.json()

        final_columns = ["geometry", "site"]

        df = None
        for sensor in variables:
            if response_data:
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns, interval=interval)
                df = merge_df(df, sensor_df)

        if df is not None:
            df = df.dropna(axis=0)
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

    @staticmethod
    def _choose_sensor_key(timeseries_response, sensor):
        """
        Choose between possible keys for a variable

        Args:
            timeseries_response: the ["OBSERVATIONS"] return for a station
            sensor: the mesowest variable sensor description

        Returns:
            key for the data or None
        """
        # check that the variable was returned
        set1 = f"{sensor.code}_set_1"
        set1d = f"{sensor.code}_set_1d"
        # choose the best possible response
        if set1 not in timeseries_response and set1d not in timeseries_response:
            return None
        elif set1 in timeseries_response and set1d in timeseries_response:
            len_vals = len([val for val in timeseries_response[set1]
                            if val is not None and not np.isnan(val)])
            len_vals2 = len([val for val in
                            timeseries_response[set1d]
                             if val is not None and not np.isnan(val)])
            sensor_col = set1
            if len_vals < len_vals2:
                sensor_col = set1d
        elif set1 in timeseries_response:
            sensor_col = set1
        else:
            sensor_col = set1d
        return sensor_col

    def _sensor_response_to_df(self, response_data, sensor, final_columns,
                               interval: str = 'H'):
        """
        Convert the response data from the API to a GeoDataFrame
        Format and map columns in the dataframe
        Args:
            response_data: JSON list response from CDEC API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
            interval: string interval for resampling
        Returns:
            GeoDataFrame
        """
        # handle no data returned
        response_summary = response_data.get('SUMMARY')
        if response_summary and response_summary.get(
            "RESPONSE_MESSAGE"
        ) == self.NO_DATA_MESSAGE:
            return None
        # parse the data for the station
        station_response = response_data['STATION']
        if len(station_response) == 0:
            return None
        else:
            timeseries_response = station_response[0]['OBSERVATIONS']
        # check that the variable was returned
        sensor_col = self._choose_sensor_key(timeseries_response, sensor)
        if sensor_col is None:
            return None

        sensor_df = pd.DataFrame.from_dict(
            timeseries_response
        )
        sensor_df.replace(-9999.0, np.nan, inplace=True)
        # This mapping is important.
        sensor_df.rename(
            columns={
                "date_time": "datetime",
                sensor_col: sensor.name,
            },
            inplace=True,
        )
        final_columns += [sensor.name, f"{sensor.name}_units"]

        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
        # set index so joining works
        sensor_df.set_index("datetime", inplace=True)
        # handle timezones
        # mesowest can return utc so we do not need to localize
        sensor_df = sensor_df.tz_convert(self.desired_tzinfo)
        sensor_df = sensor_df.filter(final_columns)

        # make sure we resample to dataframe to the desired output freq
        sensor_df = resample_df(sensor_df, sensor, interval=interval)
        if sensor_df is None:
            return None
        # clean up dataframe and add other columns
        sensor_df = sensor_df.dropna(axis=0)
        sensor_df = gpd.GeoDataFrame(
            sensor_df,
            geometry=[self.metadata] * len(sensor_df),
        )
        sensor_df["site"] = [self.id] * len(sensor_df)

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
            buffer: buffer added to search box
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
        buffer = kwargs["buffer"]
        adjusted_bounds = [
            bounds["minx"] - buffer, bounds["miny"] - buffer,
            bounds["maxx"] + buffer, bounds["maxy"] + buffer,
        ]
        bbox_str = ','.join(str(ab) for ab in adjusted_bounds)
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

    @classmethod
    def create_token_json(token):
        """
        Creates the neccessary synoptic token json for mesowest requests.
        To get public token visit: https://synopticdata.com/mesonet-api
        Args:
            token: Syntoptic Lab's public token.
        Returns:
            None
        """
        json_dict = {'token': token}
        with open(abspath(expanduser("~/.synoptic_token.json")), 'w') as outfile:
            json.dump(json_dict, outfile)

import logging
import json
from os.path import expanduser, isfile, abspath
from .base import PointData
import datetime
from typing import List
import geopandas as gpd
import requests

from ..variables import MesowestVariables, SensorDescription

LOG = logging.getLogger("metloom.pointdata.mesowest")


class MesowestPointData(PointData):

    ALLOWED_VARIABLES = MesowestVariables
    MESO_URL = "https://api.synopticdata.com/v2/stations/timeseries"
    META_URL = "https://api.synopticdata.com/v2/stations/metadata"
    DATASOURCE = "Mesowest"

    def __init__(self, station_id, name, token_json="~/.synoptic_token.json", metadata=None):
        super(MesowestPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None
        self._raw_elements = None
        self._tzinfo = None

        token_json = abspath(expanduser(token_json))
        if not isfile(token_json):
            raise IOError(f"Token file missing. Please sign up for a token with Synoptic Labs and add it to a json.\n Missing {token_json}!")

        with open(token_json) as fp:
            self.token = json.load(fp)['token']

        # Add the token to our urls
        self.META_URL = self.META_URL + f"?token={self.token}"
        self.MESO_URL = self.MESO_URL + f"?token={self.token}"

    def _get_metadata(self):
        if self._raw_metadata is None:
            resp = requests.get(self.META_URL, params={"stid": self.id})
            resp.raise_for_status()
            print(resp.json())
            self._raw_metadata = resp.json()["STATION"]
        return self._raw_metadata


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
            ['geometry', 'site', 'measurementDate']. Additionally, for each
            variables, it should have column f'{variable.name}' and
            f'{variable.name}_UNITS'
            See CDECPointData._get_data for example implementation and
            TestCDECStation.tny_daily_expected for example dataframe.
            Datetimes should be in UTC
        """
        raise NotImplementedError("get_daily_data is not implemented")


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
            ['geometry', 'site', 'measurementDate']. Additionally, for each
            variables, it should have column f'{variable.name}' and
            f'{variable.name}_UNITS'
            See CDECPointData._get_data for example implementation and
            TestCDECStation.tny_daily_expected for example dataframe.
            Datetimes should be in UTC
        """
        raise NotImplementedError("get_hourly_data is not implemented")


    def points_from_geometry(
        self,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        snow_courses=False,
        within_geometry=True
    ):
        """
        Find a collection of points with measurements for certain variables
        contained within a shapefile. Any point in the shapefile with
        measurements for any of the variables should be included
        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription
            snow_courses: boolean for including only snowcourse data or no
                snowcourse data
            within_geometry: filter the points to within the shapefile
                instead of just the extents. Default True
        Returns:
            PointDataCollection
        """
        raise NotImplementedError("points_from_geometry not implemented")

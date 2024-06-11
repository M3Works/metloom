"""
Location to keep readers that pull in a flat file like a csv.
"""
import os

import pandas as pd

from metloom.pointdata import PointData
from metloom.variables import VariableBase, SensorDescription
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging
import requests
from typing import List
import geopandas as gpd
import numpy as np


LOG = logging.getLogger(__name__)


class StationInfo(Enum):
    """
    Since there is not enough info via an API, csv readers rely on a list of stations and info associated with
    them to build the calls. This should allow for a common dataset with multiple stations to be verified and
    isolated
    """

    # Name, id, lat, long, http filename
    # GM_STUDY_PLOT = "Grand Mesa Study Plot", "GMSP",  39.05084, 108.06144,"2017.06.21/SNEX_Met_GMSP2_final_output.csv"

    @property
    def station_name(self):
        return self.value[0]

    @property
    def station_id(self):
        return self.value[1]

    @property
    def latitude(self):
        return self.value[2]

    @property
    def longitude(self):
        return self.value[3]

    @property
    def elevation(self):
        return self.value[4]

    @property
    def path(self):
        return Path(self.value[5])

    @classmethod
    def all_station_ids(cls):
        return  [e.station_id for e in cls]

    @classmethod
    def all_points(cls):
        return  [e.point for e in cls]
    @classmethod
    def from_station_id(cls, station_id):
        result = None
        for e in cls:
            if station_id == e.station_id:
                result = e
                break
        return result

    @property
    def point(self):
        return gpd.points_from_xy([self.longitude],
                                  [self.latitude],
                                  [self.elevation])[0]

class CSVPointData(PointData):
    """
    Some met station data is stored off in flat csv files. This class enables the
    management of downloading those files while still allowing a similar interface
    """
    ALLOWED_VARIABLES = VariableBase
    ALLOWED_STATIONS = StationInfo
    UTC_OFFSET_HOURS = 0 # Allows users to specificy the timezone of the datasets

    def __init__(self, station_id, name=None, metadata=None, cache='./cache'):
        """
        See docstring for PointData.__init__
        """
        super(CSVPointData, self).__init__(
            station_id, name,
            metadata=metadata
        )
        self._raw_metadata = None
        self._tzinfo = timezone(timedelta(hours=self.UTC_OFFSET_HOURS))
        self._cache = Path(cache)


        self._station_info = None
        self.datafile = None

        self.valid = False

    def _verify_station(self):
        """ Verifies the station is valid using the associated enum"""
        self._station_info = self.ALLOWED_STATIONS.from_station_id(self.id)
        self.datafile = self._cache.joinpath(self._station_info.path.name)

        if self._station_info is not None:
            # Auto assign name
            self.name = self._station_info.station_name if self.name is None else self.name
            return True
        else:
            LOG.error(f"Station ID {self.id} is not valid, allowed id's are {', '.join(self.ALLOWED_STATIONS.all_station_ids())}")
            return False

    def _verify_sensor(self, resp_df, variable: SensorDescription):
        """ Verifies the station is valid using the associated enum"""
        if variable.code in resp_df.columns:
            return True
        else:
            LOG.debug(f"{variable.name} not found in {self.id} data")
            return False

    def _file_url(self):
        """Returns the url to the file containing the station data"""
        raise NotImplementedError('CSVPointData._file_url() must be implemented to download csv station data.')

    def _assign_datetime(self, resp_df):
        raise NotImplementedError('CSVPointData._assign_datetime() must be implemented to download csv station data.')


    def _download(self, url):
        """Download the file"""
        with requests.get(url, stream=True) as r:
            LOG.info('Downloading csv file...')
            with open(self.datafile, mode='w+') as fp:
                for line in r.iter_lines():
                    fp.write(line.decode('utf-8') + '\n')

    def _get_one_variable(self, resp_df, start, end, period, variable:SensorDescription):
        """
        Retrieve a single variable and process it accordingly
        """
        method = "sum" if variable.accumulated else "average"
        if self._verify_sensor(resp_df, variable):
            isolated = resp_df[variable.code].loc[start:end]
            isolated[isolated == -9999] = np.nan
            if method == 'average':
                data = isolated.resample(period).mean()
            elif method == 'sum':
                data = isolated.resample(period).sum()
            else:
                raise Exception('Invalid aggregation method')
        else:
            data = None
            LOG.debug(f"No data returned for {variable}")

        return data

    def _get_data(
        self, start_date, end_date, variables: List[SensorDescription],
        period):
        """
        Utilizes cached data or downloads the data
        """
        self.valid = self._verify_station()

        if not self.datafile.exists():
            if self.valid:
                url = self._file_url()

                # Make the cache dir
                if not self._cache.is_dir():
                    os.mkdir(self._cache)

                self._download(url)

        resp_df = pd.read_csv(self.datafile, parse_dates=[0])
        resp_df = self._assign_datetime(resp_df)

        df = pd.DataFrame()
        for i, variable in enumerate(variables):
            df_var = self._get_one_variable(resp_df, start_date, end_date, period, variable)
            if df_var is not None:
                if i==0:
                    df.index= df_var.index

                df[variable.name] = df_var

        if np.all(df.isnull()):
            return None

        # Set the site info
        df["site"] = [self.id] * len(df)
        df["datasource"] = [self.DATASOURCE] * len(df)
        # Make this a geodataframe
        df = gpd.GeoDataFrame(df, geometry=[self.metadata] * len(df))
        df = df.reset_index().set_index(["datetime", "site"])

        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "D"
        )

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "H"
        )

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        raise NotImplementedError("Not implemented")

    def _get_metadata(self):
        pass

    @classmethod
    def points_from_geometry(
        self,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        within_geometry=True,
        buffer=0.0, **kwargs):
        # Avoid multiple polys and use a buffer if requested.
        projected_geom = geometry.dissolve().buffer(buffer).to_crs(4326)

        gdf = gpd.GeoDataFrame(geometry=self.ALLOWED_STATIONS.all_points(), data=[], crs=4326)
        # Use the exact geometry to filter, otherwise use the bounds of the poly
        if within_geometry:
            search_area = projected_geom.iloc[0]
        else:
            search_area = projected_geom.envelope.iloc[0]

        filtered_gdf = gdf[gdf.within(search_area)]
        return filtered_gdf
    @property
    def metadata(self):
        """
        Hardcode the metadata
        """
        return self._station_info.point

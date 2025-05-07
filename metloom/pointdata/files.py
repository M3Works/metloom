"""
Location to keep readers that pull in a flat file like a csv.
"""
import os

import pandas as pd

from metloom.pointdata import PointData
from metloom.variables import SensorDescription
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
    Since there is not enough info via an API, csv readers rely on a list of stations
    and info associated with them to build the calls. This should allow for a common
    dataset with multiple stations to be verified and isolated
    """

    # Defined: Name, id, lat, long, http sub path to file

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
        return [e.station_id for e in cls]

    @classmethod
    def all_points(cls):
        return [e.point for e in cls]

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
    ALLOWED_STATIONS = StationInfo
    UTC_OFFSET_HOURS = 0  # Allows users to specificy the timezone of the datasets

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
        self.valid = False

    def _verify_station(self):
        """ Verifies the station is valid using the associated enum"""
        self._station_info = self.ALLOWED_STATIONS.from_station_id(self.id)

        if self._station_info is not None:
            # Auto assign name
            if self.name is None:
                self.name = self._station_info.station_name

            return True
        else:
            LOG.error(f"Station ID {self.id} is not valid, allowed id's are "
                      f"{', '.join(self.ALLOWED_STATIONS.all_station_ids())}")
            return False

    def _verify_sensor(self, resp_df, variable: SensorDescription):
        """ Verifies the station is valid using the associated enum"""
        if variable.code in resp_df.columns:
            return True
        else:
            LOG.debug(f"{variable.name} not found in {self.id} data")
            return False

    def _file_urls(self, *args):
        """Returns the url to the file containing the station data"""
        raise NotImplementedError('CSVPointData._file_urls() must be implemented to '
                                  'download csv station data.')

    def _assign_datetime(self, resp_df):
        raise NotImplementedError('CSVPointData._assign_datetime() must be implemented '
                                  'to download csv station data.')

    def _download(self, urls):
        """Download the file(s)"""
        filenames = []
        for url in urls:
            filename = self._cache.joinpath(Path(url).name)
            filenames.append(filename)
            if not filename.exists():
                with requests.get(url, stream=True) as r:
                    LOG.info(f'Downloading {Path(url).name}...')
                    lines = r.iter_lines()
                    with open(filename, mode='w+') as fp:
                        for line in lines:
                            fp.write(line.decode('utf-8') + '\n')
        return filenames

    def _get_one_variable(self, resp_df, period, variable: SensorDescription):
        """
        Retrieve a single variable and process it accordingly
        """
        method = "sum" if variable.accumulated else "average"
        if self._verify_sensor(resp_df, variable):
            isolated = resp_df[variable.code].loc[:]

            # TODO: This may only be true for SNOWEX
            isolated.loc[isolated == -9999] = np.nan
            if method == 'average':
                data = isolated.resample(period).mean()
            elif method == 'sum':
                data = isolated.resample(period).sum()
            else:
                raise Exception('Invalid aggregation method')
        else:
            data = None
            LOG.debug(f"No data returned for {variable.name}")

        return data

    def _get_data(self, start_date, end_date, variables: List[SensorDescription],
                  period):
        """
        Utilizes cached data or downloads the data
        """
        self.valid = self._verify_station()

        if not self.valid:
            return None

        urls = self._file_urls(self._station_info.station_id,
                               start_date, end_date)

        # Make the cache dir
        if not self._cache.is_dir():
            os.mkdir(self._cache)

        # Download data if it doesn't exist locally.
        files = self._download(urls)
        dfs = [pd.read_csv(f, index_col=False, low_memory=False) for f in files]
        resp_df = pd.concat(dfs)
        resp_df = self._assign_datetime(resp_df)

        # use a predefined index to show nans in the event of patch data
        df = pd.DataFrame(index=pd.date_range(start_date, end_date, freq=period,
                                              name='datetime'),
                          columns=[v.name for v in variables])

        # Use this instead .loc to avoid index on patchy data
        ind = (resp_df.index >= pd.Timestamp(start_date)) & (
            resp_df.index < pd.Timestamp(end_date)
        )
        isolated = resp_df.loc[ind, resp_df.columns]
        for i, variable in enumerate(variables):
            df_var = self._get_one_variable(isolated, period, variable)
            if df_var is not None:
                if not np.all(df_var.isnull()):
                    df[variable.name].loc[df_var.index] = df_var
                    df[f"{variable.name}_units"] = variable.units
            else:
                df = df.drop(columns=[variable.name])

        # All nan data suggests no matching data
        if np.all(df.isnull()):
            return None

        # Make this a geodataframe
        df["site"] = [self.id] * len(df)
        df["datasource"] = [self.DATASOURCE] * len(df)
        # Make this a geodataframe
        df = gpd.GeoDataFrame(df, geometry=[self.metadata] * len(df))
        df = df.reset_index().set_index(["datetime", "site"])

        self.validate_sensor_df(df)
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "D"
        )

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "h"
        )

    def _get_metadata(self):
        return self._station_info.point

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription], within_geometry=True,
                             buffer=0.0, **kwargs):
        # Avoid multiple polys and use a buffer if requested.
        projected_geom = geometry.dissolve().buffer(buffer).to_crs(4326)

        gdf = gpd.GeoDataFrame(geometry=cls.ALLOWED_STATIONS.all_points(), data=[],
                               crs=4326)
        # Use the exact geometry to filter, otherwise use the bounds of the poly
        if within_geometry:
            search_area = projected_geom.iloc[0]
        else:
            search_area = projected_geom.envelope.iloc[0]

        filtered_gdf = gdf[gdf.within(search_area)]
        return filtered_gdf

from datetime import datetime
from typing import List

import geopandas as gpd
import pandas as pd
import logging

from ..variables import VariableBase, SensorDescription

LOG = logging.getLogger("metloom.pointdata.base")


class PointDataCollection:
    """
    Iterator class for a collection of PointData objects.
    This allows conversion to a GeoDataFrame
    """

    def __init__(self, points: List[object] = None):
        """

        Args:
            points: List of point data objects
        """
        self.points = points or []
        self._index = 0

    def add_point(self, point):
        """
        Append point to collection of PointData objects

        Args:
            point: PointData object
        """
        self.points.append(point)

    def to_dataframe(self):
        """
        Returns:
            GeoDataFrame of points. Columns are ['name', 'id', 'geometry']
        """
        names = []
        ids = []
        meta = []
        for point in self.points:
            names += [point.name]
            ids += [point.id]
            meta += [point.metadata]

        obj = {"name": names, "id": ids}
        return gpd.GeoDataFrame.from_dict(obj, geometry=meta)

    def __len__(self):
        return len(self.points)

    def __iter__(self):
        for item in self.points:
            yield item


class PointData(object):
    ALLOWED_VARIABLES = VariableBase
    ITERATOR_CLASS = PointDataCollection

    def __init__(self, station_id, name, metadata=None):
        """

        Args:
            station_id: code used within datasource API to access station
            name: station name. This will be used in the GeoDataFrames
            metadata: Optional shapely point. This will bypass the
                _get_metadata method if provided
        """
        self.id = station_id
        self.name = name
        self._metadata = metadata
        self.desired_tzinfo = "UTC"

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

    def get_snow_course_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        Get snow course data
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
        raise NotImplementedError("get_snow_course_data is not implemented")

    def _get_metadata(self):
        """
        Method to get a shapely Point object to describe the
        Returns:
            shapely.point.Point object in Longitude, Latitude
        """
        raise NotImplementedError("_get_metadata is not implemented")

    def _handle_df_tz(self, val):
        """
        Covert one entry from a df from cls.TZINFO to UTC
        """
        if pd.isna(val):
            return val
        else:
            local = val.tz_localize(self.tzinfo)
            return local.tz_convert(self.desired_tzinfo)

    @property
    def tzinfo(self):
        """
        tzinfo that pandas can use for tz_localize
        """
        return self._tzinfo

    @property
    def metadata(self):
        """
        metadata property
        Returns:
            shapely.point.Point object in Longitude, Latitude with z in ft
        """
        if self._metadata is None:
            self._metadata = self._get_metadata()
        return self._metadata

    def points_from_geometry(
        self,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        snow_courses=False,
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
        Returns:
            PointDataCollection
        """
        raise NotImplementedError("points_from_geometry not implemented")

    def validate_sensor_df(self, gdf: gpd.GeoDataFrame):
        """
        Validate that the GeoDataFrame returned is formatted correctly.
        The goal of this method is to ensure base classes are returning a
        consistent format of dataframe
        """
        expected_columns = ["measurementDate", "geometry"]
        expected_indexes = ["datetime", "site"]
        assert isinstance(gdf, gpd.GeoDataFrame)
        columns = gdf.columns
        index_names = gdf.index.names
        # check for required indexes
        for ei in expected_indexes:
            assert ei in index_names
        # check for expected columns
        for column in expected_columns:
            assert column in columns
        remaining_columns = [c for c in columns if c not in expected_columns]
        # make sure all variables have a units column as well
        for rc in remaining_columns:
            if "_units" not in rc:
                assert f"{rc}_units" in remaining_columns

    def __repr__(self):
        return f"{self.__class__.__name__}({self.id!r}, {self.name!r})"

    def __str__(self):
        return f"{self.name} ({self.id})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id and self.name == other.name

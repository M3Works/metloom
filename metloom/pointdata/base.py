import copy
import logging
from datetime import datetime
from typing import List

import pandas as pd

import geopandas as gpd

from ..variables import SensorDescription, VariableBase

LOG = logging.getLogger("metloom.pointdata.base")


class DataValidationError(RuntimeError):
    pass


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
        datasource = []
        for point in self.points:
            names += [point.name]
            ids += [point.id]
            meta += [point.metadata]
            datasource += [point.DATASOURCE]

        obj = {"name": names, "id": ids, "datasource": datasource}
        return gpd.GeoDataFrame.from_dict(obj, geometry=meta)

    def __len__(self):
        return len(self.points)

    def __iter__(self):
        for item in self.points:
            yield item


class GenericPoint(object):
    """
    Class for storing metadata. and defining the expected data format
    returned from `get_data` methods

    """
    ALLOWED_VARIABLES = VariableBase
    ITERATOR_CLASS = PointDataCollection
    DATASOURCE = None
    EXPECTED_COLUMNS = ["geometry", "datasource"]
    EXPECTED_INDICES = ["datetime", "site"]
    NON_VARIABLE_COLUMNS = EXPECTED_INDICES + EXPECTED_COLUMNS

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

    def _get_metadata(self):
        """
        Method to get a shapely Point object to describe the station location

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

    @classmethod
    def validate_sensor_df(cls, gdf: gpd.GeoDataFrame):
        """
        Validate that the GeoDataFrame returned is formatted correctly.
        The goal of this method is to ensure base classes are returning a
        consistent format of dataframe
        """
        if gdf is None:
            return
        assert isinstance(gdf, gpd.GeoDataFrame)
        columns = gdf.columns
        index_names = gdf.index.names
        # check for required indexes
        for ei in cls.EXPECTED_INDICES:
            if ei not in index_names:
                raise DataValidationError(
                    f"{ei} was expected, but not found as an"
                    f" index of the final dataframe"
                )
        # check for expected columns - avoid modifying at class level
        expected_columns = copy.deepcopy(cls.EXPECTED_COLUMNS)
        possible_extras = ["measurementDate", "quality_code"]
        for pe in possible_extras:
            if pe in columns:
                expected_columns += [pe]
        for column in expected_columns:
            if column not in columns:
                raise DataValidationError(
                    f"{column} was expected, but not found as a"
                    f" column of the final dataframe"
                )

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


class PointData(GenericPoint):
    """
    Extend GenericPoint and add functions for finding data from geometry
    and for gettings daily, hourly, or snow course data
    """

    # Default kwargs for function points from geometry
    POINTS_FROM_GEOM_DEFAULTS = {
        'within_geometry': True, 'snow_courses': False,
        'buffer': 0.0, "filter_to_active": False
    }

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

    @classmethod
    def _add_default_kwargs(cls, kwargs):
        """
        Populates the kwargs for the points from geometry function
        """
        for k, v in cls.POINTS_FROM_GEOM_DEFAULTS.items():
            if k not in kwargs.keys():
                kwargs[k] = v
        return kwargs

    def points_from_geometry(
        self,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        snow_courses=False,
        within_geometry=True,
        buffer=0.0
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
            buffer: buffer added to search box
        Returns:
            PointDataCollection
        """
        raise NotImplementedError("points_from_geometry not implemented")

    @classmethod
    def _validate_geodataframe(cls, gdf: gpd.GeoDataFrame):
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise DataValidationError('Returned DataFrame must be a GeoDataframe')

    @classmethod
    def _validate_df_indicies(cls, gdf: gpd.GeoDataFrame):
        """ Confirm the df is indexed properly"""
        for ei in cls.EXPECTED_INDICES:
            if ei not in gdf.index.names:
                raise DataValidationError(
                    f"{ei} was expected, but not found as an"
                    f" index of the final dataframe"
                )

    @classmethod
    def _validate_df_columns(cls, gdf: gpd.GeoDataFrame, expected_columns: List[str]):
        # check for expected columns - avoid modifying at class level
        possible_extras = ["measurementDate", "quality_code"]
        columns = gdf.columns
        for pe in possible_extras:
            if pe in columns:
                expected_columns += [pe]

        for column in expected_columns:
            if column not in columns:
                raise DataValidationError(
                    f"{column} was expected, but not found as a"
                    f" column of the final dataframe"
                )

    @classmethod
    def _validate_df_units(cls, gdf: gpd.GeoDataFrame, expected_columns: List[str]):
        """
        Check the variables requested have units associated
        """
        remaining_columns = [c for c in gdf.columns if c not in expected_columns]
        # make sure all variables have a units column as well
        for rc in remaining_columns:
            if "_units" not in rc:
                if f"{rc}_units" not in remaining_columns:
                    raise DataValidationError(f'Missing units column for {rc}')

    @classmethod
    def validate_sensor_df(cls, gdf: gpd.GeoDataFrame):
        """
        Validate that the GeoDataFrame returned is formatted correctly.
        The goal of this method is to ensure base classes are returning a
        consistent format of dataframe
        """
        if gdf is None:
            return

        expected_columns = copy.deepcopy(cls.EXPECTED_COLUMNS)

        # Confirm the dataframe is a geodataframe
        cls._validate_geodataframe(gdf)
        # Confirm the df is indexed properly
        cls._validate_df_indicies(gdf)
        # Confirm the columns are correct
        cls._validate_df_columns(gdf, expected_columns)
        # Confirm that any columns from variables have associated units
        cls._validate_df_units(gdf, expected_columns)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.id!r}, {self.name!r})"

    def __str__(self):
        return f"{self.name} ({self.id})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id and self.name == other.name

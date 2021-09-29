from datetime import datetime, timezone, timedelta, date
from typing import List

import geopandas as gpd
import pandas as pd
import pytz
import requests
import logging
from climata import snotel
from climata.base import FilterOpt, DateOpt, as_list
from wq.io.exceptions import NoData

from .dataframe_utils import join_df
from .variables import (
    VariableBase, CdecStationVariables, SnotelVariables, SensorDescription)

LOG = logging.getLogger("dataloom.point_data")


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

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        """
        Get daily measurement data
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of dataloom.variables.SensorDescription object
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

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        """
        Get hourly measurement data
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of dataloom.variables.SensorDescription object
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

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        """
        Get snow course data
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of dataloom.variables.SensorDescription object
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

    def points_from_geometry(self, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription],
                             snow_courses=False):
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
        return f'{self.__class__.__name__}({self.id!r}, {self.name!r})'

    def __str__(self):
        return f'{self.name} ({self.id})'

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id and self.name == other.name


class CDECPointData(PointData):
    """
    Implement PointData methods for CDEC data source
    API documentation here https://cdec.water.ca.gov/dynamicapp/
    """
    ALLOWED_VARIABLES = CdecStationVariables
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
    META_URL = "http://cdec.water.ca.gov/cdecstation2/CDecServlet/" \
               "getStationInfo"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(CDECPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._raw_metadata = None
        self._tzinfo = pytz.timezone("US/Pacific")

    def _get_all_metadata(self):
        """
        Get all the raw metadata for a station. This is a list of sensor
        descriptions for the station
        Returns:
            A list of dictionaries describing the sensors at a station
        """
        if self._raw_metadata is None:
            resp = requests.get(self.META_URL, params={'stationID': self.id})
            resp.raise_for_status()
            self._raw_metadata = resp.json()["STATION"]
        return self._raw_metadata

    def is_only_snow_course(self):
        """
        Determine if a station only has snow course measurements
        """
        data = self._get_all_metadata()
        manual_check = [
            d["DUR_CODE"] == "M" for d in data if d['SENS_GRP'] == "snow"
        ]
        result = False
        if len(manual_check) > 0 and all(manual_check):
            result = True
        if result and not self.is_only_monthly():
            # This would happen if all snow sensors have code "M"
            # but there are other hourly or daily sensors
            raise Exception(
                f"We have not accounted for this scenario. Please talk to "
                f"a Micah about how {self.id} violates their assumptions.")
        return result

    def is_partly_snow_course(self):
        """
        Determine if any of the snow sensors at a station are on a monthly
        interval
        Assumption: Monthly snow sensor measurements are snow courses
        """
        data = self._get_all_metadata()
        return any(
            [d["DUR_CODE"] == "M" for d in data if d['SENS_GRP'] == "snow"]
        )

    def is_only_monthly(self):
        """
        determine if all sensors for a station are on a monthly interval
        """
        data = self._get_all_metadata()
        manual_check = [
            d["DUR_CODE"] == "M" for d in data
        ]
        if len(manual_check) > 0 and all(manual_check):
            return True
        return False

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        data = self._get_all_metadata()
        # TODO: Should this be sensor specific?
        metadata_by_name = {d["SENS_LONG_NAME"]: d for d in data}
        # default to the first sensor
        chosen_sensor_data = data[0]
        # try to replace it with a desired sensor
        # TODO: Change this
        for choice in ["SNOW, WATER CONTENT", "SNOW DEPTH",
                       "PRECIPITATION, ACCUMULATED"]:
            if metadata_by_name.get(choice) is not None:
                chosen_sensor_data = metadata_by_name[choice]
                break

        return gpd.points_from_xy(
            [chosen_sensor_data["LONGITUDE"]],
            [chosen_sensor_data["LATITUDE"]],
            z=[chosen_sensor_data["ELEVATION"]]
        )[0]

    def _data_request(self, params):
        """
        Make get request to CDEC and return JSON
        Args:
            params: dictionary of request parameters
        Returns:
            dictionary of response values
        """
        resp = requests.get(self.CDEC_URL, params=params)
        resp.raise_for_status()
        return resp.json()

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
        sensor_df = gpd.GeoDataFrame.from_dict(
            response_data,
            geometry=[self.metadata] * len(response_data),
        )
        # this mapping is important. Sometimes obsDate is null
        sensor_df.rename(columns={
            "date": "datetime",
            "obsDate": "measurementDate",
            "value": sensor.name,
            "units": f"{sensor.name}_units",
            "stationId": "site"
        },
            inplace=True)
        final_columns += [sensor.name, f"{sensor.name}_units"]
        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
        sensor_df["measurementDate"] = pd.to_datetime(
            sensor_df["measurementDate"]
        )
        sensor_df["datetime"] = sensor_df["datetime"].apply(
            self._handle_df_tz
        )
        sensor_df["measurementDate"] = sensor_df["measurementDate"] \
            .apply(self._handle_df_tz)
        # set index so joinng works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        return sensor_df

    def _get_data(self, start_date: datetime, end_date: datetime,
                  variables: List[SensorDescription], duration: str):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of dataloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            duration: CDEC duration code ['M', 'H', 'D']
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """
        params = {
            "Stations": self.id,
            "dur_code": duration,
            "Start": start_date.isoformat(),
            "End": end_date.isoformat()
        }
        df = None
        final_columns = ["geometry", "site", "measurementDate"]
        for sensor in variables:
            params["SensorNums"] = sensor.code
            response_data = self._data_request(params)
            if response_data:
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns
                )
                df = join_df(df, sensor_df)

        if df is not None and len(df.index) > 0:
            df.reset_index(inplace=True)
            df.set_index(keys=["datetime", "site"], inplace=True)
            df.index.set_names(["datetime", "site"], inplace=True)
        self.validate_sensor_df(df)
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        """
        See docstring for PointData.get_daily_data
        Example query:
        https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet?
        Stations=TNY&SensorNums=3&dur_code=D&Start=2021-05-16&End=2021-05-16
        """
        return self._get_data(start_date, end_date, variables, "D")

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        """
        See docstring for PointData.get_hourly_data
        """
        return self._get_data(start_date, end_date, variables, "H")

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        """
        See docstring for PointData.get_snow_course_data
        """
        if not self.is_partly_snow_course():
            raise ValueError(f"{self.id} is not a snow course")
        return self._get_data(start_date, end_date, variables, "M")

    @staticmethod
    def _station_sensor_search(bounds, sensor: SensorDescription, dur=None,
                               collect=None):
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
        # TODO: do we want this buffer?
        buffer = 0.00
        dur_str = f"&dur_chk=on&dur={dur}" if dur else "&dur="
        collect_str = f"&collect_chk=on&collect={collect}" if collect \
            else "&collect=NONE+SPECIFIED"
        url = f"https://cdec.water.ca.gov/dynamicapp/staSearch?sta=" \
              f"&sensor_chk=on&sensor={sensor.code}" \
              f"{collect_str}" \
              f"{dur_str}" \
              f"&active_chk=on&active=Y" \
              f"&loc_chk=on" \
              f"&lon1={bounds['minx']-buffer}&lon2={bounds['maxx']+buffer}" \
              f"&lat1={bounds['miny']-buffer}&lat2={bounds['maxy']+buffer}" \
              f"&elev1=-5&elev2=99000&nearby=&basin=NONE+SPECIFIED" \
              f"&hydro=NONE+SPECIFIED&county=NONE+SPECIFIED&agency_num=160" \
              f"&display=sta"
        try:
            return pd.read_html(url)[0]
        except ValueError:
            LOG.error(f"No tables for {url}")
            return None

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription],
                             snow_courses=False
                             ):
        """
        See docstring for PointData.points_from_geometry
        """
        # Assume station search result is in 4326
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        search_df = None
        station_search_kwargs = {}
        # Filter to manual, monthly measurements if looking for snow courses
        if snow_courses:
            station_search_kwargs["dur"] = "M"
            station_search_kwargs["collect"] = "MANUAL+ENTRY"
        for variable in variables:
            result_df = cls._station_sensor_search(
                bounds, variable, **station_search_kwargs
            )
            if result_df is not None:
                result_df["index_id"] = result_df["ID"]
                result_df.set_index("index_id")
                search_df = join_df(search_df, result_df, how="outer")
        # return empty collection if we didn't find any points
        if search_df is None:
            return cls.ITERATOR_CLASS([])
        gdf = gpd.GeoDataFrame(search_df, geometry=gpd.points_from_xy(
            search_df["Longitude"], search_df["Latitude"],
            z=search_df["ElevationFeet"]
        ))
        # filter to points within shapefile
        # TODO: do we want to make this optional?
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]['geometry'])]

        points = [
            cls(row[0], row[1], metadata=row[2]) for row in zip(
                filtered_gdf['ID'], filtered_gdf["Station Name"],
                filtered_gdf["geometry"]
            )
        ]
        # filter to snow courses or not snowcourses depending on desired result
        if snow_courses:
            return cls.ITERATOR_CLASS(
                [p for p in points if p.is_partly_snow_course()]
            )
        else:
            return cls.ITERATOR_CLASS(
                [p for p in points if not p.is_only_snow_course()]
            )


class LoomStationIO(snotel.StationIO):
    """
    Extend climata.snotel.StationIO to allow filtering based on network
    and longitude
    """
    max_longitude = FilterOpt(url_param='maxLongitude')
    min_longitude = FilterOpt(url_param='minLongitude')
    networks = FilterOpt(url_param='networkCds')


class MonthlyDataIO(snotel.SnotelIO):
    """
    Extend climata.snotel.SnotelIO for sampling SEMIMONTHLY data.
    This allows sampling of snowcourses
    """
    data_function = 'getData'

    # Applicable WebserviceLoader default options
    station = FilterOpt(required=True, url_param='stationTriplets')
    parameter = FilterOpt(required=True, url_param='elementCd')
    start_date = DateOpt(required=True, url_param='beginDate')
    end_date = DateOpt(required=True, url_param='endDate')

    default_params = {
        'ordinal': 1,
        'duration': 'SEMIMONTHLY',
        'getFlags': 'true',
        'alwaysReturnDailyFeb29': 'false',
    }

    def parse(self):
        data = self.data[0]
        if not data or 'values' not in data:
            raise NoData
        dates = as_list(data['collectionDates'])
        vals = as_list(data['values'])
        flags = as_list(data['flags'])

        self.data = [{
            'date': datetime.strptime(date_val, '%Y-%m-%d').date(),
            'value': val,
            'flag': flag
        } for date_val, val, flag in
            zip(dates, vals, flags) if date_val is not None]


class StationMonthlyDataIO(snotel.StationDataIO):
    """
    Extension of climata.snotel.StationDataIO to allow snowcourse sampling
    """
    inner_io_class = MonthlyDataIO
    duration = "SEMIMONTHLY"


class SnotelPointData(PointData):
    """
    Implement PointData methods for SNOTEL data source
    API documentation here:
    https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService/webServiceReference/
    Website has variable and network codes
    Possible testing resource:
    https://wcc.sc.egov.usda.gov/awdbWebService/webservice/testwebservice.jsf?webserviceName=/awdbWebService
    """
    ALLOWED_VARIABLES = SnotelVariables

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(SnotelPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._raw_metadata = None
        self._tzinfo = None

    def _snotel_response_to_df(self, response: snotel.StationDataIO,
                               variables: List[VariableBase], date_key="date"):
        """
        Convert the response from climata.snotel classes into
        Args:
            response: climata.snotel data response
            variables: list of variables to extract
            date_key: which key the response uses to store date(time)s
        """
        df = None
        # TODO: possible DRY oportunity here too
        final_columns = ["geometry", "site", "measurementDate"]
        for param in response:
            for variable in variables:
                if param.elementcd == variable.code:
                    data = param.data
                    transformed = [
                        {"datetime": getattr(v, date_key),
                         "site": self.id,
                         "measurementDate": getattr(v, date_key),
                         variable.name: v.value,
                         f"{variable.name}_units": param.storedunitcd
                         }
                        for v in data
                    ]
                    final_columns += [variable.name, f"{variable.name}_units"]
                    sensor_df = gpd.GeoDataFrame.from_dict(
                        transformed,
                        geometry=[self.metadata] * len(transformed)
                    )
                    # TODO: possibly an opportunity for DRY here (see CDEC)
                    sensor_df["datetime"] = pd.to_datetime(
                        sensor_df["datetime"])
                    sensor_df["measurementDate"] = pd.to_datetime(
                        sensor_df["measurementDate"]
                    )
                    sensor_df["datetime"] = sensor_df["datetime"].apply(
                        self._handle_df_tz
                    )
                    sensor_df["measurementDate"] = \
                        sensor_df["measurementDate"].apply(self._handle_df_tz)
                    # set index so joining works
                    sensor_df.set_index("datetime", inplace=True)
                    sensor_df = sensor_df.filter(final_columns)
                    df = join_df(df, sensor_df)

        if df is not None and len(df.index) > 0:
            df.reset_index(inplace=True)
            df.set_index(keys=["datetime", "site"], inplace=True)
            df.index.set_names(["datetime", "site"], inplace=True)
        self.validate_sensor_df(df)
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        """
        See docstring for PointData.get_daily_data
        """
        params = snotel.StationDailyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat()
        )
        return self._snotel_response_to_df(params, variables)

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        """
        See docstring for PointData.get_hourly_data
        """
        params = snotel.StationHourlyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat()
        )
        return self._snotel_response_to_df(
            params, variables, date_key="datetime"
        )

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        """
        See docstring for PointData.get_snow_course_data
        """
        params = StationMonthlyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat()
        )
        return self._snotel_response_to_df(
            params, variables, date_key="date"
        )

    def _get_all_metadata(self):
        """
        Set _raw_metadata once using climata.snotel
        """
        if self._raw_metadata is None:
            self._raw_metadata = snotel.StationMetaIO(
                station=self.id
            )
        return self._raw_metadata

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        all_metadata = self._get_all_metadata()
        data = all_metadata[0]
        return gpd.points_from_xy(
            [data.longitude], [data.latitude], z=[data.elevation]
        )[0]

    def _get_tzinfo(self):
        """
        Return timezone info that pandas can use from the raw_metadata
        """
        all_metadata = self._get_all_metadata()
        metadata = all_metadata[0]
        # Snow courses might not have a timezone attached
        if hasattr(metadata, "stationdatatimezone"):
            tz_hours = metadata.stationdatatimezone
        else:
            LOG.error(
                f"Could not find timezone info for {self.id} ({self.name})"
            )
            tz_hours = 0
        return timezone(timedelta(hours=tz_hours))

    @property
    def tzinfo(self):
        if self._tzinfo is None:
            self._tzinfo = self._get_tzinfo()
        return self._tzinfo

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription],
                             snow_courses=False):
        """
        See docstring for PointData.points_from_geometry
        """
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        # TODO: network may need to change to get streamflow
        network = "SNOW" if snow_courses else "SNTL"
        all_stations = []
        df = None
        for variable in variables:
            # this search is default AND on all parameters
            # so search for each variable seperately
            response = LoomStationIO(
                max_latitude=bounds["maxy"],
                min_latitude=bounds["miny"],
                max_longitude=bounds["maxx"],
                min_longitude=bounds["minx"],
                networks=network,  # only snotel, not
                parameter=variable.code,
                # ordinals=  # TODO: what are ordinals?
            )
            if len(response.data) > 0:
                result_df = pd.DataFrame.from_records(response.data)
                result_df["index_id"] = result_df["stationTriplet"]
                df = join_df(df, result_df, how="outer")
        # TODO: possible DRYing out here

        if df is None:
            return cls.ITERATOR_CLASS([])

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(
            df["longitude"], df["latitude"], z=df["elevation"]
        ))
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]['geometry'])]
        points = [
            cls(row[0], row[1], metadata=row[2]) for row in zip(
                filtered_gdf['stationTriplet'], filtered_gdf['name'],
                filtered_gdf['geometry']
            )
        ]
        return PointDataCollection(points)

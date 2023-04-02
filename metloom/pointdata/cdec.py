from datetime import datetime, timezone, timedelta
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import logging

from geopandas import GeoDataFrame

from .base import PointData
from ..variables import CdecStationVariables, SensorDescription
from ..dataframe_utils import append_df, merge_df, resample_whole_df

LOG = logging.getLogger("metloom.pointdata.cdec")


class CDECPointData(PointData):
    """
    Implement PointData methods for CDEC data source
    API documentation here https://cdec.water.ca.gov/dynamicapp/
    """

    ALLOWED_VARIABLES = CdecStationVariables
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
    META_URL = "https://cdec.water.ca.gov/dynamicapp/staMeta"
    DATASOURCE = "CDEC"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(CDECPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None
        # CDEC has datetimes that aren't found in US/Pacific. Use this instead
        self._tzinfo = timezone(timedelta(hours=-8.0))

    def _parse_sensor_table(self, df):
        expected_cols = [
            "Sensor Description", "Sensor Number", "Duration",
            "Plot", "Data Collection", "Data Available"
        ]

        df_sensors = None

        if len(df.columns) == len(expected_cols):
            df_sensors = pd.DataFrame(
                df.values, columns=[
                    "Sensor Description", "Sensor Number", "Duration",
                    "Plot", "Data Collection", "Data Available"
                ]
            )
            df_sensors["Duration"] = df_sensors["Duration"].map(
                lambda x: x.replace("(", "").replace(")", "").strip()
            )
            duration_values = df_sensors["Duration"].values
            if not any(
                [k in duration_values for k in
                 ["monthly", "daily", "hourly", "event"]]
            ):
                df_sensors = None

        # If we didn't find any valid durations, we have the wrong table
        return df_sensors

    def _parse_meta_page(self, df):
        result = {}
        # restructure the dataframes into a usable format
        df_loc = df[0]
        df1 = df_loc.iloc[:, :2].transpose()
        df2 = df_loc.iloc[:, 2:].transpose()
        result["location"] = pd.DataFrame(
            df1.values[1:], columns=df1.iloc[0]
        ).join(
            pd.DataFrame(df2.values[1:], columns=df2.iloc[0])
        )
        # Make sure we read the expected table
        if "Longitude" not in result["location"].columns.values:
            LOG.error(result["location"])
            raise RuntimeError(f"Failed parsing metadata for {self.id}")

        # parse and cleanup the sensor info
        df_sensors = self._parse_sensor_table(df[1])
        if df_sensors is None:
            df_sensors = self._parse_sensor_table(df[2])
            if df_sensors is None:
                LOG.error(f"Failed to find sensor info for {self.id}")
                raise RuntimeError(f"Failed to find sensor info for {self.id}")

        result["sensors"] = df_sensors

        return result

    def _get_all_metadata(self):
        """
        Get all the raw metadata for a station. This is a list of sensor
        descriptions for the station
        Returns:
            A list of dictionaries describing the sensors at a station
        """
        if self._raw_metadata is None:
            url = self.META_URL + f"?station_id={self.id}"
            df = pd.read_html(url)
            self._raw_metadata = self._parse_meta_page(df)
        return self._raw_metadata

    def _check_snowcourse_sensors(self):
        """
        Returns a list of booleans for snow sensors that checks if they
        are monthly in cadence
        """
        snow_sensors = [18, 3, 82]
        data = self._get_all_metadata()
        sensors = data["sensors"]
        manual_check = []
        for d, num in zip(sensors["Duration"], sensors["Sensor Number"]):
            if int(num) in snow_sensors:
                manual_check.append(d == "monthly")
        return manual_check

    def is_only_snow_course(self, variables: List[SensorDescription]):
        """
        Determine if a station only has snow course measurements
        """
        manual_check = self._check_snowcourse_sensors()

        result = False
        if len(manual_check) > 0 and all(manual_check):
            result = True
        if result and not self.is_only_monthly():
            # This would happen if all snow sensors have code "M"
            # but there are other hourly or daily sensors
            if any(
                [sensor not in [
                    self.ALLOWED_VARIABLES.SWE, self.ALLOWED_VARIABLES.SNOWDEPTH
                ] for sensor in variables]
            ):
                # this is an acceptable scenario where we are looking for
                # more than just snow variables and the station has them
                result = False
            else:
                # We are only looking for snow variables and this only has
                # monthly snow variables so it is effectively only
                # a snow course
                result = True
        return result

    def is_partly_snow_course(self):
        """
        Determine if any of the snow sensors at a station are on a monthly
        interval
        Assumption: Monthly snow sensor measurements are snow courses
        """
        result = self._check_snowcourse_sensors()
        return any(result)

    def is_only_monthly(self):
        """
        determine if all sensors for a station are on a monthly interval
        """
        data = self._get_all_metadata()
        duration = data["sensors"]["Duration"].values
        manual_check = [d == "monthly" for d in duration]
        if len(manual_check) > 0 and all(manual_check):
            return True
        return False

    @staticmethod
    def _parse_str_deg(value):
        return float(value.replace("°", ""))

    @staticmethod
    def _parse_elevation(value):
        num, unit = value.split(" ")
        if unit.lower() not in ["ft", "feet"]:
            raise RuntimeError("Unexpected unit for elevation")
        return float(num)

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        data = self._get_all_metadata()
        location_data = data["location"].iloc[0]

        return gpd.points_from_xy(
            [self._parse_str_deg(location_data["Longitude"])],
            [self._parse_str_deg(location_data["Latitude"])],
            z=[self._parse_elevation(location_data["Elevation"])],
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

    def _sensor_response_to_df(self, response_data, sensor, final_columns,
                               resample_duration=None):
        """
        Convert the response data from the API to a GeoDataFrame
        Format and map columns in the dataframe
        Args:
            response_data: JSON list response from CDEC API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
            resample_duration: duration to resample to
        Returns:
            GeoDataFrame
        """
        sensor_df = gpd.GeoDataFrame.from_dict(
            response_data,
            geometry=[self.metadata] * len(response_data),
        )
        sensor_df.replace(-9999.0, np.nan, inplace=True)
        # this mapping is important. Sometimes obsDate is null
        column_map = {
            "date": "datetime",
            "value": sensor.name,
            "units": f"{sensor.name}_units",
            "stationId": "site",
        }
        if "measurementDate" in final_columns:
            column_map["obsDate"] = "measurementDate"

        sensor_df.rename(
            columns=column_map,
            inplace=True,
        )
        final_columns += [sensor.name, f"{sensor.name}_units"]
        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])

        # resample if necessary
        if resample_duration:
            sensor_df = resample_whole_df(
                sensor_df.set_index("datetime"), sensor,
                interval=resample_duration
            ).reset_index()
            sensor_df = GeoDataFrame(sensor_df, geometry=sensor_df["geometry"])

        sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)
        if "measurementDate" in sensor_df.columns:
            sensor_df["measurementDate"] = pd.to_datetime(sensor_df["measurementDate"])
            sensor_df["measurementDate"] = sensor_df["measurementDate"].apply(
                self._handle_df_tz
            )
        # set index so joining works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        sensor_df = sensor_df.loc[pd.notna(sensor_df[sensor.name])]
        return sensor_df

    def _get_data_fallback(self, params, duration_list):
        """
        Allow for fallback on finer resolution API durations with resample
        if the desired duration does not return data
        Args:
            params: request params with or without dur_code
            duration_list: list of durations to try. First index is desired
                durations
        """
        if len(duration_list) < 1:
            raise ValueError("Duration list cannot be empty")
        response_data = []
        df_duration = duration_list[0]
        for duration in duration_list:
            params["dur_code"] = duration
            response_data = self._data_request(params)
            if response_data:
                df_duration = duration
                break
        return response_data, df_duration

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        duration_list: List[str],
        include_measurement_date=False
    ):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            duration_list: CDEC duration code and fallbacks ['D', 'H', 'E']
            include_measurement_date: boolean for including the
                'measurmentDate' column in the resulting dataframe. This column
                is only relevant for snow courses
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """
        params = {
            "Stations": self.id,
            "Start": start_date.isoformat(),
            "End": end_date.isoformat(),
        }
        df = None
        final_columns = ["geometry", "site"]
        desired_duration = duration_list[0]
        if include_measurement_date:
            final_columns += ["measurementDate"]
        for sensor in variables:
            params["SensorNums"] = sensor.code
            response_data, response_duration = self._get_data_fallback(
                params, duration_list
            )
            if response_data:
                # don't resample if we have the desired duration
                if response_duration == desired_duration:
                    resample_duration = None
                else:
                    resample_duration = desired_duration
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns,
                    resample_duration=resample_duration
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

    def get_event_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        return self._get_data(start_date, end_date, variables, ["E"])

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data
        Example query:
        https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet?
        Stations=TNY&SensorNums=3&dur_code=D&Start=2021-05-16&End=2021-05-16
        """
        return self._get_data(start_date, end_date, variables, ["D", "H", "E"])

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        return self._get_data(start_date, end_date, variables, ["H", "E"])

    def get_snow_course_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_snow_course_data
        """
        if not self.is_partly_snow_course():
            raise ValueError(f"{self.id} is not a snow course")
        return self._get_data(
            start_date, end_date, variables, ["M"], include_measurement_date=True
        )

    @staticmethod
    def _station_sensor_search(
        bounds, sensor: SensorDescription, dur=None, collect=None,
        buffer=0.0
    ):
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
        dur_str = f"&dur_chk=on&dur={dur}" if dur else "&dur="
        collect_str = (
            f"&collect_chk=on&collect={collect}"
            if collect
            else "&collect=NONE+SPECIFIED"
        )
        url = (
            f"https://cdec.water.ca.gov/dynamicapp/staSearch?sta="
            f"&sensor_chk=on&sensor={sensor.code}"
            f"{collect_str}"
            f"{dur_str}"
            f"&active_chk=on&active=Y"
            f"&loc_chk=on"
            f"&lon1={bounds['minx']-buffer}&lon2={bounds['maxx']+buffer}"
            f"&lat1={bounds['miny']-buffer}&lat2={bounds['maxy']+buffer}"
            f"&elev1=-5&elev2=99000&nearby=&basin=NONE+SPECIFIED"
            f"&hydro=NONE+SPECIFIED&county=NONE+SPECIFIED&agency_num=160"
            f"&display=sta"
        )
        try:
            return pd.read_html(url)[0]
        except ValueError:
            LOG.error(f"No tables for {url}")
            return None

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
            snow_courses: Boolean for including only snowcourse data or no
            snowcourse data
            within_geometry: filter the points to within the shapefile
            instead of just the extents. Default True
            buffer: buffer added to search box

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

        # Filter to manual, monthly measurements if looking for snow courses
        if kwargs['snow_courses']:
            station_search_kwargs["dur"] = "M"
            station_search_kwargs["collect"] = "MANUAL+ENTRY"
        for variable in variables:
            result_df = cls._station_sensor_search(
                bounds, variable, buffer=kwargs["buffer"],
                **station_search_kwargs
            )
            if result_df is not None:
                result_df["index_id"] = result_df["ID"]
                result_df.set_index("index_id", inplace=True)
                search_df = append_df(
                    search_df, result_df
                ).drop_duplicates(subset=['ID'])
        # return empty collection if we didn't find any points
        if search_df is None:
            return cls.ITERATOR_CLASS([])
        clms = search_df.columns.values
        if "ElevationFeet" in clms:
            elev_key = "ElevationFeet"
        elif "Elevation Feet" in clms:
            elev_key = "Elevation Feet"
        else:
            raise RuntimeError("No key for elevation")
        gdf = gpd.GeoDataFrame(
            search_df,
            geometry=gpd.points_from_xy(
                search_df["Longitude"],
                search_df["Latitude"],
                z=search_df[elev_key],
            ),
        )
        # filter to points within shapefile
        if kwargs['within_geometry']:
            filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        else:
            filtered_gdf = gdf

        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf.index,
                filtered_gdf["Station Name"],
                filtered_gdf["geometry"],
            )
        ]
        # filter to snow courses or not snowcourses depending on desired result
        if kwargs['snow_courses']:
            return cls.ITERATOR_CLASS([p for p in points if p.is_partly_snow_course()])
        else:
            return cls.ITERATOR_CLASS(
                [p for p in points if not p.is_only_snow_course(variables)]
            )

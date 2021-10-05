from datetime import datetime
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import pytz
import requests
import logging

from .base import PointData
from ..variables import CdecStationVariables, SensorDescription
from ..dataframe_utils import join_df

LOG = logging.getLogger("metloom.pointdata.cdec")


class CDECPointData(PointData):
    """
    Implement PointData methods for CDEC data source
    API documentation here https://cdec.water.ca.gov/dynamicapp/
    """

    ALLOWED_VARIABLES = CdecStationVariables
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
    META_URL = "http://cdec.water.ca.gov/cdecstation2/CDecServlet/" "getStationInfo"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(CDECPointData, self).__init__(station_id, name, metadata=metadata)
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
            resp = requests.get(self.META_URL, params={"stationID": self.id})
            resp.raise_for_status()
            self._raw_metadata = resp.json()["STATION"]
        return self._raw_metadata

    def is_only_snow_course(self):
        """
        Determine if a station only has snow course measurements
        """
        data = self._get_all_metadata()
        manual_check = [d["DUR_CODE"] == "M" for d in data if d["SENS_GRP"] == "snow"]
        result = False
        if len(manual_check) > 0 and all(manual_check):
            result = True
        if result and not self.is_only_monthly():
            # This would happen if all snow sensors have code "M"
            # but there are other hourly or daily sensors
            raise Exception(
                f"We have not accounted for this scenario. Please talk to "
                f"a Micah about how {self.id} violates their assumptions."
            )
        return result

    def is_partly_snow_course(self):
        """
        Determine if any of the snow sensors at a station are on a monthly
        interval
        Assumption: Monthly snow sensor measurements are snow courses
        """
        data = self._get_all_metadata()
        return any([d["DUR_CODE"] == "M" for d in data if d["SENS_GRP"] == "snow"])

    def is_only_monthly(self):
        """
        determine if all sensors for a station are on a monthly interval
        """
        data = self._get_all_metadata()
        manual_check = [d["DUR_CODE"] == "M" for d in data]
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
        for choice in [
            "SNOW, WATER CONTENT",
            "SNOW DEPTH",
            "PRECIPITATION, ACCUMULATED",
        ]:
            if metadata_by_name.get(choice) is not None:
                chosen_sensor_data = metadata_by_name[choice]
                break

        return gpd.points_from_xy(
            [chosen_sensor_data["LONGITUDE"]],
            [chosen_sensor_data["LATITUDE"]],
            z=[chosen_sensor_data["ELEVATION"]],
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
        sensor_df.replace(-9999.0, np.nan, inplace=True)
        # this mapping is important. Sometimes obsDate is null
        sensor_df.rename(
            columns={
                "date": "datetime",
                "obsDate": "measurementDate",
                "value": sensor.name,
                "units": f"{sensor.name}_units",
                "stationId": "site",
            },
            inplace=True,
        )
        final_columns += [sensor.name, f"{sensor.name}_units"]
        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
        sensor_df["measurementDate"] = pd.to_datetime(sensor_df["measurementDate"])
        sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)
        sensor_df["measurementDate"] = sensor_df["measurementDate"].apply(
            self._handle_df_tz
        )
        # set index so joinng works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        return sensor_df

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        duration: str,
    ):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            duration: CDEC duration code ['M', 'H', 'D']
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """
        params = {
            "Stations": self.id,
            "dur_code": duration,
            "Start": start_date.isoformat(),
            "End": end_date.isoformat(),
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
        return self._get_data(start_date, end_date, variables, "D")

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        return self._get_data(start_date, end_date, variables, "H")

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
        return self._get_data(start_date, end_date, variables, "M")

    @staticmethod
    def _station_sensor_search(
        bounds, sensor: SensorDescription, dur=None, collect=None
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
        # TODO: do we want this buffer?
        buffer = 0.00
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
        snow_courses=False,
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
        gdf = gpd.GeoDataFrame(
            search_df,
            geometry=gpd.points_from_xy(
                search_df["Longitude"],
                search_df["Latitude"],
                z=search_df["ElevationFeet"],
            ),
        )
        # filter to points within shapefile
        # TODO: do we want to make this optional?
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]

        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf["ID"],
                filtered_gdf["Station Name"],
                filtered_gdf["geometry"],
            )
        ]
        # filter to snow courses or not snowcourses depending on desired result
        if snow_courses:
            return cls.ITERATOR_CLASS([p for p in points if p.is_partly_snow_course()])
        else:
            return cls.ITERATOR_CLASS(
                [p for p in points if not p.is_only_snow_course()]
            )

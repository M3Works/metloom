from datetime import datetime
from typing import List

import geopandas as gpd
import pandas as pd
import pytz
import requests
import logging

from .dataframe_utils import join_df
from .variables import VariableBase, CdecStationVariables, SensorDescription

LOG = logging.getLogger("dataloom.point_data")

"""
Maybe a pandas table read on https://cdec.water.ca.gov/reportapp/javareports?name=COURSES.202104
for snow survey?
Where are the locations?


Maybe a class for variables with a list of preferred codes for how to access the variables
Or an enum. Could have met enum and snow enum (or class) for each point data class
"""


class PointDataCollection:
    def __init__(self, points: List[object] = None):
        self.points = points or []
        self._index = 0

    def add_point(self, point: object):
        self.points.append(point)

    def to_dataframe(self):
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
        self.id = station_id
        self.name = name
        self._metadata = metadata

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        raise NotImplementedError("get_daily_data is not implemented")

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        raise NotImplementedError("get_hourly_data is not implemented")

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        raise NotImplementedError("get_snow_course_data is not implemented")

    def _get_metadata(self):
        raise NotImplementedError("_get_metadata is not implemented")

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self._get_metadata()
        return self._metadata

    def points_from_geometry(self, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription]):
        raise NotImplementedError("points_from_geometry not implemented")

    def __repr__(self):
        return f'{self.__class__.__name__}({self.id!r}, {self.name!r})'

    def __str__(self):
        return f'{self.name} ({self.id})'


class CDECPointData(PointData):
    """
    Sample data using CDEC API
    API documentation here https://cdec.water.ca.gov/dynamicapp/
    """
    TZINFO = pytz.timezone("US/Pacific")
    # TODO: are these mappings static?
    # TODO: should we tailor this to each station based on metadata sensor returns?
    ALLOWED_VARIABLES = CdecStationVariables
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
    META_URL = "http://cdec.water.ca.gov/cdecstation2/CDecServlet/" \
               "getStationInfo"

    def __init__(self, station_id, name, metadata=None):
        super(CDECPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None

    def _get_all_metadata(self):
        if self._raw_metadata is None:
            resp = requests.get(self.META_URL, params={'stationID': self.id})
            resp.raise_for_status()
            self._raw_metadata = resp.json()["STATION"]
        return self._raw_metadata

    def is_only_snow_course(self):
        data = self._get_all_metadata()
        # TODO: is M monthly or manual?
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
        data = self._get_all_metadata()
        return any(
            [d["DUR_CODE"] == "M" for d in data if d['SENS_GRP'] == "snow"]
        )

    def is_only_monthly(self):
        data = self._get_all_metadata()
        manual_check = [
            d["DUR_CODE"] == "M" for d in data
        ]
        if len(manual_check) > 0 and all(manual_check):
            return True
        return False

    def _get_metadata(self):
        # TODO: Elevation
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
        resp = requests.get(self.CDEC_URL, params=params)
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def _handle_df_tz(cls, val):
        if pd.isna(val):
            return val
        else:
            local = val.tz_localize(cls.TZINFO)
            return local.tz_convert("UTC")

    def _sensor_response_to_df(self, response_data, sensor, final_columns):
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
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        return sensor_df

    def _get_data(self, start_date: datetime, end_date: datetime,
                  variables: List[SensorDescription], duration: str):
        # TODO: should we scrape the data like we do in firn?
        # Example would be the table here https://cdec.water.ca.gov/dynamicapp/QueryDaily?s=CVM&end=2021-09-17
        # The data is cleaner, but it is probably a more brittle approach
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
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        """
        Example query: https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet?Stations=TNY&SensorNums=3&dur_code=D&Start=2021-05-16&End=2021-05-16
        """
        return self._get_data(start_date, end_date, variables, "D")

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        """
        """
        return self._get_data(start_date, end_date, variables, "H")

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        """
        Another approach could be https://cdec.water.ca.gov/dynamicapp/snowQuery?course_num=PRK&month=April&start_date=2021&end_date=2021&data_wish=HTML
        # TODO: verify approaches are the same
        """
        if not self.is_partly_snow_course():
            raise ValueError(f"{self.id} is not a snow course")
        return self._get_data(start_date, end_date, variables, "M")

    @staticmethod
    def _station_sensor_search(bounds, sensor: SensorDescription, dur=None):
        """
        Station search form https://cdec.water.ca.gov/dynamicapp/staSearch?
        """
        # TODO: do we want this buffer?
        buffer = 0.00
        # TODO: filter to active status?
        # TODO: Can filter collection type for snowcourses. i.e. collect=MANUAL+ENTRY
        # TODO: can also filter Duration for monthly when requesting snowcourse
        # &collect_chk=on&collect=MANUAL+ENTRY
        # &dur_chk=on&dur=H
        # &active_chk=on&active=Y
        # &collect_chk=on&collect=NONE+SPECIFIED
        # f"&sensor_chk=on&sensor={sensor.code}" \
        dur_str = f"&dur_chk=on&dur={dur}" if dur else "&dur="
        url = f"https://cdec.water.ca.gov/dynamicapp/staSearch?sta=" \
              f"&sensor_chk=on&sensor={sensor.code}" \
              f"&collect=NONE+SPECIFIED" \
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
        except ValueError as e:
            LOG.error(f"No tables for {url}")
            return None

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription],
                             snow_courses=False
                             ):
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        search_df = None
        for variable in variables:
            result_df = cls._station_sensor_search(bounds, variable)
            if result_df is not None:
                result_df["index_id"] = result_df["ID"]
                result_df.set_index("index_id")
                search_df = join_df(search_df, result_df, how="outer")
        if search_df is None:
            return []
        gdf = gpd.GeoDataFrame(search_df, geometry=gpd.points_from_xy(
            search_df["Longitude"], search_df["Latitude"],
            z=search_df["ElevationFeet"]
        ))
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]['geometry'])]

        points = [
            cls(row[0], row[1], metadata=row[2]) for row in zip(
                filtered_gdf['ID'], filtered_gdf["Station Name"],
                filtered_gdf["geometry"]
            )
        ]
        if snow_courses:
            return cls.ITERATOR_CLASS(
                [p for p in points if p.is_partly_snow_course()]
            )
        else:
            return cls.ITERATOR_CLASS(
                [p for p in points if not p.is_only_snow_course()]
            )

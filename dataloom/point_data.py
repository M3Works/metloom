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


class PointData:
    ALLOWED_VARIABLES = VariableBase

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


class PointDataCollection:
    def __init__(self, points: List[PointData] = None):
        self.points = points or []
        self._index = 0

    def add_point(self, point: PointData):
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


class CDECStation(PointData):
    TZINFO = pytz.timezone("US/Pacific")
    # TODO: are these mappings static?
    # TODO: should we tailor this to each station based on metadata sensor returns?
    ALLOWED_VARIABLES = CdecStationVariables
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
    META_URL = "http://cdec.water.ca.gov/cdecstation2/CDecServlet/" \
               "getStationInfo"

    def __init__(self, station_id, name, metadata=None):
        super(CDECStation, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None

    def _get_all_metadata(self):
        if self._raw_metadata is None:
            resp = requests.get(self.META_URL, params={'stationID': self.id})
            resp.raise_for_status()
            self._raw_metadata = resp.json()["STATION"]
        return self._raw_metadata

    def is_only_snow_course(self):
        data = self._get_all_metadata()
        manual_check = [
            d["DUR_CODE"] == "M" for d in data if d['SENS_GRP'] == "snow"
        ]
        result = False
        if len(manual_check) > 0 and all(manual_check):
            result = True
        if result and not self.is_only_manual():
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

    def is_only_manual(self):
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
        final_columns = ["geometry", "site"]
        for sensor in variables:
            params["SensorNums"] = sensor.code
            response_data = self._data_request(params)
            if response_data:
                sensor_df = gpd.GeoDataFrame.from_dict(
                    response_data,
                    geometry=[self.metadata] * len(response_data)
                )
                sensor_df.rename(columns={
                    "date": "datetime",
                    "value": sensor.name,
                    "units": f"{sensor.name}_units",
                    "stationId": "site"
                },
                    inplace=True)
                final_columns += [sensor.name, f"{sensor.name}_units"]
                sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
                sensor_df.set_index("datetime", inplace=True)
                df = join_df(df, sensor_df)
                df = df.filter(final_columns)
        # add a units column
        if df is not None and len(df.index) > 0:
            df.index = df.index.tz_localize(self.TZINFO)
            df.index = df.index.tz_convert("UTC")
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
        """
        if not self.is_partly_snow_course():
            raise ValueError(f"{self.id} is not a snow course")
        return self._get_data(start_date, end_date, variables, "M")

    @staticmethod
    def _station_sensor_search(bounds, sensor: SensorDescription):
        # TODO: filter to active status?
        url = f"https://cdec.water.ca.gov/dynamicapp/staSearch?sta=" \
              f"&sensor={sensor.code}" \
              f"&collect=NONE+SPECIFIED&dur=&active=&loc_chk=on" \
              f"&lon1={bounds['minx']}&lon2={bounds['maxx']}" \
              f"&lat1={bounds['miny']}&lat2={bounds['maxy']}" \
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
            return PointDataCollection(
                [p for p in points if p.is_partly_snow_course()]
            )
        else:
            return PointDataCollection(
                [p for p in points if not p.is_only_snow_course()]
            )

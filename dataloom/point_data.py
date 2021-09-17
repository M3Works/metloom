from datetime import datetime
import geopandas as gpd
import pandas as pd
import pytz
import requests

"""
Maybe a pandas table read on https://cdec.water.ca.gov/reportapp/javareports?name=COURSES.202104
for snow survey?
Where are the locations?


Maybe a class for variables with a list of preferred codes for how to access the variables
Or an enum. Could have met enum and snow enum (or class) for each point data class
"""


class PointData:
    AVAILABLE_VARIABLES = []
    VARIABLE_TO_API_MAPPING = {}

    def __init__(self, station_id, name, metadata=None):
        self.id = station_id
        self.name = name
        self._metadata = metadata

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variable: str):
        raise NotImplementedError("get_daily_data is not implemented")

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variable: str):
        raise NotImplementedError("get_hourly_data is not implemented")

    def _get_metadata(self):
        raise NotImplementedError("_get_metadata is not implemented")

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self._get_metadata()
        return self._metadata

    def points_from_geometry(self, geometry: gpd.GeoDataFrame):
        raise NotImplementedError("points_from_geometry not implemented")


class CDECStation(PointData):
    TZINFO = pytz.timezone("US/Pacific")
    AVAILABLE_VARIABLES = []
    # TODO: are these mappings static?
    VARIABLE_TO_SENSOR_MAPPING = {
        "snow_depth": 18,
        "swe": 3,
        "air_temp": 4,
        "precip": 2
    }
    CDEC_URL = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"

    def __init__(self, station_id, name, metadata=None):
        super(CDECStation, self).__init__(station_id, name, metadata=metadata)

    def _get_all_metadata(self):
        url = "http://cdec.water.ca.gov/cdecstation2/CDecServlet/getStationInfo"
        resp = requests.get(url, params={'stationID': self.id})
        resp.raise_for_status()
        return resp.json()["STATION"]

    def _get_metadata(self):
        # TODO: Elevation
        data = self._get_all_metadata()
        # TODO: Should this be sensor specific?
        metadata_by_name = {d["SENS_LONG_NAME"]: d for d in data}
        # default to the first sensor
        chosen_sensor_data = data[0]
        # try to replace it with a desired sensor
        for choice in ["SNOW, WATER CONTENT", "SNOW DEPTH",
                       "PRECIPITATION, ACCUMULATED"]:
            if metadata_by_name.get(choice) is not None:
                chosen_sensor_data = metadata_by_name[choice]
                break

        return gpd.points_from_xy(
            [chosen_sensor_data["LONGITUDE"]], [chosen_sensor_data["LATITUDE"]]
        )[0]

    def _get_data(self, start_date: datetime, end_date: datetime,
                  variable: str, duration: str):
        # TODO: should we scrape the data like we do in firn?
        # Example would be the table here https://cdec.water.ca.gov/dynamicapp/QueryDaily?s=CVM&end=2021-09-17
        # The data is cleaner, but it is probably a more brittle approach
        sensor_num = self.VARIABLE_TO_SENSOR_MAPPING[variable]
        params = {
            "Stations": self.id,
            "SensorNums": sensor_num,
            "dur_code": duration,
            "Start": start_date.isoformat(),
            "End": end_date.isoformat()
        }
        resp = requests.get(self.CDEC_URL, params=params)
        resp.raise_for_status()
        response_data = resp.json()
        df = gpd.GeoDataFrame.from_dict(
            response_data,
            geometry=[self.metadata] * len(response_data)
        )
        df.rename(columns={
            "date": "datetime",
            "value": variable,
            "units": f"{variable}_units"},
            inplace=True)
        # add a units column
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        if len(df.index) > 0:
            df.index = df.index.tz_localize(self.TZINFO)
            df.index = df.index.tz_convert("UTC")
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variable: str):
        """
        Example query: https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet?Stations=TNY&SensorNums=3&dur_code=D&Start=2021-05-16&End=2021-05-16
        """
        return self._get_data(start_date, end_date, variable, "D")

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variable: str):
        """
        """
        return self._get_data(start_date, end_date, variable, "H")

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame):
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        # TODO: sensor filtering
        url = f"https://cdec.water.ca.gov/dynamicapp/staSearch?sta=" \
              f"" \
              f"&collect=NONE+SPECIFIED&dur=&active=&loc_chk=on" \
              f"&lon1={bounds['minx']}&lon2={bounds['maxx']}" \
              f"&lat1={bounds['miny']}&lat2={bounds['maxy']}" \
              f"&elev1=-5&elev2=99000&nearby=&basin=NONE+SPECIFIED" \
              f"&hydro=NONE+SPECIFIED&county=NONE+SPECIFIED&agency_num=160" \
              f"&display=sta"

        search_df = pd.read_html(url)[0]
        gdf = gpd.GeoDataFrame(search_df, geometry=gpd.points_from_xy(
            search_df["Longitude"], search_df["Latitude"],
            z=search_df["ElevationFeet"]
        ))
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]['geometry'])]

        return [
            cls(row[0], row[1], row[2]) for row in zip(
                filtered_gdf['ID'], filtered_gdf["Station Name"],
                filtered_gdf["geometry"]
            )
        ]

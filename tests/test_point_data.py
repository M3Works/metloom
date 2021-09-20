import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import geopandas as gpd
import pandas as pd

from dataloom.point_data import CDECStation
from dataloom.dataframe_utils import join_df, append_df
from dataloom.variables import CdecStationVariables


class TestCDECStation(object):
    @staticmethod
    def cdec_daily_response():
        return [
            {
                "stationId": "TNY", "durCode": "D", "SENSOR_NUM": 3,
                "sensorType": "SNOW WC", "date": "2021-5-16 00:00",
                "obsDate": "2021-5-16 00:00", "value": -0.11, "dataFlag": " ",
                "units": "INCHES"},
            {
                "stationId": "TNY", "durCode": "D", "SENSOR_NUM": 3,
                "sensorType": "SNOW WC", "date": "2021-5-17 00:00",
                "obsDate": "2021-5-17 00:00", "value": -0.10, "dataFlag": " ",
                "units": "INCHES"},
            {
                "stationId": "TNY", "durCode": "D", "SENSOR_NUM": 3,
                "sensorType": "SNOW WC", "date": "2021-5-18 00:00",
                "obsDate": "2021-5-18 00:00", "value": -0.10, "dataFlag": " ",
                "units": "INCHES"},
        ]

    @pytest.fixture(scope="class")
    def tny_station(self):
        return CDECStation("TNY", "Tenaya Lake")

    @pytest.fixture(scope="class")
    def tny_daily_expected(self):
        points = gpd.points_from_xy([-119.0], [42.0])
        df = gpd.GeoDataFrame.from_dict(
            [
                {'datetime': pd.Timestamp(
                    '2021-05-16 07:00:00+0000', tz='UTC'),
                 'PRECIPITATION': -0.11, 'PRECIPITATION_units': 'INCHES',
                 'site': 'TNY'},
                {'datetime': pd.Timestamp(
                    '2021-05-17 07:00:00+0000', tz='UTC'),
                 'PRECIPITATION': -0.10, 'PRECIPITATION_units': 'INCHES',
                 'site': 'TNY'},
                {'datetime': pd.Timestamp(
                    '2021-05-18 07:00:00+0000', tz='UTC'),
                 'PRECIPITATION': -0.10, 'PRECIPITATION_units': 'INCHES',
                 'site': 'TNY'},
            ],
            geometry=[points[0]] * 3,
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter([
            "datetime", "geometry", "site", "PRECIPITATION",
            "PRECIPITATION_units"]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    @classmethod
    def tny_side_effect(cls, url, **kwargs):
        mock = MagicMock()
        if kwargs["params"].get("dur_code") == "D":
            mock.json.return_value = cls.cdec_daily_response()
        elif kwargs["params"].get("dur_code") == "H":
            raise NotImplementedError()
        elif "getStationInfo" in url:
            mock.json.return_value = {
                "STATION": [{
                    "SENS_LONG_NAME": "SNOW, WATER CONTENT",
                    "ELEVATION": 1000.0,
                    "LATITUDE": 42.0,
                    "LONGITUDE": -119.0
                }]
            }
        else:
            raise ValueError("unknown scenario")
        return mock


    def test_get_metadata(self, tny_station):
        with patch("dataloom.point_data.requests") as mock_requests:
            mock_requests.get.side_effect = self.tny_side_effect
            metadata = tny_station.metadata
            mock_get = mock_requests.get
            mock_get.call_count == 1
            mock_get.assert_called_with(
                "http://cdec.water.ca.gov/cdecstation2/CDecServlet/"
                "getStationInfo",
                params={'stationID': 'TNY'}
            )
        expected = gpd.points_from_xy([-119.0], [42.0])[0]
        assert expected == metadata

    def test_get_daily_data(self, tny_station, tny_daily_expected):
        with patch("dataloom.point_data.requests") as mock_requests:
            mock_requests.get.side_effect = self.tny_side_effect
            response = tny_station.get_daily_data(
                datetime(2021, 5, 16), datetime(2021, 5, 18),
                [CdecStationVariables.PRECIPITATION]
            )
            mock_get = mock_requests.get
            mock_get.assert_any_call(
                "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet",
                params={'Stations': 'TNY', 'dur_code': 'D',
                        'Start': '2021-05-16T00:00:00',
                        'End': '2021-05-18T00:00:00', 'SensorNums': '2'}
            )
            mock_get.call_count == 2
        pd.testing.assert_frame_equal(response, tny_daily_expected)

    def test_points_from_geometry(self):
        pass


# def test_cdec_station():
#     st = CDECStation("TNY", None)
#     data = st.get_daily_data(
#         datetime(2020, 2, 1), datetime(2020, 2, 10),
#         [st.ALLOWED_VARIABLES.PRECIPITATION, st.ALLOWED_VARIABLES.SWE]
#     )
#     st.metadata
#
#
# def test_search_stations():
#     # TODO: test that varaible order doesn't affect the metadata
#     fp = "/Users/micahsandusky/projects/m3works/data_from_aso/Tuol_subbasins/hetchy_subbasin.shp"
#     obj = gpd.read_file(fp)
#     points = CDECStation.points_from_geometry(obj, [CdecStationVariables.PRECIPITATION,
#                                                     # CdecStationVariables.SWE
#                                                     ]
#                                               )
#     print(points)
#
#
# def test_stations():
#     fp = "/Users/micahsandusky/projects/m3works/data_from_aso/Tuol_subbasins/hetchy_subbasin.shp"
#     obj = gpd.read_file(fp)
#     points = CDECStation.points_from_geometry(obj, [
#         CDECStation.ALLOWED_VARIABLES.PRECIPITATION,
#         CDECStation.ALLOWED_VARIABLES.TEMPERATURE
#     ])
#     df = None
#     # TODO: this is only returning temperature
#     for point in points:
#         df_new = point.get_daily_data(
#             datetime(2020, 2, 1), datetime(2020, 2, 10),
#             [point.ALLOWED_VARIABLES.PRECIPITATION,
#              point.ALLOWED_VARIABLES.TEMPERATURE]
#         )
#
#         df = append_df(df, new_df=df_new)
#     # sort by dates
#     df.sort_index(level=0, inplace=True)
#     print(df)

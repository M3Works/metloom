from datetime import timezone, timedelta, datetime
from unittest.mock import MagicMock, patch
import re

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import shapely
from pandas import Timestamp
from pathlib import Path

from metloom.pointdata import CDECPointData, PointDataCollection
from metloom.variables import CdecStationVariables
from tests.test_point_data import BasePointDataTest, side_effect_error


class TestCDECStation(BasePointDataTest):
    CDEC_MOCKS_DIR = Path(__file__).parent.joinpath("data/cdec_mocks")

    @staticmethod
    def cdec_daily_precip_response():
        return [
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 2,
                "sensorType": "SNOW WC",
                "date": "2021-5-16 00:00",
                "obsDate": "2021-5-16 00:00",
                "value": -0.11,
                "dataFlag": " ",
                "units": "INCHES",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 2,
                "sensorType": "SNOW WC",
                "date": "2021-5-17 00:00",
                "obsDate": "2021-5-17 00:00",
                "value": -0.10,
                "dataFlag": " ",
                "units": "INCHES",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 2,
                "sensorType": "SNOW WC",
                "date": "2021-5-18 00:00",
                "obsDate": "2021-5-18 00:00",
                "value": -0.10,
                "dataFlag": " ",
                "units": "INCHES",
            },
        ]

    @staticmethod
    def cdec_daily_temp_response():
        return [
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-15 00:00",
                "obsDate": "2021-5-15 00:00",
                "value": 2.1,
                "dataFlag": " ",
                "units": "DEG F",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-17 00:00",
                "obsDate": "2021-5-17 00:00",
                "value": 2.4,
                "dataFlag": " ",
                "units": "DEG F",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-18 00:00",
                "obsDate": "2021-5-18 00:00",
                "value": 2.2,
                "dataFlag": " ",
                "units": "DEG F",
            },
        ]

    @staticmethod
    def cdec_hourly_temp_response():
        return [
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-15 00:00",
                "obsDate": "2021-5-15 00:00",
                "value": 2.1,
                "dataFlag": " ",
                "units": "DEG F",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-15 01:00",
                "obsDate": "2021-5-15 01:00",
                "value": 2.4,
                "dataFlag": " ",
                "units": "DEG F",
            },
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 30,
                "sensorType": "SNOW WC",
                "date": "2021-5-15 03:00",
                "obsDate": "2021-5-15 03:00",
                "value": 2.2,
                "dataFlag": " ",
                "units": "DEG F",
            },
        ]

    @pytest.fixture(scope="function")
    def tny_station(self):
        return CDECPointData("TNY", "Tenaya Lake")

    @pytest.fixture(scope="class")
    def expected_points(self):
        return gpd.points_from_xy([-119.448], [37.838], z=[1000.0])

    @pytest.fixture(scope="class")
    def tny_daily_expected(self, expected_points):
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2021-05-15 08:00:00+0000",
                                             tz="UTC"),
                    "ACCUMULATED PRECIPITATION": np.nan,
                    "ACCUMULATED PRECIPITATION_units": np.nan,
                    "AVG AIR TEMP": 2.1,
                    "AVG AIR TEMP_units": "DEG F",
                    "site": "TNY",
                    "datasource": "CDEC"
                },
                {
                    "datetime": pd.Timestamp("2021-05-16 08:00:00+0000", tz="UTC"),
                    "ACCUMULATED PRECIPITATION": -0.11,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "AVG AIR TEMP": np.nan,
                    "AVG AIR TEMP_units": np.nan,
                    "site": "TNY",
                    "datasource": "CDEC"
                },
                {
                    "datetime": pd.Timestamp("2021-05-17 08:00:00+0000", tz="UTC"),
                    "ACCUMULATED PRECIPITATION": -0.10,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "AVG AIR TEMP": 2.4,
                    "AVG AIR TEMP_units": "DEG F",
                    "site": "TNY",
                    "datasource": "CDEC"
                },
                {
                    "datetime": pd.Timestamp("2021-05-18 08:00:00+0000", tz="UTC"),
                    "ACCUMULATED PRECIPITATION": -0.10,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "AVG AIR TEMP": 2.2,
                    "AVG AIR TEMP_units": "DEG F",
                    "site": "TNY",
                    "datasource": "CDEC"
                },
            ],
            geometry=[expected_points[0]] * 4,
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime",
                "geometry",
                "site",
                "measurementDate",
                "ACCUMULATED PRECIPITATION",
                "ACCUMULATED PRECIPITATION_units",
                "AVG AIR TEMP",
                "AVG AIR TEMP_units",
                "datasource"
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    @classmethod
    def tny_meta_return(cls):
        result = [
            pd.read_csv(cls.CDEC_MOCKS_DIR.joinpath("raw_tny_locations.csv")),
            pd.read_csv(cls.CDEC_MOCKS_DIR.joinpath("raw_tny_sensors.csv")),
            pd.read_csv(cls.CDEC_MOCKS_DIR.joinpath("raw_tny_notes.csv"))
        ]
        return result

    def read_html_side_effect(self, url, **kwargs):
        if "dynamicapp/staSearch" in url:
            return self.station_search_side_effect(url)
        elif "dynamicapp/staMeta" in url:
            return self.tny_meta_return()
        else:
            raise ValueError("Unknown html scenario in the test mocks")

    @pytest.fixture
    def mock_read_html(self):
        with patch("metloom.pointdata.cdec.pd.read_html") as mock_table_read:
            mock_table_read.side_effect = self.read_html_side_effect
            yield mock_table_read

    @classmethod
    def tny_side_effect(cls, url, **kwargs):
        mock = MagicMock()
        params = kwargs["params"]
        if params.get("dur_code") == "D" and params.get('SensorNums') == "2":
            mock.json.return_value = cls.cdec_daily_precip_response()
        elif params.get("dur_code") == "D" and params.get('SensorNums') == "30":
            mock.json.return_value = cls.cdec_daily_temp_response()
        elif params.get("dur_code") == "H":
            raise NotImplementedError()
        else:
            raise ValueError("unknown scenario")
        return mock

    @classmethod
    def tny_hourly_side_effect(cls, url, **kwargs):
        mock = MagicMock()
        params = kwargs["params"]
        if params.get("dur_code") == 'H':
            mock.json.return_value = cls.cdec_hourly_temp_response()
        elif "getStationInfo" in url:
            mock.json.return_value = cls.tny_meta_return()
        else:
            mock.json.return_value = []
        return mock

    @classmethod
    def station_search_side_effect(cls, *args, **kargs):
        url = args[0]
        sensor_num = re.findall(r'.*&sensor=(\d+)&', url)[0]
        if sensor_num == "3":
            return cls.station_search_response()
        elif sensor_num == "18":
            return [
                pd.DataFrame.from_records(
                    [
                        (
                            "AAA",
                            "A Fake Station",
                            "TUOLUMNE R",
                            "TUOLUMNE",
                            -119.0,
                            37.0,
                            9900,
                            "CA Dept of Water Resources/DFM-Hydro-SMN",
                            np.nan,
                        ),
                        (
                            "DAN",
                            "DANA MEADOWS",
                            "TUOLUMNE R",
                            "TUOLUMNE",
                            -119.257,
                            37.897,
                            9800,
                            "CA Dept of Water Resources/DFM-Hydro-SMN",
                            np.nan,
                        ),
                        (
                            "TNY",
                            "TENAYA LAKE",
                            "MERCED R",
                            "MARIPOSA",
                            -119.448,
                            37.838,
                            8150,
                            "CA Dept of Water Resources/DFM-Hydro-SMN",
                            np.nan,
                        ),
                        (
                            "BBB",
                            "B Fake Station",
                            "TUOLUMNE R",
                            "TUOLUMNE",
                            -119.5,
                            37.5,
                            9905,
                            "CA Dept of Water Resources/DFM-Hydro-SMN",
                            np.nan,
                        ),
                    ],
                    columns=[
                        "ID",
                        "Station Name",
                        "River Basin",
                        "County",
                        "Longitude",
                        "Latitude",
                        "ElevationFeet",
                        "Operator",
                        "Map",
                    ],
                )
            ]
        else:
            raise ValueError(f"{sensor_num} is not configured")

    @staticmethod
    def station_search_response():
        return [
            pd.DataFrame.from_records(
                [
                    (
                        "GIN",
                        "GIN FLAT",
                        "MERCED R",
                        "MARIPOSA",
                        -119.773,
                        37.767,
                        7050,
                        "CA Dept of Water Resources/DFM-Hydro-SMN",
                        np.nan,
                    ),
                    (
                        "DAN",
                        "DANA MEADOWS",
                        "TUOLUMNE R",
                        "TUOLUMNE",
                        -119.257,
                        37.897,
                        9800,
                        "CA Dept of Water Resources/DFM-Hydro-SMN",
                        np.nan,
                    ),
                    (
                        "TNY",
                        "TENAYA LAKE",
                        "MERCED R",
                        "MARIPOSA",
                        -119.448,
                        37.838,
                        8150,
                        "CA Dept of Water Resources/DFM-Hydro-SMN",
                        np.nan,
                    ),
                    (
                        "GFL",
                        "GIN FLAT (COURSE)",
                        "MERCED R",
                        "MARIPOSA",
                        -119.773,
                        37.765,
                        7000,
                        "Yosemite National Park",
                        np.nan,
                    ),
                    (
                        "TUM",
                        "TUOLUMNE MEADOWS",
                        "TUOLUMNE R",
                        "TUOLUMNE",
                        -119.350,
                        37.873,
                        8600,
                        "CA Dept of Water Resources/DFM-Hydro-SMN",
                        np.nan,
                    ),
                    (
                        "SLI",
                        "SLIDE CANYON",
                        "TUOLUMNE R",
                        "TUOLUMNE",
                        -119.43,
                        38.092,
                        9200,
                        "CA Dept of Water Resources/DFM-Hydro-SMN",
                        np.nan,
                    ),
                ],
                columns=[
                    "ID",
                    "Station Name",
                    "River Basin",
                    "County",
                    "Longitude",
                    "Latitude",
                    "ElevationFeet",
                    "Operator",
                    "Map",
                ],
            )
        ]

    def test_class_variables(self):
        assert CDECPointData("no", "no").tzinfo == timezone(timedelta(hours=-8.0))

    def test_get_metadata(self, mock_read_html, tny_station, expected_points):
        metadata = tny_station.metadata

        mock_read_html.assert_called_once_with(
            "https://cdec.water.ca.gov/dynamicapp/staMeta?station_id=TNY"
        )

        # elevation is 1000 just to make sure our mocks are working
        assert expected_points[0] == metadata

    def test_get_daily_data(self, mock_read_html, tny_station, tny_daily_expected):
        with patch("metloom.pointdata.cdec.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self.tny_side_effect
            response = tny_station.get_daily_data(
                datetime(2021, 5, 15),
                datetime(2021, 5, 18),
                [CdecStationVariables.PRECIPITATIONACCUM,
                 CdecStationVariables.TEMPAVG],
            )
            # mock_get = mock_requests.get
            mock_get.assert_any_call(
                "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet",
                params={
                    "Stations": "TNY",
                    "dur_code": "D",
                    "Start": "2021-05-15T00:00:00",
                    "End": "2021-05-18T00:00:00",
                    "SensorNums": "30",
                },
            )
            assert mock_get.call_count == 2
        mock_read_html.assert_called_once()
        pd.testing.assert_frame_equal(response, tny_daily_expected)

    def test_get_daily_from_hourly_data(
        self, mock_read_html, tny_station, expected_points
    ):
        """
        Check that we fall back on resampled hourly data if we don't find
        daily data
        """
        with patch("metloom.pointdata.cdec.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self.tny_hourly_side_effect
            response = tny_station.get_daily_data(
                datetime(2021, 5, 15),
                datetime(2021, 5, 16),
                [CdecStationVariables.TEMPAVG],
            )
            assert mock_get.call_count == 2
            mock_read_html.assert_called_once()
            expected = gpd.GeoDataFrame.from_dict(
                {
                    'AVG AIR TEMP': {
                        (Timestamp('2021-05-15 08:00:00+0000', tz='UTC'),
                            'TNY'): 2.233333},
                    'AVG AIR TEMP_units': {
                        (Timestamp('2021-05-15 08:00:00+0000', tz='UTC'),
                            'TNY'): 'DEG F'},
                    'datasource': {
                        (Timestamp('2021-05-15 08:00:00+0000', tz='UTC'),
                         'TNY'): 'CDEC'}
                }, geometry=expected_points
            )
            expected.index.set_names(["datetime", "site"], inplace=True)
            pd.testing.assert_frame_equal(
                response, expected, check_exact=False, check_like=True
            )

    def test_points_from_geometry(self, mock_read_html, shape_obj):
        expected_url = (
            "https://cdec.water.ca.gov/dynamicapp/staSearch?"
            "sta=&sensor_chk=on&sensor=3"
            "&collect=NONE+SPECIFIED&dur="
            "&active_chk=on&active=Y"
            "&loc_chk=on&lon1=-119.8"
            "&lon2=-119.2&lat1=37.7"
            "&lat2=38.2"
            "&elev1=-5&elev2=99000&nearby=&basin=NONE+SPECIFIED"
            "&hydro=NONE+SPECIFIED&county=NONE+SPECIFIED"
            "&agency_num=160&display=sta"
        )
        result = CDECPointData.points_from_geometry(
            shape_obj, [CdecStationVariables.SWE]
        )
        calls = mock_read_html.call_args_list
        assert calls[0][0][0] == expected_url
        assert len(result) == 6
        assert [st.id for st in result] == [
            "GIN", "DAN", "TNY", "GFL", "TUM", "SLI"
        ]

    @staticmethod
    def check_str_for_float(whole_string, key):
        """
        Parse lat/lon from cdec search string for table read
        """
        split_str = whole_string.split("&")
        matches = [ss for ss in split_str if key in ss]
        if len(matches) > 1 or len(matches) == 0:
            raise ValueError(f"{key} is a bad checker")
        match = matches[0]
        result_val = match.split("=")[-1]
        return float(result_val)

    def test_points_from_geometry_buffer(self, mock_read_html, shape_obj):
        CDECPointData.points_from_geometry(
            shape_obj, [CdecStationVariables.SWE], buffer=0.1
        )

        result_str = mock_read_html.call_args_list[0][0][0]
        expected = {
            'lat2': 38.3, 'lat1': 37.6,
            'lon2': -119.1, 'lon1': -119.9
        }
        for k, v in expected.items():
            result = self.check_str_for_float(result_str, k)
            assert v == pytest.approx(result)

    def test_points_from_geometry_multi_sensor(self, mock_read_html, shape_obj):
        with patch.object(
            CDECPointData, 'is_only_snow_course', return_value=False
        ):
            result = CDECPointData.points_from_geometry(
                shape_obj,
                [CdecStationVariables.SWE, CdecStationVariables.SNOWDEPTH],
                within_geometry=False
            )
            expected_names = [
                "A Fake Station", "B Fake Station", "GIN FLAT",
                "DANA MEADOWS", "TENAYA LAKE", "GIN FLAT (COURSE)",
                "TUOLUMNE MEADOWS", "SLIDE CANYON"
            ]
            expected_codes = [
                "AAA", "BBB", "GIN", "DAN", "TNY", "TUM", "SLI", "GFL"
            ]
            assert len(result) == 8
            assert all([st.id in expected_codes for st in result])
            assert all([st.name in expected_names for st in result])

    def test_points_from_geometry_fail(self, shape_obj):
        with patch("metloom.pointdata.cdec.pd") as mock_pd:
            mock_pd.read_html.side_effect = side_effect_error
            result = CDECPointData.points_from_geometry(
                shape_obj, [CdecStationVariables.SWE]
            )
            assert result.points == []

    def test_point_collection_to_dataframe(self, mock_read_html, shape_obj):
        result = CDECPointData.points_from_geometry(
            shape_obj, [CdecStationVariables.SWE]
        )
        assert mock_read_html.call_count == 7
        assert isinstance(result, PointDataCollection)
        points_df = result.to_dataframe()
        for idp, point in enumerate(result):
            point_row = points_df.iloc[idp]
            assert point.name == point_row["name"]
            assert point.id == point_row["id"]
            assert point.metadata == point_row["geometry"]
            assert point.DATASOURCE == point_row["datasource"]

    def test_can_parse_dates(self, tny_station):
        df = pd.DataFrame.from_records([
            {"datetime": "2021-03-14 01:00:00"},
            # This time does not exist in US/Pacific, but does in CDEC
            {"datetime": "2021-03-14 02:00:00"},
            {"datetime": "2021-03-14 03:00:00"},
        ])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.index = df.index.tz_localize(tny_station.tzinfo)
        df.tz_convert("UTC")


class TestCdecUptime:
    """
    Live tests of cdec
    """
    @pytest.fixture(scope="class")
    def tum(self):
        point = CDECPointData("TUM", "Tuolumne Meadows")
        # call metadata once so we cache it off
        print(point.metadata)
        return point

    def test_metadata(self, tum):
        assert tum.metadata == shapely.geometry.Point(-119.348096, 37.876406, 8600)

    def test_is_snowcourse(self, tum):
        assert tum.is_partly_snow_course()

    def test_is_only_monthly(self, tum):
        assert not tum.is_only_monthly()

    def test_is_only_snowcourse(self, tum):
        assert not tum.is_only_snow_course([tum.ALLOWED_VARIABLES.SWE])


class TestCdecUptimeTRK:
    @pytest.fixture(scope="class")
    def stn(self):
        point = CDECPointData("TRK", "Truckee River Near Truckee")
        # call metadata once so we cache it off
        print(point.metadata)
        return point

    def test_metadata(self, stn):
        assert stn.metadata == shapely.geometry.Point(
            -120.205475, 39.296295, 5858
        )

    def test_is_snowcourse(self, stn):
        assert not stn.is_partly_snow_course()

    def test_is_only_monthly(self, stn):
        assert not stn.is_only_monthly()

    def test_is_only_snowcourse(self, stn):
        assert not stn.is_only_snow_course([stn.ALLOWED_VARIABLES.SWE])

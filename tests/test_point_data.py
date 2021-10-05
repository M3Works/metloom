import numpy as np
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
from copy import deepcopy
import geopandas as gpd
import pandas as pd
import pytz
from os import path
from climata import snotel

from metloom.pointdata import (
    CDECPointData,
    PointDataCollection,
    PointData,
    SnotelPointData,
)
from metloom.variables import CdecStationVariables, SnotelVariables


def side_effect_error(*args):
    raise ValueError("Testing error")


class TestPointData(object):
    @pytest.fixture(scope="class")
    def data_dir(self):
        this_dir = path.dirname(__file__)
        return path.join(this_dir, "data")

    @pytest.fixture(scope="class")
    def shape_obj(self, data_dir):
        fp = path.join(data_dir, "testing.shp")
        return gpd.read_file(fp)

    @staticmethod
    def expected_response(dates, vals, var, unit, station, points):
        obj = []
        for dt, v in zip(dates, vals):
            obj.append(
                {
                    "datetime": pd.Timestamp(dt, tz="UTC"),
                    "measurementDate": pd.Timestamp(dt, tz="UTC"),
                    var: v,
                    f"{var}_units": unit,
                    "site": station.id,
                }
            )
        df = gpd.GeoDataFrame.from_dict(
            obj,
            geometry=[points] * len(dates),
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            ["datetime", "geometry", "site", "measurementDate", var, f"{var}_units"]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df


class TestCDECStation(TestPointData):
    @staticmethod
    def cdec_daily_response():
        return [
            {
                "stationId": "TNY",
                "durCode": "D",
                "SENSOR_NUM": 3,
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
                "SENSOR_NUM": 3,
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
                "SENSOR_NUM": 3,
                "sensorType": "SNOW WC",
                "date": "2021-5-18 00:00",
                "obsDate": "2021-5-18 00:00",
                "value": -0.10,
                "dataFlag": " ",
                "units": "INCHES",
            },
        ]

    @pytest.fixture(scope="function")
    def tny_station(self):
        return CDECPointData("TNY", "Tenaya Lake")

    @pytest.fixture(scope="class")
    def tny_daily_expected(self):
        points = gpd.points_from_xy([-119.0], [42.0], z=[1000.0])
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2021-05-16 07:00:00+0000", tz="UTC"),
                    "measurementDate": pd.Timestamp(
                        "2021-05-16 07:00:00+0000", tz="UTC"
                    ),
                    "ACCUMULATED PRECIPITATION": -0.11,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "site": "TNY",
                },
                {
                    "datetime": pd.Timestamp("2021-05-17 07:00:00+0000", tz="UTC"),
                    "measurementDate": pd.Timestamp(
                        "2021-05-17 07:00:00+0000", tz="UTC"
                    ),
                    "ACCUMULATED PRECIPITATION": -0.10,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "site": "TNY",
                },
                {
                    "datetime": pd.Timestamp("2021-05-18 07:00:00+0000", tz="UTC"),
                    "measurementDate": pd.Timestamp(
                        "2021-05-18 07:00:00+0000", tz="UTC"
                    ),
                    "ACCUMULATED PRECIPITATION": -0.10,
                    "ACCUMULATED PRECIPITATION_units": "INCHES",
                    "site": "TNY",
                },
            ],
            geometry=[points[0]] * 3,
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
            ]
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
                "STATION": [
                    {
                        "SENS_LONG_NAME": "SNOW, WATER CONTENT",
                        "ELEVATION": 1000.0,
                        "LATITUDE": 42.0,
                        "LONGITUDE": -119.0,
                    }
                ]
            }
        else:
            raise ValueError("unknown scenario")
        return mock

    @pytest.fixture(scope="class")
    def station_search_response(self):
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
        assert CDECPointData("no", "no").tzinfo == pytz.timezone("US/Pacific")
        # Base implementation should fail
        with pytest.raises(AttributeError):
            PointData("foo", "bar").tzinfo

    def test_get_metadata(self, tny_station):
        with patch("metloom.pointdata.cdec.requests") as mock_requests:
            mock_requests.get.side_effect = self.tny_side_effect
            metadata = tny_station.metadata
            mock_get = mock_requests.get
            assert mock_get.call_count == 1
            mock_get.assert_called_with(
                "http://cdec.water.ca.gov/cdecstation2/CDecServlet/" "getStationInfo",
                params={"stationID": "TNY"},
            )
        expected = gpd.points_from_xy([-119.0], [42.0], z=[1000.0])[0]
        assert expected == metadata

    def test_get_daily_data(self, tny_station, tny_daily_expected):
        with patch("metloom.pointdata.cdec.requests") as mock_requests:
            mock_requests.get.side_effect = self.tny_side_effect
            response = tny_station.get_daily_data(
                datetime(2021, 5, 16),
                datetime(2021, 5, 18),
                [CdecStationVariables.PRECIPITATIONACCUM],
            )
            mock_get = mock_requests.get
            mock_get.assert_any_call(
                "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet",
                params={
                    "Stations": "TNY",
                    "dur_code": "D",
                    "Start": "2021-05-16T00:00:00",
                    "End": "2021-05-18T00:00:00",
                    "SensorNums": "2",
                },
            )
            assert mock_get.call_count == 2
        pd.testing.assert_frame_equal(response, tny_daily_expected)

    def test_points_from_geometry(self, shape_obj, station_search_response):
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
        with patch("metloom.pointdata.cdec.pd.read_html") as mock_table_read:
            mock_table_read.return_value = station_search_response
            result = CDECPointData.points_from_geometry(
                shape_obj, [CdecStationVariables.SWE]
            )
            mock_table_read.assert_called_with(expected_url)
            assert len(result) == 5
            assert [st.id for st in result] == ["GIN", "DAN", "TNY", "TUM", "SLI"]

    def test_points_from_geometry_fail(self, shape_obj):
        with patch("metloom.pointdata.cdec.pd") as mock_pd:
            mock_pd.read_html.side_effect = side_effect_error
            result = CDECPointData.points_from_geometry(
                shape_obj, [CdecStationVariables.SWE]
            )
            assert result.points == []

    def test_point_collection_to_dataframe(self, shape_obj, station_search_response):
        with patch("metloom.pointdata.cdec.pd.read_html") as mock_table_read:
            mock_table_read.return_value = station_search_response
            result = CDECPointData.points_from_geometry(
                shape_obj, [CdecStationVariables.SWE]
            )
            assert isinstance(result, PointDataCollection)
            points_df = result.to_dataframe()
            for idp, point in enumerate(result):
                point_row = points_df.iloc[idp]
                assert point.name == point_row["name"]
                assert point.id == point_row["id"]
                assert point.metadata == point_row["geometry"]


class MockSnotelIO(snotel.SnotelIO):
    """
    Mock the data structure that the climata.snotel classes return
    """

    def __init__(self, mock_obj, *args, **kwargs):
        """
        Args:
            mock_obj: a list of dictionaries that will be stored
        """
        self.mock_dict = mock_obj
        super(MockSnotelIO, self).__init__(*args, **kwargs)

    def load(self):
        self.data = self.mock_dict
        transformed = []
        # store the nested 'data' attribute as the same class
        # to allow attribute level access of variables
        if len(self.data) > 0 and self.data[0].get("data", False):
            for row in self.data:
                inner_data = MockSnotelIO(row["data"])
                new_row = deepcopy(row)
                new_row["data"] = inner_data
                transformed += [new_row]
            self.data = transformed


class TestSnotelPointData(TestPointData):
    @pytest.fixture(scope="function")
    def station(self):
        return SnotelPointData("538:CO:SNTL", "TestSite")

    @pytest.fixture(scope="class")
    def points(self):
        return gpd.points_from_xy([-107.67552], [37.9339], z=[9800.0])[0]

    @pytest.fixture(scope="class")
    def meta_response_dict(self):
        return {
            "actonId": "07M27S",
            "beginDate": "1979-10-01 00:00:00",
            "countyName": "Ouray",
            "elevation": 9800.0,
            "endDate": "2100-01-01 00:00:00",
            "fipsCountryCd": "US",
            "fipsCountyCd": "091",
            "fipsStateNumber": "08",
            "huc": "140200060201",
            "hud": "14020006",
            "latitude": 37.9339,
            "longitude": -107.67552,
            "name": "Idarado",
            "shefId": "IDRC2",
            "stationTriplet": "538:CO:SNTL",
        }

    @pytest.fixture(scope="class")
    def mock_meta_response(self, meta_response_dict):
        return MockSnotelIO([{"stationDataTimeZone": -8.0, **meta_response_dict}])

    @pytest.fixture(scope="class")
    def mock_coursemeta_response(self, meta_response_dict):
        return MockSnotelIO([meta_response_dict])

    @pytest.fixture(scope="function")
    def standard_snotel_return(self):
        return [{"elementCd": SnotelVariables.SWE.code, "storedunitcd": "in"}]

    def test_metadata(self, mock_meta_response):
        with patch("metloom.pointdata.snotel.snotel.StationMetaIO") as mock_snotel:
            mock_snotel.return_value = mock_meta_response
            obj = SnotelPointData("538:CO:SNTL", "eh")
            assert (
                obj.metadata
                == gpd.points_from_xy([-107.67552], [37.9339], z=[9800.0])[0]
            )
            assert obj.tzinfo == timezone(timedelta(hours=-8.0))

    @pytest.mark.parametrize(
        "mocked_class, date_name, dts, expected_dts, vals, d1, d2, fn_name",
        [
            (
                "snotel.StationHourlyDataIO",
                "dateTime",
                ["2020-03-20 00:00", "2020-03-20 01:00", "2020-03-20 02:00"],
                ["2020-03-20 08:00", "2020-03-20 09:00", "2020-03-20 10:00"],
                [13.19, 13.17, 13.14],
                datetime(2020, 3, 20, 0),
                datetime(2020, 3, 20, 2),
                "get_hourly_data",
            ),
            (
                "snotel.StationDailyDataIO",
                "date",
                ["2020-03-20", "2020-03-21", "2020-03-22"],
                ["2020-03-20 08:00", "2020-03-21 08:00", "2020-03-22 08:00"],
                [13.19, 13.17, 13.14],
                datetime(2020, 3, 20),
                datetime(2020, 3, 22),
                "get_daily_data",
            ),
            (
                "StationMonthlyDataIO",
                "date",
                ["2020-01-28", "2020-02-27"],
                ["2020-01-28 00:00", "2020-02-27 00:00"],
                [13.19, 13.17],
                datetime(2020, 1, 20),
                datetime(2020, 3, 15),
                "get_snow_course_data",
            ),
        ],
    )
    def test_get_data_methods(
        self,
        mocked_class,
        date_name,
        dts,
        expected_dts,
        vals,
        d1,
        d2,
        fn_name,
        station,
        standard_snotel_return,
        mock_meta_response,
        mock_coursemeta_response,
        points,
    ):
        with patch(f"metloom.pointdata.snotel.{mocked_class}") as mock_snotel, patch(
            "metloom.pointdata.snotel.snotel.StationMetaIO"
        ) as mock_meta:
            if "snow_course" in fn_name:
                mock_meta.return_value = mock_coursemeta_response
            else:
                mock_meta.return_value = mock_meta_response
            standard_snotel_return[0]["data"] = [
                {date_name: dt, "flag": "V", "value": v} for dt, v, in zip(dts, vals)
            ]
            mock_snotel.return_value = MockSnotelIO(standard_snotel_return)

            vrs = [SnotelVariables.SWE]
            fn = getattr(station, fn_name)
            result = fn(d1, d2, vrs)
            expected = self.expected_response(
                expected_dts, vals, SnotelVariables.SWE.name, "in", station, points
            )
            pd.testing.assert_frame_equal(result, expected)

    def test_points_from_geometry(self, shape_obj):
        with patch("metloom.pointdata.snotel.LoomStationIO") as mock_search:
            mock_search.return_value = MockSnotelIO(
                [
                    {
                        "actonId": None,
                        "beginDate": "1930-02-01 00:00:00",
                        "countyName": "Tuolumne",
                        "elevation": 6500.0,
                        "endDate": "2100-01-01 00:00:00",
                        "fipsCountryCd": "US",
                        "fipsCountyCd": "109",
                        "fipsStateNumber": "06",
                        "huc": "180400090302",
                        "latitude": 37.995,
                        "longitude": -119.78,
                        "name": "Fake1",
                        "shefId": None,
                        "stationTriplet": "FFF:CA:SNOW",
                    },
                    {
                        "actonId": None,
                        "beginDate": "1948-02-01 00:00:00",
                        "countyName": "Tuolumne",
                        "elevation": 9300.0,
                        "endDate": "2100-01-01 00:00:00",
                        "fipsCountryCd": "US",
                        "fipsCountyCd": "109",
                        "fipsStateNumber": "06",
                        "huc": "180400090402",
                        "hud": "18040009",
                        "latitude": 38.18333,
                        "longitude": -119.61667,
                        "name": "Fake2",
                        "shefId": None,
                        "stationTriplet": "BBB:CA:SNOW",
                    },
                ]
            )
            result = SnotelPointData.points_from_geometry(
                shape_obj, [SnotelVariables.SWE], snow_courses=True
            )
            ids = [point.id for point in result]
            names = [point.name for point in result]
            assert ids == ["FFF:CA:SNOW", "BBB:CA:SNOW"]
            assert names == ["Fake1", "Fake2"]

    def test_points_from_geometry_fail(self, shape_obj):
        with patch("metloom.pointdata.snotel.LoomStationIO") as mock_search:
            mock_search.return_value = MockSnotelIO([])
            result = SnotelPointData.points_from_geometry(
                shape_obj, [SnotelVariables.SWE], snow_courses=True
            )
            assert len(result) == 0

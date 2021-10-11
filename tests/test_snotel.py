from datetime import timezone, timedelta, datetime
from unittest.mock import patch, MagicMock
from collections import OrderedDict

import geopandas as gpd
import pandas as pd
import pytest

from metloom.pointdata import SnotelPointData
from metloom.variables import SnotelVariables
from tests.test_point_data import TestPointData


class MockZeepObject:
    def __init__(self, obj):
        """
        Args:
            obj: dictionary of values
        """
        self.mock_dict = OrderedDict(obj)
        self.__values__ = self.mock_dict
        for k, v in obj.items():
            setattr(self, k, v)

    def __dict__(self):
        return self.mock_dict

    def __getitem__(self, item):
        return self.mock_dict[item]


class TestSnotelPointData(TestPointData):

    @pytest.fixture(scope="class")
    def points(self):
        return gpd.points_from_xy([-107.67552], [37.9339], z=[9800.0])[0]

    @staticmethod
    def snotel_meta_sideeffect(*args, **kwargs):
        """
        Mock out the metadata response
        """
        code = kwargs["stationTriplet"]
        available_stations = {
            "538:CO:SNTL": {
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
                "stationDataTimeZone": -8.0
            },
            "538:CO:SNOW": {
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
                "stationTriplet": "538:CO:SNOW",
            },
            "FFF:CA:SNOW": {
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
            "BBB:CA:SNOW": {
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
        }
        return MockZeepObject(available_stations[code])

    @staticmethod
    def snotel_data_sideeffect(*args, **kwargs):
        duration = kwargs["duration"]
        if duration == "SEMIMONTHLY":
            return [
                MockZeepObject({
                    'beginDate': '2020-01-20 00:00:00',
                    'collectionDates': ['2020-01-28', '2020-02-27'],
                    'duration': 'SEMIMONTHLY',
                    'endDate': '2020-03-14 00:00:00',
                    'flags': ['V', 'V'], 'stationTriplet': '538:CO:SNTL',
                    'values': [13.19, 13.17]})
            ]
        if duration == "DAILY":
            return [MockZeepObject({
                'beginDate': '2020-03-20 00:00:00',
                'collectionDates': [], 'duration': 'DAILY',
                'endDate': '2020-03-22 00:00:00', 'flags': ['V', 'V', 'V'],
                'stationTriplet': '538:CO:SNTL',
                'values': [13.19, 13.17, 13.14]})]

    @staticmethod
    def snotel_hourly_sideeffect(*args, **kwargs):
        return [
            {
                'beginDate': '2020-01-02 00:00', 'endDate': '2020-01-20 00:00',
                'stationTriplet': '538:CO:SNTL',
                'values': [
                    {
                        'dateTime': '2020-03-20 00:00',
                        'flag': 'V',
                        'value': 13.19
                    }, {
                        'dateTime': '2020-03-20 01:00',
                        'flag': 'V',
                        'value': 13.17
                    }, {
                        'dateTime': '2020-03-20 02:00',
                        'flag': 'V',
                        'value': 13.14
                    }]}]

    @pytest.fixture(scope="class")
    def mock_elements(self):
        return [
            MockZeepObject(
                {
                    'beginDate': '1980-07-23 00:00:00', 'dataPrecision': 1,
                    'duration': 'DAILY', 'elementCd': 'WTEQ',
                    'endDate': '2100-01-01 00:00:00', 'heightDepth': None,
                    'ordinal': 1,
                    'originalUnitCd': 'in', 'stationTriplet': '538:CO:SNTL',
                    'storedUnitCd': 'in'}),
            MockZeepObject(
                {'beginDate': '1980-07-23 00:00:00', 'dataPrecision': 1,
                 'duration': 'SEMIMONTHLY', 'elementCd': 'WTEQ',
                 'endDate': '2100-01-01 00:00:00', 'heightDepth': None,
                 'ordinal': 1,
                 'originalUnitCd': 'in', 'stationTriplet': '538:CO:SNTL',
                 'storedUnitCd': 'in'}),
            MockZeepObject(
                {'beginDate': '1979-10-01 00:00:00', 'dataPrecision': 1,
                 'duration': 'HOURLY', 'elementCd': 'WTEQ',
                 'endDate': '2100-01-01 00:00:00', 'heightDepth': None,
                 'ordinal': 1,
                 'originalUnitCd': 'in', 'stationTriplet': '538:CO:SNTL',
                 'storedUnitCd': 'in'}),
            MockZeepObject(
                {'beginDate': '1980-07-23 00:00:00', 'dataPrecision': 1,
                 'duration': 'MONTHLY', 'elementCd': 'WTEQ',
                 'endDate': '2100-01-01 00:00:00', 'heightDepth': None,
                 'ordinal': 1,
                 'originalUnitCd': 'in', 'stationTriplet': '538:CO:SNTL',
                 'storedUnitCd': 'in'})]

    @pytest.fixture(scope="class")
    def mock_zeep_client(self, mock_elements):
        with patch("metloom.pointdata.snotel_client.zeep.Client") as mock_client:
            mock_service = MagicMock()
            # setup the individual services
            mock_service.getStationMetadata.side_effect = self.snotel_meta_sideeffect
            mock_service.getStationElements.return_value = mock_elements
            mock_service.getStations.return_value = ["FFF:CA:SNOW",
                                                     "BBB:CA:SNOW"]
            mock_service.getData.side_effect = self.snotel_data_sideeffect
            mock_service.getHourlyData.side_effect = self.snotel_hourly_sideeffect
            # assign service to client
            mock_client.return_value.service = mock_service
            yield mock_client

    def test_metadata(self, mock_zeep_client):
        obj = SnotelPointData("538:CO:SNTL", "eh")
        assert (
            obj.metadata
            == gpd.points_from_xy([-107.67552], [37.9339], z=[9800.0])[0]
        )
        assert obj.tzinfo == timezone(timedelta(hours=-8.0))

    @pytest.mark.parametrize(
        "station_id, dts, expected_dts, vals, d1, d2, fn_name",
        [
            (
                "538:CO:SNTL",
                ["2020-03-20 00:00", "2020-03-20 01:00", "2020-03-20 02:00"],
                ["2020-03-20 08:00", "2020-03-20 09:00", "2020-03-20 10:00"],
                [13.19, 13.17, 13.14],
                datetime(2020, 3, 20, 0),
                datetime(2020, 3, 20, 2),
                "get_hourly_data",
            ),
            (
                "538:CO:SNTL",
                ["2020-03-20", "2020-03-21", "2020-03-22"],
                ["2020-03-20 08:00", "2020-03-21 08:00", "2020-03-22 08:00"],
                [13.19, 13.17, 13.14],
                datetime(2020, 3, 20),
                datetime(2020, 3, 22),
                "get_daily_data",
            ),
            (
                "538:CO:SNOW",
                ["2020-01-28", "2020-02-27"],
                ["2020-01-28 00:00", "2020-02-27 00:00"],
                [13.19, 13.17],
                datetime(2020, 1, 20),
                datetime(2020, 3, 15),
                "get_snow_course_data",
            ),
        ],
    )
    def test_get_data_methods(self, station_id, dts, expected_dts, vals, d1,
                              d2,
                              fn_name, points, mock_zeep_client):
        station = SnotelPointData(station_id, "TestSite")
        vrs = [SnotelVariables.SWE]
        fn = getattr(station, fn_name)
        result = fn(d1, d2, vrs)
        expected = self.expected_response(
            expected_dts, vals, SnotelVariables.SWE.name, "in", station, points
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_points_from_geometry(self, shape_obj, mock_zeep_client):
        result = SnotelPointData.points_from_geometry(
            shape_obj, [SnotelVariables.SWE], snow_courses=True
        )
        ids = [point.id for point in result]
        names = [point.name for point in result]
        assert len(names) == 2
        assert set(ids) == {"FFF:CA:SNOW", "BBB:CA:SNOW"}
        assert set(names) == {"Fake1", "Fake2"}

    def test_points_from_geometry_fail(self, shape_obj, mock_zeep_client):
        mock_zeep_client.return_value.service.getStations.return_value = []
        result = SnotelPointData.points_from_geometry(
            shape_obj, [SnotelVariables.SWE], snow_courses=True
        )
        assert len(result) == 0

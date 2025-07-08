from datetime import timezone, timedelta, datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
from collections import OrderedDict

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from metloom.pointdata import SnotelPointData
from metloom.variables import SnotelVariables
from tests.test_point_data import BasePointDataTest
from tests.utils import read_json


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


class TestSnotelPointData(BasePointDataTest):
    MOCKS_DIR = Path(__file__).parent.joinpath("data/snotel_mocks/").absolute()

    @pytest.fixture(scope="class")
    def points(self):
        return gpd.points_from_xy([-107.6762], [37.93389], z=[9800.0])[0]

    @classmethod
    def side_effect(cls, *args, **kwargs):
        """
        All request side effects
        """
        url = args[0]
        if "services/v1/stations" in url:
            result = cls.snotel_meta_sideeffect(*args, **kwargs)
        elif "services/v1/data" in url:
            result = cls.snotel_data_sideeffect(*args, **kwargs)
        else:
            raise ValueError("Unknown URL in mock: " + url)
        obj = MagicMock()
        obj.json.return_value = result
        return obj

    @staticmethod
    def snotel_meta_sideeffect(*args, **kwargs):
        """
        Mock out the metadata response
        """
        codes = kwargs["params"]["stationTriplets"].split(",")
        available_stations = {
            "538:CO:SNTL": {
                "stationTriplet": "538:CO:SNTL",
                "stationId": "538",
                "stateCode": "CO",
                "networkCode": "SNTL",
                "name": "Idarado",
                "dcoCode": "CO",
                "countyName": "Ouray",
                "huc": "140200060201",
                "elevation": 9800,
                "latitude": 37.93389,
                "longitude": -107.6762,
                "dataTimeZone": -8,
                "shefId": "IDRC2",
                "operator": "NRCS",
                "beginDate": "1979-10-01 00:00",
                "endDate": "2100-01-01 00:00"
            },
            "FFF:CA:SNOW": {
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
                "dataTimeZone": -8,
                "stationTriplet": "FFF:CA:SNOW",
            },
            "BBB:CA:SNOW": {
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
                "dataTimeZone": -8,
                "stationTriplet": "BBB:CA:SNOW",
            },
        }
        return [available_stations[code] for code in codes if code in available_stations]

    @classmethod
    def snotel_data_sideeffect(cls, *args, **kwargs):
        duration = kwargs["params"]["duration"]
        fname = None
        if duration == "SEMIMONTHLY":
            fname = cls.MOCKS_DIR.joinpath("semimonthly_swe.json")
        elif duration == "DAILY":
            fname = cls.MOCKS_DIR.joinpath("daily.json")
        elif duration == "HOURLY":
            element_cd = kwargs["params"]["elements"]
            cd_file_map = {
                "WTEQ": "hourly_swe.json",
                "PRCPSA": "hourly_precip.json",
                "STO:-2": "hourly_soil.json",
            }
            fname = cls.MOCKS_DIR.joinpath(cd_file_map.get(element_cd))

        if fname is None:
            raise ValueError("No mock file found for duration: " + duration)

        return read_json(fname)

    @pytest.fixture
    def mock_requests(self):
        with patch("requests.get") as mock_get:
            # Mock our gets
            mock_get.side_effect = self.side_effect
            yield mock_get

    @pytest.fixture
    def mock_zeep_find(self):
        """
        Mock the zeep client to return a mock service with
        getStations method returning a list of station triplets.
        """
        with patch(
            "metloom.pointdata.snotel.snotel_client.zeep.Client"
        ) as mock_client:
            mock_service = MagicMock()
            # setup the individual services
            mock_service.getStations.return_value = ["FFF:CA:SNOW",
                                                     "BBB:CA:SNOW"]
            # assign service to client
            mock_client.return_value.service = mock_service
            yield mock_client

    def test_metadata(self, mock_requests):
        obj = SnotelPointData("538:CO:SNTL", "eh")
        assert (
            obj.metadata == gpd.points_from_xy(
                [-107.6762], [37.93389], z=[9800.0])[0]
        )
        assert obj.tzinfo == timezone(timedelta(hours=-8.0))

    @pytest.mark.parametrize(
        "station_id, dts, expected_dts, vals, d1, d2, fn_name",
        [
            (
                "538:CO:SNTL",
                ["2020-01-02 00:00", "2020-01-02 01:00", "2020-01-02 02:00"],
                ["2020-01-02 08:00", "2020-01-02 09:00", "2020-01-02 10:00"],
                {
                    SnotelVariables.SWE.name: [6.9, 6.9, 6.8],
                    f"{SnotelVariables.SWE.name}_units": ["in", "in", "in"]
                },
                datetime(2020, 1, 2, 0),
                datetime(2020, 1, 2, 2),
                "get_hourly_data",
            ),
            (
                "538:CO:SNTL",
                ["2020-01-02 00:00", "2020-01-02 01:00", "2020-01-02 02:00"],
                ["2020-01-02 08:00", "2020-01-02 09:00", "2020-01-02 10:00"],
                {
                    SnotelVariables.TEMPGROUND2IN.name: [-1.0, -1.2, -2.0],
                    f"{SnotelVariables.TEMPGROUND2IN.name}_units":
                        ["degF", "degF", "degF"]
                },
                datetime(2020, 1, 2, 0),
                datetime(2020, 1, 2, 2),
                "get_hourly_data",
            ),
            (
                "538:CO:SNTL",
                ["2020-03-20", "2020-03-21", "2020-03-22"],
                ["2020-03-20 08:00", "2020-03-21 08:00", "2020-03-22 08:00"],
                {
                    SnotelVariables.SWE.name: [11.6, 11.6, 11.8],
                    f"{SnotelVariables.SWE.name}_units": ["in", "in", "in"]
                },
                datetime(2020, 3, 20),
                datetime(2020, 3, 22),
                "get_daily_data",
            ),
            (
                "538:CO:SNTL",
                ["2020-01-16", "2020-02-01", "2020-02-16", "2020-02-27"],
                ["2020-01-16 08:00", "2020-02-01 08:00", "2020-02-16 08:00", "2020-03-01 08:00"],
                {
                    SnotelVariables.SWE.name: [6.4, 7.3, 8.8, 9.9],
                    f"{SnotelVariables.SWE.name}_units": ["in", "in", "in", "in"]
                },
                datetime(2020, 1, 20),
                datetime(2020, 3, 15),
                "get_snow_course_data",
            ),
        ],
    )
    def test_get_data_methods(
            self, station_id, dts, expected_dts, vals, d1,
            d2, fn_name, points, mock_requests):
        station = SnotelPointData(station_id, "TestSite")
        if 'GROUND TEMPERATURE -2IN' in list(vals.keys()):
            vrs = [SnotelVariables.TEMPGROUND2IN]
        else:
            vrs = [SnotelVariables.SWE]
        fn = getattr(station, fn_name)
        result = fn(d1, d2, vrs)
        expected = self.expected_response(
            expected_dts, vals, station, points,
            include_measurement_date="snow_course" in fn_name
        )
        pd.testing.assert_frame_equal(
            result.sort_index(axis=1), expected
        )

    def test_get_hourly_data_multi_sensor(self, points, mock_requests):
        expected_dts = [
            "2020-01-02 08:00", "2020-01-02 09:00", "2020-01-02 10:00",
            "2020-01-02 11:00"
        ]
        expected_vals_obj = {
            SnotelVariables.SWE.name: [6.9, 6.9, 6.8, np.nan],
            f"{SnotelVariables.SWE.name}_units": ["in", "in", "in", np.nan],
            SnotelVariables.PRECIPITATION.name: [6, 6, 6.1, 6.5],
            f"{SnotelVariables.PRECIPITATION.name}_units": [
                "in", "in", "in", "in"],
        }
        station = SnotelPointData("538:CO:SNTL", "TestSite")
        vrs = [SnotelVariables.PRECIPITATION, SnotelVariables.SWE]
        result = station.get_hourly_data(
            datetime(2020, 1, 2, 0), datetime(2020, 1, 2, 4), vrs
        )
        expected = self.expected_response(
            expected_dts, expected_vals_obj, station, points
        )
        pd.testing.assert_frame_equal(
            result.sort_index(axis=1), expected
        )

    def test_points_from_geometry(self, shape_obj, mock_requests, mock_zeep_find):
        result = SnotelPointData.points_from_geometry(
            shape_obj, [SnotelVariables.SWE], snow_courses=True
        )
        ids = [point.id for point in result]
        names = [point.name for point in result]
        assert len(names) == 2
        assert set(ids) == {"FFF:CA:SNOW", "BBB:CA:SNOW"}
        assert set(names) == {"Fake1", "Fake2"}

    def test_points_from_geomtery_buffer(
        self, shape_obj, mock_requests, mock_zeep_find
    ):
        SnotelPointData.points_from_geometry(
            shape_obj, [SnotelVariables.SWE], snow_courses=False, buffer=0.1
        )
        search_kwargs = mock_zeep_find().method_calls[0][2]
        expected = {
            'maxLatitude': 38.3, 'minLatitude': 37.6,
            'maxLongitude': -119.1, 'minLongitude': -119.9
        }
        for k, v in expected.items():
            assert v == pytest.approx(search_kwargs[k])

    def test_points_from_geometry_fail(self, shape_obj, mock_requests, mock_zeep_find):
        mock_zeep_find.return_value.service.getStations.return_value = []
        result = SnotelPointData.points_from_geometry(
            shape_obj, [SnotelVariables.SWE], snow_courses=True
        )
        assert len(result) == 0

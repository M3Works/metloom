from datetime import timezone, timedelta, datetime
from os.path import join
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest

from metloom.pointdata import USGSPointData
from metloom.variables import USGSVariables
from tests.test_point_data import BasePointDataTest


DATA_DIR = str(Path(__file__).parent.joinpath("data"))


class TestUSGSStation(BasePointDataTest):

    @staticmethod
    def usgs_daily_discharge_response():
        return {
            "value": {
                "timeSeries": [
                    {
                        "values": [
                            {"value": [
                                {"value": '111', "dateTime": "2020-07-01T00:00:00.000"},
                                {"value": '112', "dateTime": "2020-07-02T00:00:00.000"}
                            ]}],
                        "sourceInfo": {
                            "geoLocation": {
                                "geogLocation":
                                    {"longitude": -106.54, "latitude": 37.35}
                            },
                            "timeZoneInfo":
                                {"defaultTimeZone": {"zoneOffset": "-5:00"}}
                        },
                        "variable": {"unit": {"unitCode": "cf/s"},
                                     "noDataValue": -9999.0},
                    }]}
        }

    @pytest.fixture(scope="function")
    def crp_station(self):
        return USGSPointData("08245000", "Conejos R bl Platoro Reservoir")

    @pytest.fixture(scope="class")
    def crp_daily_expected(self):
        points = gpd.points_from_xy([-106.54], [37.35], z=[1000.0])
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2020-07-01 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": '111',
                    "DISCHARGE_units": "cf/s",
                    "site": "08245000",
                    "datasource": "USGS"
                },
                {
                    "datetime": pd.Timestamp("2020-07-02 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": '112',
                    "DISCHARGE_units": "cf/s",
                    "site": "08245000",
                    "datasource": "USGS"
                },

            ],
            geometry=[points[0]] * 2,
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime",
                "geometry",
                "DISCHARGE",
                "site",
                "DISCHARGE_units",
                "datasource"
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    @staticmethod
    def crp_meta_return():
        with open(join(DATA_DIR, "platoro_meta.txt")) as fp:
            data_text = fp.read()

        return data_text

    @classmethod
    def crp_side_effect(cls, url, **kwargs):
        mock = MagicMock()
        params = kwargs["params"]

        if "startDT" not in params:
            mock.text = cls.crp_meta_return()
        elif "startDT" in params:
            mock.json.return_value = cls.usgs_daily_discharge_response()
        else:
            raise ValueError("unknown scenario")

        return mock

    def test_get_metadata(self, crp_station):

        with patch("metloom.pointdata.usgs.requests") as mock_requests:
            mock_requests.get.side_effect = self.crp_side_effect
            metadata = crp_station.metadata
            mock_get = mock_requests.get
            assert mock_get.call_count == 1
            mock_get.assert_called_with(
                "https://waterservices.usgs.gov/nwis/site/",
                params={
                    "format": "rdb",
                    "sites": "08245000",
                    "siteOutput": "expanded",
                    "siteStatus": "all"
                },
            )

        expected = gpd.points_from_xy([-106.54], [37.35], z=[1000.0])[0]
        assert expected == metadata

    def test_get_daily_data(self, crp_station, crp_daily_expected):
        with patch("metloom.pointdata.usgs.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self.crp_side_effect
            response = crp_station.get_daily_data(
                datetime(2020, 7, 1),
                datetime(2020, 7, 2),
                [USGSVariables.DISCHARGE],
            )

            mock_get.assert_any_call(
                "https://waterservices.usgs.gov/nwis/dv/",
                params={
                    'startDT': datetime(2020, 7, 1).date().isoformat(),
                    'endDT': datetime(2020, 7, 2).date().isoformat(),
                    'sites': '08245000',
                    'parameterCd': '00060',
                    'format': 'json',
                    'siteType': 'ST',
                    'siteStatus': 'all'
                },
            )
            assert mock_get.call_count == 2
        pd.testing.assert_frame_equal(response, crp_daily_expected)
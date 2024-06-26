import json
from os.path import join
from pathlib import Path
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from metloom.pointdata import NWSForecastPointData
from metloom.variables import NWSForecastVariables
from tests.test_point_data import BasePointDataTest

DATA_DIR = str(Path(__file__).parent.joinpath("data/nws_mocks"))


class TestNWSForecast(BasePointDataTest):
    @classmethod
    def get_side_effect(cls, *args, **kwargs):
        url = args[0]
        if ".gov/gridpoints" in url:
            with open(join(DATA_DIR, "meta_and_data.json")) as fp:
                data = json.load(fp)
        elif ".gov/points" in url:
            with open(join(DATA_DIR, "initial_meta.json")) as fp:
                data = json.load(fp)
        else:
            raise RuntimeError(f"{url} is an unknown option")

        obj = MagicMock()
        obj.json.return_value = data
        return obj

    @pytest.fixture(scope="class")
    def mocked_requests(self):
        with patch("requests.get") as mock_get:
            mock_get.side_effect = self.get_side_effect
            yield mock_get

    @pytest.fixture(scope="function")
    def station(self, mocked_requests):
        point1 = Point(-119, 43)
        pt = NWSForecastPointData(
            "test", None, initial_metadata=point1
        )
        yield pt

    @pytest.fixture(scope="class")
    def expected_meta(self):
        return Point(-118.99345926915265, 42.99291053264557, 5000.00016)

    @pytest.fixture(scope="class")
    def daily_expected(self, expected_meta):
        dts = [
            '2024-06-25T00:00:00+0000', '2024-06-26T00:00:00+0000',
            '2024-06-27T00:00:00+0000', '2024-06-28T00:00:00+0000',
            '2024-06-29T00:00:00+0000', '2024-06-30T00:00:00+0000',
            '2024-07-01T00:00:00+0000', '2024-07-02T00:00:00+0000',
            '2024-07-03T00:00:00+0000'
        ]
        temp_values = [
            26.82539683, 23.647343, 16.18357488, 14.19191919, 19.46859903,
            18.68686869, 16.06280193, 16.61835749, 27.22222222]
        df = pd.DataFrame()
        df["datetime"] = pd.to_datetime(dts)
        df["AIR TEMP"] = temp_values
        df['AIR TEMP_units'] = ["degC"] * len(df)
        df["site"] = ["test"] * len(df)
        df["datasource"] = ["NWS Forecast"] * len(df)
        df = gpd.GeoDataFrame(df, geometry=[expected_meta] * len(df))
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime",
                "geometry",
                "AIR TEMP",
                "site",
                "AIR TEMP_units",
                "datasource",
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    def test_get_metadata(self, station, expected_meta):
        result = station.metadata
        assert expected_meta == result

    def test_get_daily_data(self, station, daily_expected):
        response = station.get_daily_forecast(
            [NWSForecastVariables.TEMP],
        )
        pd.testing.assert_frame_equal(
            response.sort_index(axis=1),
            daily_expected.sort_index(axis=1)
        )

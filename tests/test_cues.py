from datetime import datetime
from os.path import join
from pathlib import Path
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pandas as pd
import pytest

from metloom.pointdata import CuesLevel1
from metloom.variables import CuesLevel1Variables
from tests.test_point_data import BasePointDataTest

DATA_DIR = str(Path(__file__).parent.joinpath("data/cues_mocks"))


class TestCuesStation(BasePointDataTest):

    @pytest.fixture(scope="function")
    def station(self):
        return CuesLevel1(None, None)

    @pytest.fixture(scope="class")
    def expected_meta(self):
        return gpd.points_from_xy(
            [-119.029128], [37.643093], [9661]
        )[0]

    @pytest.fixture(scope="class")
    def daily_expected(self, expected_meta):
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2020-03-15 08:00:00+0000", tz="UTC"),
                    "DOWNWARD SHORTWAVE RADIATION": 95.64,
                },
                {
                    "datetime": pd.Timestamp("2020-03-16 08:00:00+0000", tz="UTC"),
                    "DOWNWARD SHORTWAVE RADIATION": 86.87,
                },
                {
                    "datetime": pd.Timestamp("2020-03-17 08:00:00+0000", tz="UTC"),
                    "DOWNWARD SHORTWAVE RADIATION": 182.23,
                },

            ],
            geometry=[expected_meta] * 3,
        )
        df["DOWNWARD SHORTWAVE RADIATION_units"] = ["Watts/meter^2"] * len(df)
        df["site"] = ["CUES"] * len(df)
        df["datasource"] = ["UCSB CUES"] * len(df)
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime",
                "geometry",
                "DOWNWARD SHORTWAVE RADIATION",
                "site",
                "DOWNWARD SHORTWAVE RADIATION_units",
                "datasource",
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    @classmethod
    def get_url_response(cls, resp="daily"):
        if resp == 'daily':
            with open(join(DATA_DIR, "daily_response.txt")) as fp:
                data = fp.read()
        elif resp == 'hourly':
            with open(join(DATA_DIR, "hourly_response.txt")) as fp:
                data = fp.read()
        else:
            raise RuntimeError(f"{resp} is an unknown option")

        obj = MagicMock()
        obj.content = data.encode()
        return obj

    def test_get_metadata(self, station, expected_meta):
        assert expected_meta == station.metadata

    def test_get_daily_data(self, station, daily_expected):
        with patch("metloom.pointdata.cues.requests") as mock_requests:
            mock_requests.post.side_effect = [
                self.get_url_response(),
            ]
            response = station.get_daily_data(
                datetime(2020, 3, 15),
                datetime(2020, 3, 17),
                [CuesLevel1Variables.DOWNSHORTWAVE],
            )
        pd.testing.assert_frame_equal(
            response.sort_index(axis=1),
            daily_expected.sort_index(axis=1)
        )

    def test_get_hourly_data(self, station):
        """
        Test that we get hourly data correctly.
        This also uses the `UPSHORTWAVE` variable so we can test
        that the instrument specific implementation of variables is working.
        """
        with patch("metloom.pointdata.cues.requests") as mock_requests:
            mock_requests.post.side_effect = [
                self.get_url_response(resp="hourly"),
            ]
            resp = station.get_hourly_data(
                datetime(2020, 4, 1), datetime(2020, 4, 2),
                [CuesLevel1Variables.UPSHORTWAVE],
            )
        resp = resp.reset_index()
        assert resp["datetime"].values[0] == pd.to_datetime("2020-04-01 08")
        assert resp["datetime"].values[-1] == pd.to_datetime("2020-04-02 07")
        assert resp["UPWARD SHORTWAVE RADIATION"].values[0] == -9.78
        assert resp["UPWARD SHORTWAVE RADIATION"].values[-1] == -8.44
        assert all(resp["site"].values == "CUES")

    def test_points_from_geometry_failure(self, station):
        with pytest.raises(NotImplementedError):
            station.points_from_geometry(None, None)

import json
from datetime import datetime
from pathlib import Path
import geopandas as gpd
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from metloom.pointdata import norway
from metloom.variables import MetNorwayVariables


class TestMetNorway:
    MOCKS_DIR = Path(__file__).parent.joinpath("data/frost_mocks")

    @classmethod
    def _get_side_effect(cls, *args, **kwargs):
        """
        Side effect for requests.get
        """
        mock_resp = MagicMock()
        # mock observations endpoint
        if "observations/v0" in args[0]:

            if "air_temperature" in kwargs["params"]["elements"]:
                # Success case
                mock_resp.status_code = 200
                with open(cls.MOCKS_DIR.joinpath("hourly_temp.json")) as fp:
                    obj = json.load(fp)
                mock_resp.json.return_value = obj
            else:
                # Case of no data
                mock_resp.status_code = 412

        # mock metadata endpoint
        elif "sources/v0" in args[0]:
            params = kwargs["params"]
            mock_resp.status_code = 200
            with open(cls.MOCKS_DIR.joinpath("search.json")) as fp:
                obj = json.load(fp)
            ids = params.get("ids")
            if ids:
                # filter to ids
                data = obj["data"]
                data = [d for d in data if d["id"] in ids]
                obj["data"] = data

            mock_resp.json.return_value = obj
        else:
            raise NotImplementedError("No other method implemented")
        return mock_resp

    @staticmethod
    def _post_side_effect(*args, **kwargs):
        url = args[0]
        if "auth/accessToken" in url:
            obj = MagicMock()
            obj.json.return_value = {
                "access_token": "FAKE",
                "expires_in": 3600
            }
        else:
            raise NotImplementedError("No other method implemented")
        return obj

    @pytest.fixture(scope='session')
    def token_file(self):
        """
        Json token file fixture for mocking having a token
        """
        d = {
            'client_id': '####',
            'client_secret': '####'
        }
        json_file = Path(__file__).parent.joinpath('frost_token.json')

        with open(json_file, 'w+') as fp:
            json.dump(d, fp)

        yield json_file
        # Clean up
        if json_file.is_file():
            json_file.unlink()

    @pytest.fixture(scope="class")
    def mock_request(self):
        with patch("metloom.pointdata.norway.requests") as mr:
            mr.get.side_effect = self._get_side_effect
            mr.post.side_effect = self._post_side_effect
            yield mr

    @pytest.fixture(scope="class")
    def obj(self, mock_request, token_file):
        yield norway.MetNorwayPointData(
            "SN46432", "BASURA", token_json=token_file
        )

    def test_daily_data(self, obj):
        result = obj.get_daily_data(
            datetime(2023, 8, 1), datetime(2023, 8, 5),
            [MetNorwayVariables.TEMP]
        )
        assert result["AIR TEMP"].values == pytest.approx(
            [13.44583333, 14.325, 13.86666667, 11.64166667]
        )
        assert all(result["AIR TEMP_units"] == ["degC"] * 4)
        assert all(result["quality_code"] == "resampled")

    def test_hourly_data(self, obj):
        result = obj.get_hourly_data(
            datetime(2023, 8, 1), datetime(2023, 8, 5),
            [MetNorwayVariables.TEMP]
        )

        assert len(result) == 96
        assert all(result["AIR TEMP_units"] == ["degC"] * 96)
        assert result["AIR TEMP"].mean() == pytest.approx(13.319792)

    def test_daily_nodata(self, obj):
        result = obj.get_daily_data(
            datetime(2023, 8, 1), datetime(2023, 8, 5),
            [MetNorwayVariables.SWE]
        )
        assert result is None

    def test_points_from_geometry(self, mock_request, token_file):
        shp = gpd.read_file(
            self.MOCKS_DIR.joinpath("box.shp")
        )
        result = norway.MetNorwayPointData.points_from_geometry(
            shp, [MetNorwayVariables.TEMP], token_json=token_file
        )
        assert len(result.points) == 16
        assert result.points[0].id == "SN46432"

    @pytest.mark.parametrize(
        "ref_time, offset, resolution, timeseries_id, expected", [
            ("2023-08-01T00:00:00.000Z", "PT0H", "PT1H", 0,
             "2023-08-01T00:00:00.000Z"),
            ("2023-08-01T00:00:00.000Z", "PT6H", "PT1H", 0,
             "2023-08-01T06:00:00.000Z"),
            ("2023-08-01T00:00:00.000Z", "PT6H", "PT1H", 1,
             "2023-08-01T07:00:00.000Z"),
            ("2023-08-01T00:00:00.000Z", "PT6H", "PT12H", 1,
             "2023-08-01T18:00:00.000Z"),
        ]
    )
    def test_observation_time(
        self, ref_time, offset, resolution, timeseries_id, expected
    ):
        result = norway.MetNorwayPointData._time_info_to_observation_time(
            ref_time, offset, resolution, timeseries_id
        )
        assert result == pd.to_datetime(expected)

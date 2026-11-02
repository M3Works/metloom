"""
Mocked data are real files from the CW3E datashare

* ELK - 2024-05-27 to 2024-05-29. The README lists a 25 column layout
  for these dates but the files have 27 columns.
* MYD - 2025-01-05 to 2025-01-06. Old 12 column layout with precip in
  inches and snow depth in cm
* FMI - README only. Has an undated layout following a dated one
"""
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

from metloom.pointdata import CW3EPointData
from metloom.variables import CW3EVariables

DATA_DIR = Path(__file__).parent.joinpath("data/cw3e_mocks")


def get_side_effect(url, *args, **kwargs):
    local_path = DATA_DIR.joinpath(Path(url).name)
    obj = MagicMock()
    if local_path.exists():
        obj.status_code = 200
        obj.content = local_path.read_bytes()
        obj.text = local_path.read_text()
    else:
        obj.status_code = 404
    return obj


class TestCW3EPointData:

    @pytest.fixture(scope="function")
    def mocked_requests(self):
        with patch("metloom.pointdata.cw3e.requests") as mock_requests:
            mock_requests.get.side_effect = get_side_effect
            session = mock_requests.Session.return_value.__enter__.return_value
            session.get.side_effect = get_side_effect
            yield mock_requests

    @pytest.fixture(scope="function")
    def cache_dir(self):
        cache = Path(__file__).parent.joinpath("cache")
        yield cache
        if cache.is_dir():
            shutil.rmtree(cache)

    @pytest.fixture(scope="function")
    def station(self, mocked_requests, cache_dir, station_id):
        pnt = CW3EPointData(station_id, cache=cache_dir)
        pnt._verify_station()
        yield pnt

    @pytest.mark.parametrize("column, expected", [
        ("Temperature (C)", ("temperature", "C")),
        ("Average Wind Speed(m/s)", ("average wind speed", "m/s")),
        ("Soil Temperature - 5 cm (C)", ("soil temperature - 5 cm", "C")),
        ("Snow depth (cm)", ("snow depth", "cm")),
        ("Year", ("year", None)),
    ])
    def test_parse_column(self, column, expected):
        assert CW3EPointData._parse_column(column) == expected

    @pytest.mark.parametrize("readme, expected", [
        ("ELK_HourlyData_README.txt", [
            (pd.Timestamp("2024-05-29"), None, 27),
            (pd.Timestamp("2023-11-08"), pd.Timestamp("2024-05-28"), 25),
        ]),
        ("FMI_HourlyData_README.txt", [
            (pd.Timestamp("2024-10-01"), None, 20),
            (None, None, 20),
        ]),
    ])
    def test_parse_readme(self, readme, expected):
        layouts = CW3EPointData._parse_readme(DATA_DIR.joinpath(readme).read_text())
        result = [(lyt["start"], lyt["end"], len(lyt["columns"])) for lyt in layouts]
        assert result == expected

    @pytest.mark.parametrize("station_id, dt, ncolumns, expected_start", [
        # README says 25 columns for this date but the files have 27
        ("ELK", datetime(2024, 5, 27), 27, pd.Timestamp("2024-05-29")),
        ("ELK", datetime(2024, 1, 1), 25, pd.Timestamp("2023-11-08")),
        # Undated layout is used outside the dated range
        ("FMI", datetime(2024, 5, 1), 20, None),
        ("FMI", datetime(2025, 5, 1), 20, pd.Timestamp("2024-10-01")),
    ])
    def test_select_layout(self, station, dt, ncolumns, expected_start):
        layout = station._select_layout(dt, ncolumns)
        assert layout["start"] == expected_start

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_select_layout_no_match(self, station):
        assert station._select_layout(datetime(2024, 1, 1), 3) is None

    @pytest.mark.parametrize("start, end, expected", [
        (datetime(2024, 5, 27), datetime(2024, 5, 29),
         ["ELK_20240527.txt", "ELK_20240528.txt"]),
        (datetime(2024, 5, 27), datetime(2024, 5, 29, 1),
         ["ELK_20240527.txt", "ELK_20240528.txt", "ELK_20240529.txt"]),
    ])
    def test_file_urls(self, start, end, expected):
        urls = CW3EPointData("ELK")._file_urls("ELK", start, end)
        assert [Path(u).name for u in urls] == expected
        assert urls[0] == (
            "https://cw3e-datashare.ucsd.edu/CW3E_SurfaceMetObs/"
            "ELK/2024/Hourly/ELK_20240527.txt"
        )

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_get_hourly_data(self, station):
        df = station.get_hourly_data(
            datetime(2024, 5, 27), datetime(2024, 5, 30),
            [CW3EVariables.TEMP, CW3EVariables.SNOWDEPTH,
             CW3EVariables.SOILMOISTURE10CM]
        )
        first = df.loc[(pd.Timestamp("2024-05-27 00:00"), "ELK")]
        assert first["AIR TEMP"] == pytest.approx(13.55)
        assert first["SNOWDEPTH"] == pytest.approx(0.13)
        assert first["SOIL MOISTURE -10CM"] == pytest.approx(0.19)
        assert first["SOIL MOISTURE -10CM_units"] == "fraction"
        assert df["AIR TEMP"].count() == 72
        assert df["datasource"].iloc[0] == "CW3E"

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_get_daily_data(self, station):
        variables = [
            CW3EVariables.PRECIPITATION, CW3EVariables.WINDSPEEDMAX,
            CW3EVariables.WINDDIR, CW3EVariables.TEMP
        ]
        df = station.get_daily_data(
            datetime(2024, 5, 27), datetime(2024, 5, 30), variables
        )
        raw = pd.read_csv(DATA_DIR.joinpath("ELK_20240529.txt"), header=None)
        day = df.loc[(pd.Timestamp("2024-05-29"), "ELK")]
        assert day["PRECIPITATION"] == pytest.approx(raw[7].sum())
        assert day["MAX WIND SPEED"] == pytest.approx(raw[9].max())
        assert day["AIR TEMP"] == pytest.approx(raw[5].mean())
        rad = np.deg2rad(raw[10])
        expected = np.rad2deg(np.arctan2(np.sin(rad).mean(), np.cos(rad).mean())) % 360
        assert day["WIND DIRECTION"] == pytest.approx(expected)

    @pytest.mark.parametrize("station_id", ["MYD"])
    def test_unit_conversion(self, station):
        df = station.get_hourly_data(
            datetime(2025, 1, 5), datetime(2025, 1, 6),
            [CW3EVariables.PRECIPITATION, CW3EVariables.SNOWDEPTH]
        )
        raw = pd.read_csv(DATA_DIR.joinpath("MYD_20250105.txt"), header=None)
        # inches to mm and cm to m
        np.testing.assert_allclose(
            df["PRECIPITATION"].dropna().astype(float).values, raw[10] * 25.4
        )
        np.testing.assert_allclose(
            df["SNOWDEPTH"].dropna().astype(float).values, raw[11] * 0.01
        )

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_missing_variable(self, station):
        """ Fuel moisture is all missing in these files"""
        df = station.get_hourly_data(
            datetime(2024, 5, 27), datetime(2024, 5, 28),
            [CW3EVariables.TEMP, CW3EVariables.FUELMOISTURE]
        )
        assert "AIR TEMP" in df.columns
        assert "FUEL MOISTURE" not in df.columns

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_missing_files(self, station, cache_dir):
        """ Days without files return None and are cached as empty"""
        df = station.get_hourly_data(
            datetime(2020, 1, 1), datetime(2020, 1, 2), [CW3EVariables.TEMP]
        )
        assert df is None
        cached = cache_dir.joinpath("CW3E", "ELK", "ELK_20200101.txt")
        assert cached.exists() and cached.stat().st_size == 0

    @pytest.mark.parametrize("station_id", ["ELK"])
    def test_uses_cache(self, station, mocked_requests):
        args = (datetime(2024, 5, 27), datetime(2024, 5, 28), [CW3EVariables.TEMP])
        station.get_hourly_data(*args)
        session = mocked_requests.Session.return_value.__enter__.return_value
        assert session.get.call_count == 1
        station.get_hourly_data(*args)
        assert session.get.call_count == 1

    def test_points_from_geometry(self):
        # Upper Yampa
        aoi = gpd.GeoDataFrame(geometry=[box(-107.5, 40, -106.5, 41)], crs=4326)
        points = CW3EPointData.points_from_geometry(aoi, [CW3EVariables.TEMP])
        ids = sorted(p.id for p in points)
        assert ids == ["AZR", "CMC", "CPR", "ELK", "HOW", "KNT", "RED", "SKI", "TCS"]
        assert points.to_dataframe()["datasource"].unique().tolist() == ["CW3E"]

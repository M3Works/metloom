import os
import geopandas as gpd
from shapely.geometry import Polygon
from datetime import datetime
from metloom.pointdata.sail import SAILPointData
from metloom.variables import SAILStationVariables
from unittest import mock
import pytest


@pytest.fixture
def mock_requests_get():
    with mock.patch("requests.get") as mock_get:
        # query
        obj0 = mock.MagicMock()
        obj0.content = None
        obj0.raise_for_status = lambda: None
        obj0.json = lambda: {
            "files": [
                os.path.join(os.path.dirname(__file__), "data", "gucwbpluvio2M1.a1.20230101.000000.nc"),
                os.path.join(os.path.dirname(__file__), "data", "gucwbpluvio2M1.a1.20230102.000000.nc"),
            ]
        }

        # mod
        obj1 = mock.MagicMock()
        with open(
            os.path.join(os.path.dirname(__file__), "data", "gucwbpluvio2M1.a1_accum_rtnrt_20230101_20230102.csv"), "rb"
        ) as f:
            obj1.content = f.read()
        obj1.raise_for_status = lambda: None

        mock_get.side_effect = [obj0, obj1]
        yield mock_get


@pytest.fixture(scope="session")
def setup_env():
    os.environ["M3W_ARM_USER_ID"] = "skroob"
    os.environ["M3W_ARM_ACCESS_TOKEN"] = "12345"


def test_get_hourly_data(mock_requests_get, setup_env):
    # the nc files exist, this test will not download
    obj = SAILPointData("GUC:M1")
    df = obj.get_hourly_data("2023-01-01", "2023-01-02", [SAILStationVariables.PRECIPITATION])
    assert df is not None
    assert len(df) == 48
    assert df.iloc[24]["PRECIPITATION"] == pytest.approx(0.01233333)
    assert df.iloc[24]["PRECIPITATION_units"] == "mm"


def test_get_daily_data(mock_requests_get, setup_env):
    # this mocks the necessary function to test the download code
    obj = SAILPointData(station_id="GUC:M1")
    df = obj.get_daily_data("2023-01-01", "2023-01-02", [SAILStationVariables.PRECIPITATION])
    assert df is not None
    assert len(df) == 2
    assert df.iloc[1]["PRECIPITATION"] == pytest.approx(0.004354167)
    assert df.iloc[1]["PRECIPITATION_units"] == "mm"


def test_check_start_end_dates():
    obj = SAILPointData(station_id="GUC:M1")
    # Test with end_date before start_date
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-02"), datetime.fromisoformat("2023-01-01"))
    # Test with start_date before 2021-09-01
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2020-09-01"), datetime.fromisoformat("2023-01-01"))
    # Test with end_date after after 2023-06-16
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-01"), datetime.fromisoformat("2024-01-01"))


@pytest.fixture
def station_inside_gdf():
    geometry = Polygon([(-106.882456, 38.957106), (-107.049037, 38.881546), (-107.036609, 39.004078)])
    return gpd.GeoSeries(geometry, crs="EPSG:4326")


@pytest.fixture
def station_outside_gdf():
    geometry = Polygon([(-106.882456, 38.94), (-107.049037, 38.97), (-107.036609, 39.004078)])
    return gpd.GeoSeries(geometry, crs="EPSG:4326")


def test_points_from_geometry_within_false(station_inside_gdf, station_outside_gdf):
    # bounding box, both should be within bounding box
    gdf = SAILPointData.points_from_geometry(
        geometry=station_inside_gdf,
        variables=[SAILStationVariables.PRECIPITATION],
        within_geometry=False,
        buffer=0.0,
    )
    assert len(gdf) == 1

    gdf = SAILPointData.points_from_geometry(
        geometry=station_outside_gdf,
        variables=[SAILStationVariables.PRECIPITATION],
        within_geometry=False,
        buffer=0.0,
    )
    assert len(gdf) == 1


def test_points_from_geometry_within_true(station_inside_gdf, station_outside_gdf):
    gdf = SAILPointData.points_from_geometry(
        geometry=station_inside_gdf,
        variables=[SAILStationVariables.PRECIPITATION],
        within_geometry=True,
        buffer=0.0,
    )
    assert len(gdf) == 1

    gdf = SAILPointData.points_from_geometry(
        geometry=station_outside_gdf,
        variables=[SAILStationVariables.PRECIPITATION],
        within_geometry=True,
        buffer=0.0,
    )
    assert len(gdf) == 0


def test_points_from_geometry_within_true_with_buffer(station_inside_gdf, station_outside_gdf):
    gdf = SAILPointData.points_from_geometry(
        geometry=station_outside_gdf,
        variables=[SAILStationVariables.PRECIPITATION],
        within_geometry=True,
        buffer=0.3,
    )
    assert len(gdf) == 1

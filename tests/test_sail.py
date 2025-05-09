import os

from datetime import datetime
from metloom.pointdata.sail import SAILPointData
from metloom.variables import SAILStationVariables
from unittest import mock
import pytest


@pytest.fixture
def mock_requests_get():
    with mock.patch("requests.get") as mock_get:
        obj = mock.MagicMock()
        obj.content = None
        obj.raise_for_status = lambda: None
        obj.json = lambda: {
            "files": [
                os.path.join(os.path.dirname(__file__), "data", "gucwbpluvio2M1.a1.20230101.000000.nc"),
                os.path.join(os.path.dirname(__file__), "data", "gucwbpluvio2M1.a1.20230102.000000.nc"),
            ]
        }
        mock_get.return_value = obj
        yield mock_get


@pytest.fixture
def mock_open():
    with mock.patch("builtins.open", mock.mock_open()) as mock_file:
        yield mock_file


@pytest.fixture
def mock_path_exists():
    with mock.patch("pathlib.Path.exists", return_value=False) as mock_exists:
        yield mock_exists


@pytest.fixture(scope="session")
def setup_env():
    os.environ["M3W_ARM_USER_ID"] = "skroob"
    os.environ["M3W_ARM_ACCESS_TOKEN"] = "12345"


def test_get_hourly_data(mock_requests_get, setup_env):
    # the nc files exist, this test will not download
    obj = SAILPointData()
    df = obj.get_hourly_data("2023-01-01", "2023-01-02", [SAILStationVariables.PRECIPITATION])
    assert df is not None
    assert len(df) == 48
    assert df.iloc[24]["PRECIPITATION"] == 0.74
    assert df.iloc[24]["PRECIPITATION_units"] == "mm"


def test_get_daily_data(mock_requests_get, mock_open, mock_path_exists, setup_env):
    # this mocks the necessary function to test the download code
    obj = SAILPointData()
    df = obj.get_daily_data("2023-01-01", "2023-01-02", [SAILStationVariables.PRECIPITATION])
    assert df is not None
    assert len(df) == 2
    assert df.iloc[1]["PRECIPITATION"] == 6.27
    assert df.iloc[1]["PRECIPITATION_units"] == "mm"


def test_check_start_end_dates():
    obj = SAILPointData()
    # Test with end_date before start_date
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-02"), datetime.fromisoformat("2023-01-01"))
    # Test with start_date before 2021-09-01
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2020-09-01"), datetime.fromisoformat("2023-01-01"))
    # Test with end_date after after 2023-06-16
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-01"), datetime.fromisoformat("2024-01-01"))

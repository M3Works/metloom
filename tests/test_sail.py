from datetime import datetime
from metloom.pointdata.sail import SAILPointData
from metloom.variables import SAILStationVariables
import pytest

def test_get_hourly_data():
    obj = SAILPointData()
    df = obj.get_hourly_data("2023-01-01", "2023-01-02", [SAILStationVariables.PRECIPITATION])
    assert df is not None
    assert len(df) == 48
    assert df.iloc[24]["PRECIPITATION"] == 0.74
    assert df.iloc[24]["PRECIPITATION_units"] == "mm"

def test_get_daily_data():
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
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-02"), datetime.fromisoformat( "2023-01-01"))
    # Test with start_date before 2021-09-01
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2020-09-01"), datetime.fromisoformat("2023-01-01"))
    # Test with end_date after after 2023-06-16
    with pytest.raises(ValueError):
        obj._check_start_end_dates(datetime.fromisoformat("2023-01-01"), datetime.fromisoformat("2024-01-01"))

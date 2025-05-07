
from metloom.pointdata.sail import SAILPointData
from metloom.variables import SAILStationVariables

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

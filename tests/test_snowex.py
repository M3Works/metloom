"""
Tests our snowex reader which is pulling down csv data from the nsidc
"""
from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet
from datetime import datetime

def test_snowex():
    pnt = SnowExMet('LSOS')
    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 15)
    df = pnt.get_daily_data(SnowExVariables)

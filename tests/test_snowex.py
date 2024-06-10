"""
Tests our snowex reader which is pulling down csv data from the nsidc
"""
import matplotlib.pyplot as plt

from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet
from datetime import datetime

def test_snowex():
    pnt = SnowExMet('LSOS', "Local Scale Obs Site")
    start = datetime(2017, 1, 1)
    end = datetime(2017, 5, 1)
    df = pnt.get_hourly_data(start, end, [SnowExVariables.UPSHORTWAVE])
    fig, ax = plt.subplots(1)
    df = df.reset_index().set_index('datetime')
    ax.plot(df.index, df[SnowExVariables.UPSHORTWAVE.code])
    # df.reset_index().set_index('datetime').plot()
    plt.show()

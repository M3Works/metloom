"""
Tests our snowex reader which is pulling down csv data from the nsidc
"""
import matplotlib.pyplot as plt
import pytest

from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet
from datetime import datetime
from pathlib import Path
import geopandas as gpd


DATA_DIR = str(Path(__file__).parent.joinpath("data/snowex_mocks"))

def test_snowex():
    pnt = SnowExMet('GMSP')
    start = datetime(2018, 1, 1)
    end = datetime(2018, 5, 1)
    var = SnowExVariables.SNOWDEPTH
    df = pnt.get_daily_data(start, end, [var])
    fig, ax = plt.subplots(1)
    df = df.reset_index().set_index('datetime')
    ax.plot(df.index, df[var.name])
    # df.reset_index().set_index('datetime').plot()
    plt.show()


@pytest.mark.skip("Not working yet")
def test_within_geometry():

    box_df = gpd.read_file(Path(DATA_DIR).joinpath('gm_box.shp'))
    df = SnowExMet.points_from_geometry(box_df, [SnowExVariables.TEMP_20FT])

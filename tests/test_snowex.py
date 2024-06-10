"""
Tests our snowex reader which is pulling down csv data from the nsidc
All mock data is cut to the first 2 weeks of january in 2017

"""
import os
import shutil

import matplotlib.pyplot as plt
import pytest

from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet
from datetime import datetime
from pathlib import Path
import geopandas as gpd
from unittest.mock import Mock


DATA_DIR = str(Path(__file__).parent.joinpath("data/snowex_mocks"))

class TestSnowEx:
    def copy_file(self, arg):
        print('HERE')
        f = arg.split('/')[-1]
        file = Path(DATA_DIR).joinpath(f)
        shutil.copy(file, self.cache_dir() + file.name)

    @pytest.fixture(scope='function')
    def cache_dir(self):
        """Cachae dir where data is being downloaded to"""
        cache = Path(__file__).parent.joinpath('cache')
        yield cache
        if cache.is_dir():
            os.remove(cache)
    @pytest.fixture(scope='function')
    def station(self, station_id):
        pnt = SnowExMet(station_id)
        pnt._download = Mock(side_effect=self.copy_file)
        yield pnt

    @pytest.mark.parametrize('station_id, expected',[
        ('GMSP', 'Grand Mesa Study Plot')
    ])
    def test_station_name(self, station, station_id, expected):
        """ Check auto assignment of the name"""
        station._verify_station()
        assert station.name == expected

    @pytest.mark.parametrize('station_id, variable', [
        ('GMSP', SnowExVariables.TEMP_10FT)
    ])
    def test_station_name(self, station, station_id, variable):
        """ Check auto assignment of the name"""
        df = station.get_daily_data(datetime(2017, 1, 1), datetime(2017, 1,15), [variable])
        assert df.index.freq == 'D'

#
# def test_snowex():
#     pnt = SnowExMet('MM')
#     start = datetime(2018, 1, 1)
#     end = datetime(2018, 5, 1)
#     var = SnowExVariables.SNOWDEPTH
#     df = pnt.get_daily_data(start, end, [var])
#     fig, ax = plt.subplots(1)
#     df = df.reset_index().set_index('datetime')
#     ax.plot(df.index, df[var.name])
#     # df.reset_index().set_index('datetime').plot()
#     plt.show()
#
#
# @pytest.mark.skip("Not working yet")
# def test_within_geometry():
#
#     box_df = gpd.read_file(Path(DATA_DIR).joinpath('gm_box.shp'))
#     df = SnowExMet.points_from_geometry(box_df, [SnowExVariables.TEMP_20FT])

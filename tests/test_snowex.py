"""
Tests our snowex reader which is pulling down csv data from the nsidc
All mock data is cut to the first 2 weeks of january in 2017. 2018 for GMSP

"""
import shutil

import matplotlib.pyplot as plt
import pytest

from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet
from datetime import datetime
from pathlib import Path
import geopandas as gpd
from unittest.mock import Mock
from unittest.mock import patch

DATA_DIR = str(Path(__file__).parent.joinpath("data/snowex_mocks"))

class TestSnowEx:
    def copy_file(self, url):
        print('HERE')
        file = Path(DATA_DIR).joinpath(Path(url).name)
        cache = Path(__file__).parent.joinpath('cache')
        shutil.copy(file, cache.joinpath(file.name))

    @pytest.fixture(scope='function')
    def cache_dir(self):
        """Cachae dir where data is being downloaded to"""
        cache = Path(__file__).parent.joinpath('cache')
        yield cache
        if cache.is_dir():
            shutil.rmtree(cache)

    @pytest.fixture(scope='function')
    def station(self, cache_dir, station_id):
        with patch.object(SnowExMet, '_download', new=self.copy_file):
            pnt = SnowExMet(station_id)
            yield pnt

    @pytest.mark.parametrize('station_id, expected',[
        ('GMSP', 'Grand Mesa Study Plot')
    ])
    def test_station_name(self, station, station_id, expected):
        """ Check auto assignment of the name"""
        station._verify_station()
        assert station.name == expected

    @pytest.mark.parametrize('station_id, variable, expected_mean', [
        ('LSOS', SnowExVariables.TEMP_10FT, -6.88556),
        ('MW', SnowExVariables.SNOWDEPTH, 1.04383)
    ])
    def test_get_daily_data(self, station, station_id, variable, expected_mean):
        """ Check auto assignment of the name"""
        df = station.get_daily_data(datetime(2017, 1, 1), datetime(2017, 1,15), [variable])
        # Assert it's a daily timeseries
        assert df.index.get_level_values('datetime').inferred_freq == 'D'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

    @pytest.mark.parametrize('station_id, variable, expected_mean', [
        ('LSOS', SnowExVariables.TEMP_10FT, -7.854865),
        ('MW', SnowExVariables.SNOWDEPTH, 0.98691)
    ])
    def test_get_hourly_data(self, station, station_id, variable, expected_mean):
        """ Check auto assignment of the name"""
        df = station.get_hourly_data(datetime(2017, 1, 1, 11), datetime(2017, 1, 10, 23),
                                    [variable])
        # Assert it's hourly timeseries
        assert df.index.get_level_values('datetime').inferred_freq == 'H'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

    @pytest.mark.parametrize('within_geom, expected_count', [
        (True, 1),
        (False, 3)
    ])
    def test_within_geometry(self, within_geom, expected_count):
        """ Use the within geometry on downloaded stations """
        search_poly = gpd.read_file(Path(DATA_DIR).joinpath('gm_polygon.shp'))
        df = SnowExMet.points_from_geometry(search_poly, [SnowExVariables.TEMP_20FT],
                                            within_geometry=within_geom)
        assert len(df.index) == expected_count

def test_pull_real_data():
    pnt = SnowExMet('GMSP')
    var = SnowExVariables.SNOWDEPTH
    df = pnt.get_daily_data(datetime(2018, 1, 1), datetime(2018, 1, 15), [var])
    df = df.reset_index().set_index('datetime')
    fig, ax = plt.subplots(1)
    ax.plot(df.index, df[var.name])
    plt.show()
    print(df)

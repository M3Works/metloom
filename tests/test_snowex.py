"""
Tests our snowex reader which is pulling down csv data from the nsidc
All mock data is cut to the first 2 weeks of january in 2017. 2018 for GMSP

"""
import shutil
import pytest
from datetime import datetime, timedelta
from pathlib import Path
import geopandas as gpd
from unittest.mock import patch

from metloom.variables import SnowExVariables
from metloom.pointdata import SnowExMet


DATA_DIR = str(Path(__file__).parent.joinpath("data/snowex_mocks"))


class TestSnowEx:
    def copy_file(self, urls):
        files = []
        for url in urls:
            file = Path(DATA_DIR).joinpath(Path(url).name)
            cache = Path(__file__).parent.joinpath('cache')
            shutil.copy(file, cache.joinpath(file.name))
            files.append(file)
        return files

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
            pnt = SnowExMet(station_id, cache=cache_dir)
            yield pnt

    @pytest.mark.parametrize('station_id, expected', [
        ('GMSP', 'Grand Mesa Study Plot')
    ])
    def test_station_name(self, station, station_id, expected):
        """ Check auto assignment of the name"""
        station._verify_station()
        assert station.name == expected

    @pytest.mark.parametrize('station_id, variable, expected_mean', [
        ('LSOS', SnowExVariables.TEMP_10FT, -7.07817),
        ('MW', SnowExVariables.SNOWDEPTH, 1.03503),
    ])
    def test_get_daily_data(self, station, station_id, variable, expected_mean):
        """ Check data pulling """
        df = station.get_daily_data(datetime(2017, 1, 1),
                                    datetime(2017, 1, 15), [variable])

        # Assert it's a daily timeseries
        assert df.index.get_level_values('datetime').inferred_freq == 'D'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

    @pytest.mark.parametrize('station_id, variable, expected_mean', [
        # Test a couple stations with different variables
        ('LSOS', SnowExVariables.TEMP_10FT, -7.86599),
        ('MW', SnowExVariables.SNOWDEPTH, 0.98625),

    ])
    def test_get_hourly_data(self, station, station_id, variable, expected_mean):
        """ Check auto assignment of the name"""
        df = station.get_hourly_data(datetime(2017, 1, 1, 11),
                                     datetime(2017, 1, 10, 23),
                                     [variable])

        # Assert it's hourly timeseries
        assert df.index.get_level_values('datetime').inferred_freq.lower() == 'h'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

    @pytest.mark.parametrize("station_id, variable, start", [
        # GMSP doesnt have radiation
        ('GMSP', SnowExVariables.UPSHORTWAVE, datetime(2018, 1, 1, 11)),
        # GMSP has no data in Jan - 2017
        ('GMSP', SnowExVariables.SNOWDEPTH, datetime(2017, 1, 1, 11))

    ])
    def test_get_daily_none(self, station, station_id, variable, start):
        """ Test when a station doesn't have something, its handled"""
        end = start + timedelta(days=1)
        df = station.get_daily_data(start, end, [variable])
        assert df is None

    @pytest.mark.parametrize('within_geom, buffer, expected_count', [
        # Inside the geom is only one station
        (True, 0.0, 1),
        # inside the bounds of the geom is 3 stations
        (False, 0.0, 3),
        # Test Buffer use
        (True, 0.1, 4)
    ])
    def test_within_geometry(self, within_geom, buffer, expected_count):
        """ Use the within geometry on downloaded stations """
        search_poly = gpd.read_file(Path(DATA_DIR).joinpath('gm_polygon.shp'))
        df = SnowExMet.points_from_geometry(search_poly,
                                            [SnowExVariables.TEMP_20FT],
                                            within_geometry=within_geom,
                                            buffer=buffer)
        assert len(df.index) == expected_count

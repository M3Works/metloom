"""
Note SASP/SBSP has data stored in two files with a range of years
The mocked data is provided to test but is cropped to shorten the files

Double Note: DOY 1 == Jan 1

Mocked data has been cropped to:

* SASP/SBSP 2003-2009 -> 2009 DOY 60-80
* SASP/SBSP 2010-2023 -> 2023 DOY 60-80
* SBSG - 2023 DOY 100-120
* PTSP - 2023 DOY 60-80

"""
import pytest

from metloom.pointdata import CSASMet
from metloom.pointdata.csas import InvalidDateRange
from metloom.variables import CSASVariables
from datetime import datetime, timedelta
from pathlib import Path
import shutil
from unittest.mock import patch, MagicMock
import requests


DATA_DIR = str(Path(__file__).parent.joinpath("data/csas_mocks"))

# Convenient Dates for testing
DT_20090301 = datetime(2009, 3, 1)
DT_20090315 = datetime(2009, 3, 15)
DT_20230301 = datetime(2023, 3, 1)
DT_20230315 = datetime(2023, 3, 15)
DT_20230601 = datetime(2023, 6, 1)
DT_20230615 = datetime(2023, 6, 15)


class TestCSASMet:

    @classmethod
    def get_side_effect(cls, *args, **kwargs):
        url = Path(args[0])
        local_path = Path(__file__).parent.joinpath(DATA_DIR).joinpath(url.name)

        obj = MagicMock()
        with open(local_path, 'rb') as f:
            lines = f.readlines()

        obj.iter_lines.return_value = lines
        obj.__enter__.return_value = obj  # Black magic?
        obj.__exit__.return_value = None
        return obj

    @pytest.fixture(scope="class")
    def mocked_requests(self):
        with patch("metloom.pointdata.files.requests.get") as mock_get:
            mock_get.side_effect = self.get_side_effect
            yield mock_get

    def copy_files(self, urls):
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
    def station(self, mocked_requests, cache_dir, station_id):
        pnt = CSASMet(station_id, cache=cache_dir)
        yield pnt

    @pytest.mark.parametrize('year, doy, hour, expected', [
        (2024, 92, 1400, datetime(2024, 4, 1, 14)),
        # Check doy 1 is jan 1
        (2024, 1, 0, datetime(2024, 1, 1))

    ])
    def test_parse_datetime(self, year, doy, hour, expected):
        dt = CSASMet._parse_datetime({'Year': year, 'DOY': doy, 'Hour': hour})
        assert dt == expected

    @pytest.mark.parametrize("station_id, start, end, expected", [
        # Two stations have different files depending on the years
        ('SBSP', datetime(2003, 1, 1),
         datetime(2003, 1, 1), ['SBSP_1hr_2003-2009.csv']),
        ('SBSP', datetime(2010, 1, 1),
         datetime(2010, 1, 1), ['SBSP_1hr_2010-2023.csv']),
        # Test straddling the files
        ('SBSP', datetime(2008, 1, 1),
         datetime(2010, 1, 1), ['SBSP_1hr_2003-2009.csv', 'SBSP_1hr_2010-2023.csv']),
        # test stations that don't have year specific timeframes
        ('PTSP', datetime(2010, 1, 1), datetime(2010, 1, 2), ["PTSP_1hr.csv"]),
        ('SBSG', datetime(2010, 1, 1), datetime(2010, 1, 2), ["SBSG_1hr.csv"])
    ])
    def test_file_urls(self, station_id, start, end, expected):
        pnt = CSASMet(station_id)
        pnt._verify_station()
        urls = pnt._file_urls(station_id, start, end)
        names = sorted([Path(url).name for url in urls])
        assert names == expected

    @pytest.mark.parametrize("station_id, start, end", [
        # Test pre 2003
        ('SBSP', datetime(2002, 1, 1),
         datetime(2003, 1, 1)),
        # Test post 2023 exception
        ('SBSP', datetime(2010, 1, 1),
         datetime(2030, 1, 1))
    ])
    def test_file_urls_exception(self, station_id, start, end):
        """
        With files there are hard timeframes, test ane exception is raised when
        this is the case
        """
        pnt = CSASMet(station_id)
        pnt._verify_station()
        with pytest.raises(InvalidDateRange):
            pnt._file_urls(station_id, start, end)

    @pytest.mark.parametrize("station_id, year", [
        # Test the two SBSP urls
        ('SBSP', 2009),
        ('SBSP', 2010),
        # Test the two SASP urls
        ('SASP', 2009),
        ('SASP', 2010),
        # Test the Stream gauges url
        ('SBSG', 2005),
        # Test the PTSP url
        ('PTSP', 2003),
    ])
    def test_links_are_valid(self, station_id, year):
        """
        Seeking answers from CSAS on how and when these files are updated. Until then
        this will serve as a nice way to check the files are workin still
        """
        start = datetime(year, 1, 1)
        end = start + timedelta(days=1)

        pnt = CSASMet(station_id)
        pnt._verify_station()
        urls = pnt._file_urls(station_id, start, end)

        resp = requests.head(urls[0])
        assert resp.ok

    @pytest.mark.parametrize('station_id, variable, start, end, expected_mean', [
        ('SASP', CSASVariables.SURF_TEMP, DT_20230301, DT_20230315, -11.59220),
        # Span two files, Test data wont be complete but requires two files
        ('SASP', CSASVariables.SURF_TEMP, DT_20090301, DT_20230315, -10.35284),
    ])
    def test_get_daily_data(self, station, station_id, variable, start, end,
                            expected_mean):
        """ Check pulling two weeks of data """

        df = station.get_daily_data(start, end, [variable])
        # Assert it's a daily timeseries
        assert df.index.get_level_values('datetime').inferred_freq == 'D'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

    @pytest.mark.parametrize('station_id, variable, start, end, expected_mean', [
        ('SASP', CSASVariables.SURF_TEMP, DT_20230301, DT_20230315, -11.49969),
    ])
    def test_get_hourly_data(self, station, station_id, variable, start, end,
                             expected_mean):
        """ Check pulling two weeks of data """

        df = station.get_hourly_data(start, end, [variable])

        # Assert it's a daily timeseries
        assert df.index.get_level_values('datetime').inferred_freq.lower() == 'h'
        assert df[variable.name].mean() == pytest.approx(expected_mean, abs=1e-5)

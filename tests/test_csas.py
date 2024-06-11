import matplotlib.pyplot as plt
import pytest

from metloom.pointdata import CSASMet
from metloom.pointdata.csas import InvalidDateRange
from metloom.variables import CSASVariables
from datetime import datetime
from pathlib import Path

def test_sbb():
    start = datetime(2009, 1, 1)
    end = datetime(2009, 5, 1)
    var = CSASVariables.SNOWDEPTH
    pnt = CSASMet('SBSP')
    df = pnt.get_daily_data(start, end, [var])
    fig, ax = plt.subplots(1)
    ax.plot(df.index.get_level_values('datetime'), df[var.name])
    plt.show()


class TestCSASMet:
    @pytest.mark.parametrize('year, doy, hour, expected', [
        (2024, 92, 1400, datetime(2024, 4, 1, 14)),
        # Check doy 1 is jan 1
        (2024, 1, 0, datetime(2024, 1, 1))

    ])
    def test_parse_datetime(self, year, doy, hour, expected):
        dt = CSASMet._parse_datetime({'Year':year, 'DOY': doy, 'Hour':hour})
        assert dt == expected

    @pytest.mark.parametrize("station_id, start, end, expected",[
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

    @pytest.mark.parametrize("station_id, start, end",[
        # Test pre 2003
        ('SBSP', datetime(2002, 1, 1),
         datetime(2003, 1, 1)),
        # Test post 2023 exception
        ('SBSP', datetime(2010, 1, 1),
         datetime(2024, 1, 1))
    ])
    def test_file_urls_exception(self, station_id, start, end):
        """
        With files there are hard timeframes, test ane exception is raised when
        this is the case
        """
        pnt = CSASMet(station_id)
        pnt._verify_station()
        with pytest.raises(InvalidDateRange):
            urls = pnt._file_urls(station_id, start, end)



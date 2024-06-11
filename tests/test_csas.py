import matplotlib.pyplot as plt
import pytest

from metloom.pointdata import CSASMet
from metloom.pointdata.csas import InvalidDateRange, CSASStationInfo
from metloom.variables import CSASVariables
from datetime import datetime, timedelta
from pathlib import Path
import requests

def test_sbb():
    start = datetime(2009, 1, 1)
    end = datetime(2009, 5, 1)
    var = CSASVariables.SNOWDEPTH
    fig, (ax, ax2, ax3) = plt.subplots(3)
    # for station_id in ['SASP', 'SBSP']:
    #     pnt = CSASMet(station_id)
    #     df = pnt.get_daily_data(start, end, [var])
    #     if df is not None:
    #         ax.plot(df.index.get_level_values('datetime'), df[var.name], label=station_id)
    # ax.legend()

    pnt = CSASMet('SBSG')
    var = CSASVariables.STREAMFLOW_CFS
    df = pnt.get_daily_data(start, end, [var])
    ax2.plot(df.index.get_level_values('datetime'), df[var.name], label='Streamflow')
    ax2.legend()

    pnt = CSASMet('PTSP')
    var = CSASVariables.RH
    df = pnt.get_daily_data(start, end, [var])
    ax3.plot(df.index.get_level_values('datetime'), df[var.name], label='Putney RH')
    ax.legend()

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

import matplotlib.pyplot as plt
import pytest

from metloom.pointdata import CSASMet
from metloom.variables import CSASVariables
from datetime import datetime


def test_sbb():
    start = datetime(2020, 1, 1)
    end = datetime(2020, 5, 1)
    var = CSASVariables.SNOWDEPTH
    pnt = CSASMet('SBSP')
    df = pnt.get_daily_data(start, end, [var])
    fig, ax = plt.subplots(1)
    ax.plot(df.index, df[var.name])
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

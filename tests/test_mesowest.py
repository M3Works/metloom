from metloom.pointdata.mesowest import *
import pytest

from tests.test_point_data import BasePointDataTest, side_effect_error
import geopandas as gpd
from datetime import datetime

class TestMesowestStation(BasePointDataTest):
    def kmyl_meta_reponse(self):
        return {'STATION': [{'STATUS': 'ACTIVE',
                             'MNET_ID': '1',
                             'PERIOD_OF_RECORD': {'start': '1997-08-29T00:00:00Z', 'end': '2021-12-14T21:15:00Z'},
                             'ELEVATION': '5020',
                             'NAME': 'McCall Airport',
                             'STID': 'KMYL',
                             'ELEV_DEM': '5006.6',
                             'LONGITUDE': '-116.09978',
                             'STATE': 'ID',
                             'RESTRICTED': False,
                             'LATITUDE': '44.89425',
                             'TIMEZONE': 'America/Boise', 'ID': '283'}],
                'SUMMARY': {'NUMBER_OF_OBJECTS': 1,
                            'RESPONSE_CODE': 1,
                            'RESPONSE_MESSAGE': 'OK',
                            'METADATA_RESPONSE_TIME': '3.59511375427 ms'}}

    @pytest.fixture()
    def station(self):
        return MesowestPointData("KMYL", "Mccall Airport")

    def test_get_metadata(self, station):
        # with patch("metloom.pointdata.cdec.requests") as mock_requests:
        expected = gpd.points_from_xy([-116.09978], [44.89425], z=[5020.0])[0]
        assert station.metadata == expected

    def test_get_hourly_data(self, station):
        response = station.get_hourly_data(
            datetime(2021, 5, 15),
            datetime(2021, 5, 18),
            [MesowestVariables.TEMP],
        )
        # pd.testing.assert_frame_equal(response, tny_daily_expected)

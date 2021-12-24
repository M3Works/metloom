from metloom.pointdata.mesowest import *
import geopandas as gpd
from datetime import datetime
from collections import OrderedDict
import pytest
from tests.test_point_data import BasePointDataTest
from unittest.mock import MagicMock, patch
from os import path


class TestMesowestPointData(BasePointDataTest):
    @pytest.fixture(scope="class")
    def data_dir(self):
        this_dir = path.dirname(__file__)
        return path.join(this_dir, "data")

    @pytest.fixture(scope="class")
    def shape_obj(self, data_dir):
        fp = path.join(data_dir, "triangle.shp")
        return gpd.read_file(fp)

    def _meta_response(self, *args, **kwargs):
        """
        Mccall airport station metadata return
        """
        mock = MagicMock()

        response = {}
        params = kwargs["params"]

        if 'stid' in params.keys():
            stid = params['stid']
            if stid == 'KMYL':
                response = {'STATION': [{'ELEVATION': '5020',
                                         'NAME': 'McCall Airport',
                                         'STID': 'KMYL',
                                         'ELEV_DEM': '5006.6',
                                         'LONGITUDE': '-116.09978',
                                         'LATITUDE': '44.89425',
                                         'TIMEZONE': 'America/Boise'}]}

            elif stid == 'INTRI':
                response = {'STATION': [{'ELEVATION': '9409',
                                         'NAME': 'IN TRIANGLE',
                                         'STID': 'INTRI',
                                         'LONGITUDE': '-119.5',
                                         'LATITUDE': '38.0',
                                         'TIMEZONE': 'America/Los_Angeles',
                                         }]}

            elif stid == 'OUTTRI':
                response = {'STATION': [
                    {'ELEVATION': '7201',
                     'NAME': 'OUT TRIANGLE W/IN BOUNDS',
                     'STID': 'OUTTRI',
                     'TIMEZONE': 'America/Los_Angeles',
                     'LONGITUDE': '-119.7',
                     'LATITUDE': '38.0',
                     }]}

        elif 'bbox' in params.keys():
            response = {'STATION': [{'ELEVATION': '9409',
                                     'NAME': 'IN TRIANGLE',
                                     'STID': 'INTRI',
                                     'LONGITUDE': '-119.5',
                                     'LATITUDE': '38.0',
                                     'TIMEZONE': 'America/Los_Angeles',
                                     },
                                    {'ELEVATION': '7201',
                                     'NAME': 'OUT TRIANGLE W/IN BOUNDS',
                                     'STID': 'OUTTRI',
                                     'TIMEZONE': 'America/Los_Angeles',
                                     'LONGITUDE': '-119.7',
                                     'LATITUDE': '38.0',
                                     }
                                    ]}
        else:
            raise ValueError('Invalid test STID provided')

        mock.json.return_value = response
        return mock

    @staticmethod
    def ts_response(var, values, delta, units: str):
        """
        Build a timeseries return using only values and assuming a date start point. We do this to avoid
        a lot of lists of repeat dates.
        """

        # Build a datetime list to match the values
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        dt = [(pd.to_datetime('2021-01-01T00:00') + delta * i).strftime(fmt) for i in range(len(values))]

        # Populate the response
        response = {'UNITS': {var: units},
                    'STATION': [{
                        'NAME': 'McCall Airport',
                        'STID': 'KMYL',
                        'SENSOR_VARIABLES': {
                            'date_time': {'date_time': {},
                                          var: {f'{var}_set_1': {
                                              'position': '6.56'}}}},
                        'ELEVATION': '5006.6',
                        'LONGITUDE': '-119.5',
                        'LATITUDE': '44.89425',
                        'OBSERVATIONS': {
                            'date_time': dt,
                            f'{var}_set_1': values},
                        'TIMEZONE': 'UTC'}]}
        return response

    @pytest.fixture()
    def bbox_response(self):
        """
        Metadata response from mesowest using a bbox
        """
        mock = MagicMock()
        response = {'STATION': [{'ELEVATION': '9409',
                                 'NAME': 'IN TRIANGLE',
                                 'STID': 'INTRI',
                                 'LONGITUDE': '-119.5',
                                 'LATITUDE': '38.0',
                                 'TIMEZONE': 'America/Los_Angeles',
                                 },
                                {'ELEVATION': '7201',
                                 'NAME': 'OUT TRIANGLE W/IN BOUNDS',
                                 'STID': 'OUTTRI',
                                 'TIMEZONE': 'America/Los_Angeles',
                                 'LONGITUDE': '-119.7',
                                 'LATITUDE': '38.0',
                                 }
                                ]}
        mock.json.return_value = response
        return mock

    @pytest.fixture()
    def sub_hourly_response(self, var, values, units):
        """
        Build a sub hourly response for time series data
        """
        mock = MagicMock()

        delta = pd.to_timedelta(30, 'minutes')
        response = self.ts_response(var.code, values, delta, units)
        mock.json.return_value = response
        return mock

    @pytest.fixture()
    def sub_daily_response(self, var, values, units):
        """
        Build a sub daily response for time series data
        """
        delta = pd.to_timedelta(12, 'hours')
        return self.ts_response(var.code, values, delta, units)

    @pytest.fixture()
    def station(self):
        return MesowestPointData("KMYL", "Mccall Airport")

    @pytest.mark.parametrize('stid, long, lat, elev', [
        ("KMYL", -116.09978, 44.89425, 5020.0),
    ])
    def test_get_metadata(self, stid, long, lat, elev):
        result = False
        with patch("metloom.pointdata.mesowest.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self._meta_response
            expected = gpd.points_from_xy([long], [lat], z=[elev])[0]
            station = MesowestPointData(stid, 'test')
            result = station.metadata == expected
        assert result

    @pytest.mark.parametrize('var, values, units, expected_values, expected_dates', [
        (MesowestVariables.TEMP, [14.0, 16.0, 16.0, 18.0], 'Celsius', [15.0, 17.0],
         ['2021-01-1T00:00:00+00:00', '2021-01-1T01:00:00+00:00']),
    ])
    def test_get_hourly_data(self, station, sub_hourly_response, var, values, units, expected_values, expected_dates):
        # Patch in the made up response
        with patch("metloom.pointdata.mesowest.requests.get", return_value=sub_hourly_response):
            df = station.get_hourly_data(
                datetime(2021, 1, 1, 0),
                datetime(2021, 1, 1, 2),
                [var],
            )

        dt = [pd.to_datetime(d) for d in expected_dates]
        data = sub_hourly_response.json()['STATION'][0]
        shp_point = gpd.points_from_xy(
            [float(data["LONGITUDE"])],
            [float(data["LATITUDE"])],
            z=[float(data["ELEVATION"])],
        )[0]

        expected = gpd.GeoDataFrame.from_dict(
            OrderedDict({
                'geometry': [shp_point] * len(dt),
                'datetime': dt,
                'measurementDate': dt,
                var.name: expected_values,
                f'{var.name}_units': [units] * len(dt),
            }),
            geometry=[shp_point] * len(dt))
        expected.set_index('datetime', inplace=True)
        pd.testing.assert_frame_equal(df, expected)

    @pytest.mark.parametrize('within_geometry, expected_sid', [
        (False, ['INTRI', 'OUTTRI']),  # Use just bounds of the shapefile
        (True, ['INTRI']),  # Filter to within the shapefile
    ])
    def test_points_from_geometry(self, shape_obj, within_geometry, expected_sid):

        with patch("metloom.pointdata.mesowest.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self._meta_response
            pnts = MesowestPointData.points_from_geometry(shape_obj, [MesowestVariables.TEMP],
                                                          within_geometry=within_geometry)

        df = pnts.to_dataframe()
        assert df['id'].values == pytest.approx(expected_sid)

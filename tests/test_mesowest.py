from metloom.pointdata.mesowest import *
import geopandas as gpd
from datetime import datetime
from collections import OrderedDict
import pytest
from tests.test_point_data import BasePointDataTest
from unittest.mock import MagicMock, patch
from os import path


class TestMesowestStation(BasePointDataTest):
    @pytest.fixture(scope="class")
    def data_dir(self):
        this_dir = path.dirname(__file__)
        return path.join(this_dir, "data")

    @pytest.fixture(scope="class")
    def shape_obj(self, data_dir):
        fp = path.join(data_dir, "testing.shp")
        return gpd.read_file(fp)


    @pytest.fixture()
    def meta_reponse(self):
        """
        Mccall airport station metadata return
        """
        return {'STATION': [{'ELEVATION': '5020',
                             'NAME': 'McCall Airport',
                             'STID': 'KMYL',
                             'ELEV_DEM': '5006.6',
                             'LONGITUDE': '-116.09978',
                             'LATITUDE': '44.89425',
                             'TIMEZONE': 'America/Boise'}]}

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
                        'LONGITUDE': '-116.09978',
                        'LATITUDE': '44.89425',
                        'OBSERVATIONS': {
                            'date_time': dt,
                            f'{var}_set_1': values},
                        'TIMEZONE': 'UTC'}]}
        return response

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

    def test_get_metadata(self, station):
        # with patch("metloom.pointdata.cdec.requests") as mock_requests:
        expected = gpd.points_from_xy([-116.09978], [44.89425], z=[5020.0])[0]
        assert station.metadata == expected

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

    def test_points_from_geometry(self, station, shape_obj):
        MesowestPointData.points_from_geometry(shape_obj, [MesowestVariables.TEMP])

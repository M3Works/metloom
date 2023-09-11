import json
import os
from collections import OrderedDict
from datetime import datetime
from os import path
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import geopandas as gpd
from metloom.pointdata.geosphere_austria import GeoSphere
from metloom.variables import GeoSphereVariables
from tests.test_point_data import BasePointDataTest


class TestGeospherePointData(BasePointDataTest):
    DATA_DIR = Path(__file__).parent.joinpath("data")

    @pytest.fixture(scope="class")
    def shape_obj(self):
        fp = self.DATA_DIR.joinpath("austria_box.shp")
        return gpd.read_file(fp)

    def _meta_response(self, *args, **kwargs):
        """
        Mccall airport station metadata return
        """
        mock = MagicMock()

        url = args[0]

        if 'metadata' in url:
            with open(
                self.DATA_DIR.joinpath("geosphere_mocks/meta_mock.json")
            ) as fp:
                response = json.load(fp)

        else:
            raise ValueError('Invalid test url provided')

        mock.json.return_value = response
        return mock

    def mock_station_response(self, *args, **kwargs):
        """
        Build a sub hourly response for time series data
        """
        if "metadata" in args[0]:
            return self._meta_response(*args)
        else:
            obj = {
                'media_type': 'application/json', 'type': 'FeatureCollection',
                'version': 'v1',
                'timestamps': [
                    '2021-01-01T00:00+00:00', '2021-01-01T00:10+00:00',
                    '2021-01-01T00:20+00:00', '2021-01-01T00:30+00:00',
                    '2021-01-01T00:40+00:00', '2021-01-01T00:50+00:00',
                    '2021-01-01T01:00+00:00', '2021-01-01T01:10+00:00',
                    '2021-01-01T01:20+00:00', '2021-01-01T01:30+00:00',
                    '2021-01-01T01:40+00:00', '2021-01-01T01:50+00:00',
                    '2021-01-01T02:00+00:00'], 'features': [
                {'type': 'Feature', 'geometry': {'type': 'Point',
                                                 'coordinates': [16.35638888888889,
                                                                 48.24861111111111]},
                 'properties': {'parameters': {
                     'TL': {'name': 'Lufttemperatur', 'unit': '°C',
                            'data': [1.0, 2.0, 1.0, 3.0, 2.9, 5.0, 0.0,
                                     0.3, 0.2, 0.5, 1.1, 1.1, 1.1]}},
                                'station': '11035'}}]}
            mock_obj = MagicMock()
            mock_obj.json.return_value = obj
            return mock_obj

    @pytest.fixture()
    def station(self):
        return GeoSphere("11035", "Tester")

    @pytest.mark.parametrize('stid, long, lat, elev', [
        ("11035", 16.35638888888889, 48.24861111111111, 649.60632),
    ])
    def test_get_metadata(self, station, stid, long, lat, elev):
        with patch("metloom.pointdata.geosphere_austria.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self._meta_response
            expected = gpd.points_from_xy([long], [lat], z=[elev])[0]
            result = station.metadata == expected
            assert result

    @pytest.mark.parametrize('var, expected_values, expected_dates', [
        (GeoSphereVariables.TEMP, [2.4833333333333334, 0.5333333333333333, 1.1],
         ['2021-01-1T00:00:00+00:00', '2021-01-1T01:00:00+00:00', '2021-01-1T02:00:00+00:00']),
    ])
    def test_get_hourly_data(
        self, station, var, expected_values, expected_dates
    ):
        # Patch in the made up response
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            df = station.get_hourly_data(
                datetime(2021, 1, 1, 0),
                datetime(2021, 1, 1, 2),
                [var],
            )

        dt = [pd.to_datetime(d) for d in expected_dates]
        shp_point = gpd.points_from_xy(
            [16.35638888888889],
            [48.24861111111111],
            z=[649.60632],
        )[0]

        expected = gpd.GeoDataFrame.from_dict(
            OrderedDict({
                'site': [station.id] * len(dt),
                var.name: expected_values,
                'datetime': dt,
                f'{var.name}_units': ['°C'] * len(dt),
                'geometry': [shp_point] * len(dt),
                'datasource': ["GEOSPHERE"] * len(dt),
            }),
            geometry=[shp_point] * len(dt))
        expected.set_index(keys=["datetime", "site"], inplace=True)
        pd.testing.assert_frame_equal(df, expected)

    @pytest.mark.parametrize('w_geom, expected_sid, buffer', [
        (False, ['11266', '11125', '11121', '11320', '11123'], 0.15),  # Use just bounds of the shapefile
        (True, ['11266'], 0.0),  # Filter to within the shapefile
    ])
    def test_points_from_geometry(self, shape_obj, w_geom, expected_sid, buffer):
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            pnts = GeoSphere.points_from_geometry(
                shape_obj,
                [GeoSphereVariables.TEMP],
                within_geometry=w_geom,
                buffer=buffer
            )

        df = pnts.to_dataframe()
        assert df['id'].values == pytest.approx(expected_sid)

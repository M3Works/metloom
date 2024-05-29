import json
from collections import OrderedDict
from datetime import datetime, date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import geopandas as gpd
from metloom.pointdata.geosphere_austria import (
    GeoSphereCurrentPointData, GeoSphereHistPointData
)
from metloom.variables import (
    GeoSphereCurrentVariables, GeoSphereHistVariables
)
from tests.test_point_data import BasePointDataTest


TODAY = date.today()


class TestGeoSphereCurrentPointData(BasePointDataTest):
    DATA_DIR = Path(__file__).parent.joinpath("data")
    EXPECTED_DATETIMES = pd.date_range(
        TODAY.isoformat(), periods=3, freq='H', tz='UTC'
    )

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
            t_values = pd.date_range(
                TODAY.isoformat(), periods=13, freq='10 min', tz='UTC'
            )
            obj = {
                'media_type': 'application/json', 'type': 'FeatureCollection',
                'version': 'v1',
                'timestamps': [
                    t.isoformat() for t in t_values
                ], 'features': [
                    {'type': 'Feature', 'geometry': {'type': 'Point',
                                                     'coordinates': [
                                                         16.35638888888889,
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
        return GeoSphereCurrentPointData("11035", "Tester")

    @pytest.mark.parametrize(
        "dt, expected", [
            (datetime(2024, 1, 24), datetime(2023, 10, 24)),
            (datetime(2023, 6, 24), datetime(2023, 3, 24)),
            (datetime(2023, 3, 1), datetime(2022, 12, 1)),
        ]
    )
    def test_back_3_months(self, dt, expected, station):
        result = station._back_3_months(dt)
        assert expected == result

    @pytest.mark.parametrize('stid, long, lat, elev', [
        ("11035", 16.35638888888889, 48.24861111111111, 649.60632),
    ])
    def test_get_metadata(self, station, stid, long, lat, elev):
        with patch(
            "metloom.pointdata.geosphere_austria.requests"
        ) as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self._meta_response
            expected = gpd.points_from_xy([long], [lat], z=[elev])[0]
            result = station.metadata == expected
            assert result

    def test_get_hourly_data_fails(
        self, station
    ):
        """
        Test that we fail with dates greater than 3 months old
        """
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            with pytest.raises(ValueError):
                station.get_hourly_data(
                    datetime(2021, 1, 1, 0),
                    datetime(2021, 1, 1, 2),
                    [GeoSphereCurrentVariables.TEMP],
                )

    @pytest.mark.parametrize('var, expected_values, expected_dates', [
        (
            GeoSphereCurrentVariables.TEMP,
            [2.4833333333333334, 0.5333333333333333, 1.1],
            [t.isoformat() for t in EXPECTED_DATETIMES]
        ),
    ])
    def test_get_hourly_data(
        self, station, var, expected_values, expected_dates
    ):
        # Patch in the made up response
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            df = station.get_hourly_data(
                datetime(TODAY.year, TODAY.month, TODAY.day, 0),
                datetime(TODAY.year, TODAY.month, TODAY.day, 2),
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
        (False, ['11266', '11125', '11121', '11320', '11123'], 0.15),
        # Use just bounds of the shapefile
        (True, ['11266'], 0.0),  # Filter to within the shapefile
    ])
    def test_points_from_geometry(self, shape_obj, w_geom, expected_sid,
                                  buffer):
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            pnts = GeoSphereCurrentPointData.points_from_geometry(
                shape_obj,
                [GeoSphereCurrentVariables.TEMP],
                within_geometry=w_geom,
                buffer=buffer
            )

        df = pnts.to_dataframe()
        assert df['id'].values == pytest.approx(expected_sid)


class TestGeoSphereHistPointData(BasePointDataTest):
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
                self.DATA_DIR.joinpath("geosphere_mocks/klima_mock.json")
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
                "media_type": "application/json", "type": "FeatureCollection",
                "version": "v1", "timestamps": [
                    "2023-01-20T00:00+00:00", "2023-01-21T00:00+00:00",
                    "2023-01-22T00:00+00:00", "2023-01-23T00:00+00:00",
                    "2023-01-24T00:00+00:00", "2023-01-25T00:00+00:00"
                ],
                "features": [
                    {"type": "Feature", "geometry": {
                        "type": "Point", "coordinates": [11.700833, 47.5075]
                    }, "properties": {
                        "parameters": {
                            "schnee": {
                                "name": "Gesamtschneehöhe zum 07 Uhr"
                                        "MEZ Termin",
                                "unit": "cm",
                                "data": [3.0, 18.0, 22.0, 18.0, 18.0, 14.0]
                            }
                        }, "station": "8807"}}
                ]
            }
            mock_obj = MagicMock()
            mock_obj.json.return_value = obj
            return mock_obj

    @pytest.fixture()
    def station(self):
        return GeoSphereHistPointData("8807", "Tester2")

    @pytest.mark.parametrize('stid, long, lat, elev', [
        ("8807", 11.700833, 47.5075, 3074.14708),
    ])
    def test_get_metadata(self, station, stid, long, lat, elev):
        with patch(
            "metloom.pointdata.geosphere_austria.requests"
        ) as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self._meta_response
            expected = gpd.points_from_xy([long], [lat], z=[elev])[0]
            result = station.metadata == expected
            assert result

    @pytest.mark.parametrize('var, expected_values, expected_dates', [
        (
            GeoSphereHistVariables.SNOWDEPTH,
            [3.0, 18.0, 22.0, 18.0, 18.0, 14.0],
            ['2023-01-20T00:00:00+00:00', '2023-01-21T00:00:00+00:00',
             '2023-01-22T00:00:00+00:00', '2023-01-23T00:00:00+00:00',
             '2023-01-24T00:00:00+00:00', '2023-01-25T00:00:00+00:00']
        ),
    ])
    def test_get_daily_data(
        self, station, var, expected_values, expected_dates
    ):
        # Patch in the made up response
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            df = station.get_daily_data(
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
                [var],
            )

        dt = [pd.to_datetime(d) for d in expected_dates]
        shp_point = gpd.points_from_xy(
            [11.700833],
            [47.5075],
            z=[3074.14708],
        )[0]

        expected = gpd.GeoDataFrame.from_dict(
            OrderedDict({
                'site': [station.id] * len(dt),
                var.name: expected_values,
                'datetime': dt,
                f'{var.name}_units': ['cm'] * len(dt),
                'geometry': [shp_point] * len(dt),
                'datasource': ["GEOSPHERE"] * len(dt),
            }),
            geometry=[shp_point] * len(dt))
        expected.set_index(keys=["datetime", "site"], inplace=True)
        pd.testing.assert_frame_equal(df, expected)

    @pytest.mark.parametrize('w_geom, expected_sid, buffer', [
        (False, [
            '8807', '11900', '11901', '11902', '11903', '11910', '8800',
            '8805', '8806', '42', '151'
        ], 0.05),
        # Use just bounds of the shapefile
        (True, [
            '8807', '11900', '11901', '11903', '11910', '8800', '8805', '8806',
            '42', '151'
        ], 0.0),  # Filter to within the shapefile
    ])
    def test_points_from_geometry(self, shape_obj, w_geom, expected_sid,
                                  buffer):
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   side_effect=self.mock_station_response):
            pnts = GeoSphereHistPointData.points_from_geometry(
                shape_obj,
                [GeoSphereCurrentVariables.TEMP],
                within_geometry=w_geom,
                buffer=buffer
            )

        result = pnts.to_dataframe()['id'].values
        assert result == pytest.approx(expected_sid)

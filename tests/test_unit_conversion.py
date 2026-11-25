"""
Tests for the optional pint unit conversion feature across every client.

Approach (per the agreed plan): real API payloads are captured once by
``scratch/capture_unit_fixtures.py`` and committed as pickles under
``tests/data/unit_fixtures``. Each test loads a pickle, mocks the client's
network boundary to return it, and asserts two things:

  1. Units are correctly *inferred* from the returned payload (the
     ``{variable.name}_units`` column matches the source string).
  2. Passing ``desired_units`` converts the value column (as magnitudes) and
     updates the units column to the requested unit.

The raw payloads are fed through the real parsing code so unit inference stays
under test; only the network layer is mocked.
"""
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pytest
import shapely

from metloom.unit_conversions import (
    UREG, convert_series, normalize_unit,
)

FIXTURE_DIR = Path(__file__).parent.joinpath("data/unit_fixtures")


def load_fixture(name):
    with open(FIXTURE_DIR.joinpath(f"{name}.pkl"), "rb") as fp:
        return pickle.load(fp)


# ---------------------------------------------------------------------------
# unit_conversions module
# ---------------------------------------------------------------------------
class TestUnitConversionsModule:
    @pytest.mark.parametrize("raw, expected", [
        ("DEG F", "degF"),
        ("deg C", "degC"),
        ("INCHES", "inch"),
        ("CFS", "foot**3/second"),
        ("ac-ft", "acre_foot"),
        ("%", "percent"),
        ("wmoUnit:degC", "degC"),
        ("Watts/meter^2", "watt/meter**2"),
        ("Celsius", "degC"),
        ("Millimeters", "millimeter"),
        # already-valid / unknown strings pass through unchanged
        ("degC", "degC"),
        ("some_unknown_unit", "some_unknown_unit"),
    ])
    def test_normalize_unit(self, raw, expected):
        assert normalize_unit(raw) == expected

    def test_normalize_none(self):
        assert normalize_unit(None) is None

    def test_convert_offset_units(self):
        # 32 degF -> 0 degC, 212 degF -> 100 degC, NaN preserved
        result = convert_series(np.array([32.0, 212.0, np.nan]), "DEG F", "degC")
        assert result[0] == pytest.approx(0.0, abs=1e-6)
        assert result[1] == pytest.approx(100.0, abs=1e-6)
        assert np.isnan(result[2])

    def test_convert_ratio_units(self):
        result = convert_series(np.array([1.0, 2.0]), "INCHES", "mm")
        np.testing.assert_allclose(result, [25.4, 50.8])

    def test_incompatible_dimensions_returns_unchanged(self):
        # meters -> degC is nonsense; should warn and return values unchanged
        values = np.array([1.0, 2.0])
        result = convert_series(values, "meters", "degC")
        np.testing.assert_array_equal(result, values)

    def test_unknown_unit_returns_unchanged(self):
        values = np.array([1.0, 2.0])
        result = convert_series(values, "flibbers", "mm")
        np.testing.assert_array_equal(result, values)

    def test_shared_registry(self):
        # everything must build quantities on the same registry
        assert convert_series.__globals__["UREG"] is UREG


# ---------------------------------------------------------------------------
# CDEC (units inferred from the API 'units' field: INCHES / DEG F)
# ---------------------------------------------------------------------------
class TestCDECUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import CDECPointData
        pt = shapely.geometry.Point(-119.449875, 37.837581, 8150.0)
        return CDECPointData("TNY", "Tenaya Lake", metadata=pt)

    @pytest.fixture
    def mock_requests(self):
        data = load_fixture("cdec")

        def side_effect(url, **kwargs):
            mock = MagicMock()
            params = kwargs.get("params") or {}
            if params.get("SensorNums") == "3":
                mock.json.return_value = data["swe"]
            elif params.get("SensorNums") == "30":
                mock.json.return_value = data["temp"]
            else:
                mock.json.return_value = []
            return mock

        with patch("metloom.pointdata.cdec.requests") as mock_requests:
            mock_requests.get.side_effect = side_effect
            yield mock_requests

    def _get(self, station, desired_units=None):
        from metloom.variables import CdecStationVariables
        return station.get_daily_data(
            datetime(2021, 5, 15), datetime(2021, 5, 18),
            [CdecStationVariables.SWE, CdecStationVariables.TEMPAVG],
            desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_requests):
        df = self._get(station)
        assert df["SWE_units"].dropna().unique().tolist() == ["INCHES"]
        assert df["AVG AIR TEMP_units"].dropna().unique().tolist() == ["DEG F"]

    def test_conversion(self, station, mock_requests):
        raw = self._get(station)
        conv = self._get(station, desired_units={"SWE": "mm",
                                                 "AVG AIR TEMP": "degC"})
        # units columns updated
        assert conv["SWE_units"].unique().tolist() == ["mm"]
        assert conv["AVG AIR TEMP_units"].unique().tolist() == ["degC"]
        # values converted as magnitudes
        raw_swe = raw["SWE"].to_numpy(dtype=float)
        np.testing.assert_allclose(
            conv["SWE"].to_numpy(dtype=float), raw_swe * 25.4, equal_nan=True
        )
        raw_t = raw["AVG AIR TEMP"].to_numpy(dtype=float)
        np.testing.assert_allclose(
            conv["AVG AIR TEMP"].to_numpy(dtype=float), (raw_t - 32.0) * 5.0 / 9.0,
            equal_nan=True, atol=1e-6
        )
        # not pint Quantities
        assert conv["SWE"].to_numpy(dtype=float).dtype == float


# ---------------------------------------------------------------------------
# USGS (units inferred from unitCode: ft3/s)
# ---------------------------------------------------------------------------
class TestUSGSUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import USGSPointData
        pt = shapely.geometry.Point(-106.54, 37.35, 9866.6)
        st = USGSPointData("08245000", "Conejos", metadata=pt)
        return st

    @pytest.fixture
    def mock_response(self):
        payload = load_fixture("usgs_daily")
        with patch(
            "metloom.pointdata.usgs.USGSPointData._get_url_response"
        ) as mock_resp, patch(
            "metloom.pointdata.usgs.USGSPointData._get_tzinfo"
        ) as mock_tz:
            mock_resp.return_value = payload
            mock_tz.return_value = timezone(timedelta(hours=-7))
            yield mock_resp

    def _get(self, station, desired_units=None):
        from metloom.variables import USGSVariables
        return station.get_daily_data(
            datetime(2020, 7, 1), datetime(2020, 7, 2),
            [USGSVariables.DISCHARGE], desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_response):
        df = self._get(station)
        assert df["DISCHARGE_units"].unique().tolist() == ["ft3/s"]

    def test_conversion(self, station, mock_response):
        raw = self._get(station)
        conv = self._get(station, desired_units="m^3/s")
        assert conv["DISCHARGE_units"].unique().tolist() == ["m^3/s"]
        expected = raw["DISCHARGE"].to_numpy(dtype=float) * 0.028316846592
        np.testing.assert_allclose(
            conv["DISCHARGE"].to_numpy(dtype=float), expected, rtol=1e-6
        )


# ---------------------------------------------------------------------------
# SNOTEL (units inferred from element storedUnitCd: in / degF)
# ---------------------------------------------------------------------------
class TestSnotelUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import SnotelPointData
        pt = shapely.geometry.Point(-107.67552, 37.9339, 9800.0)
        return SnotelPointData("538:CO:SNTL", "eh", metadata=pt)

    @pytest.fixture
    def mocks(self):
        fx = load_fixture("snotel_daily")

        def data_client_factory(*args, **kwargs):
            client = MagicMock()
            client.DURATION = "DAILY"

            def get_data(element_cd=None, **kw):
                return fx["data"].get(element_cd, [])
            client.get_data.side_effect = get_data
            return client

        with patch(
            "metloom.pointdata.snotel.DailySnotelDataClient",
            side_effect=data_client_factory,
        ), patch(
            "metloom.pointdata.snotel.SnotelPointData._get_all_elements",
            return_value=fx["elements"],
        ), patch(
            "metloom.pointdata.snotel.SnotelPointData._get_tzinfo",
            return_value=timezone(timedelta(hours=fx["tz_hours"])),
        ):
            yield

    def _get(self, station, desired_units=None):
        from metloom.variables import SnotelVariables
        return station.get_daily_data(
            datetime(2020, 3, 20), datetime(2020, 3, 22),
            [SnotelVariables.SWE, SnotelVariables.TEMP],
            desired_units=desired_units,
        )

    def test_infer_units(self, station, mocks):
        df = self._get(station)
        assert df["SWE_units"].unique().tolist() == ["in"]
        assert df["AIR TEMP_units"].unique().tolist() == ["degF"]

    def test_conversion(self, station, mocks):
        raw = self._get(station)
        conv = self._get(station, desired_units={"SWE": "mm", "AIR TEMP": "degC"})
        assert conv["SWE_units"].unique().tolist() == ["mm"]
        assert conv["AIR TEMP_units"].unique().tolist() == ["degC"]
        np.testing.assert_allclose(
            conv["SWE"].to_numpy(dtype=float),
            raw["SWE"].to_numpy(dtype=float) * 25.4,
        )
        np.testing.assert_allclose(
            conv["AIR TEMP"].to_numpy(dtype=float),
            (raw["AIR TEMP"].to_numpy(dtype=float) - 32.0) * 5.0 / 9.0,
            atol=1e-6,
        )


# ---------------------------------------------------------------------------
# Mesowest (units inferred from the 'UNITS' map: Celsius / Millimeters)
# ---------------------------------------------------------------------------
class TestMesowestUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import MesowestPointData
        with patch(
            "metloom.pointdata.mesowest.MesowestPointData.token",
            new_callable=PropertyMock, return_value="faketoken",
        ):
            pt = shapely.geometry.Point(-119.5, 38.0, 7000)
            yield MesowestPointData("INMTP", "test", metadata=pt)

    @pytest.fixture
    def mock_requests(self):
        payload = load_fixture("mesowest")
        resp = MagicMock()
        resp.json.return_value = payload
        with patch("metloom.pointdata.mesowest.requests.get",
                   return_value=resp) as mock_get:
            yield mock_get

    def _get(self, station, desired_units=None):
        from metloom.variables import MesowestVariables
        return station.get_hourly_data(
            datetime(2021, 3, 16), datetime(2021, 3, 16, 2),
            [MesowestVariables.TEMP, MesowestVariables.SNOWDEPTH],
            desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_requests):
        df = self._get(station)
        assert df["AIR TEMP_units"].unique().tolist() == ["Celsius"]
        assert df["SNOWDEPTH_units"].unique().tolist() == ["Millimeters"]

    def test_conversion(self, station, mock_requests):
        raw = self._get(station)
        conv = self._get(station, desired_units={"AIR TEMP": "degF",
                                                 "SNOWDEPTH": "m"})
        assert conv["AIR TEMP_units"].unique().tolist() == ["degF"]
        assert conv["SNOWDEPTH_units"].unique().tolist() == ["m"]
        np.testing.assert_allclose(
            conv["AIR TEMP"].to_numpy(dtype=float),
            raw["AIR TEMP"].to_numpy(dtype=float) * 9.0 / 5.0 + 32.0, atol=1e-6,
        )
        np.testing.assert_allclose(
            conv["SNOWDEPTH"].to_numpy(dtype=float),
            raw["SNOWDEPTH"].to_numpy(dtype=float) / 1000.0,
        )


# ---------------------------------------------------------------------------
# GeoSphere Austria (units inferred from parameter 'unit': cm)
# ---------------------------------------------------------------------------
class TestGeoSphereUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import GeoSphereHistPointData
        pt = shapely.geometry.Point(11.700833, 47.5075, 3074.14708)
        return GeoSphereHistPointData("8807", "Tester", metadata=pt)

    @pytest.fixture
    def mock_requests(self):
        payload = load_fixture("geosphere_hist")
        resp = MagicMock()
        resp.json.return_value = payload
        with patch("metloom.pointdata.geosphere_austria.requests.get",
                   return_value=resp) as mock_get:
            yield mock_get

    def _get(self, station, desired_units=None):
        from metloom.variables import GeoSphereHistVariables
        return station.get_daily_data(
            datetime(2023, 1, 20), datetime(2023, 1, 25),
            [GeoSphereHistVariables.SNOWDEPTH], desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_requests):
        df = self._get(station)
        assert df["Snowdepth_units"].unique().tolist() == ["cm"]

    def test_conversion(self, station, mock_requests):
        raw = self._get(station)
        conv = self._get(station, desired_units="m")
        assert conv["Snowdepth_units"].unique().tolist() == ["m"]
        np.testing.assert_allclose(
            conv["Snowdepth"].to_numpy(dtype=float),
            raw["Snowdepth"].to_numpy(dtype=float) / 100.0,
        )


# ---------------------------------------------------------------------------
# CUES (units parsed out of the returned CSV column header: Watts/meter^2)
# ---------------------------------------------------------------------------
class TestCuesUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import CuesLevel1
        return CuesLevel1(None, None)

    @pytest.fixture
    def mock_requests(self):
        text = load_fixture("cues_daily")
        resp = MagicMock()
        resp.content = text.encode()
        with patch("metloom.pointdata.cues.requests") as mock_requests:
            mock_requests.post.return_value = resp
            yield mock_requests

    def _get(self, station, desired_units=None):
        from metloom.variables import CuesLevel1Variables
        return station.get_daily_data(
            datetime(2020, 3, 15), datetime(2020, 3, 17),
            [CuesLevel1Variables.DOWNSHORTWAVE], desired_units=desired_units,
        )

    @property
    def col(self):
        from metloom.variables import CuesLevel1Variables
        return CuesLevel1Variables.DOWNSHORTWAVE.name

    def test_infer_units(self, station, mock_requests):
        df = self._get(station)
        assert df[f"{self.col}_units"].unique().tolist() == ["Watts/meter^2"]

    def test_conversion(self, station, mock_requests):
        raw = self._get(station)
        conv = self._get(station, desired_units="kW/m^2")
        assert conv[f"{self.col}_units"].unique().tolist() == ["kW/m^2"]
        np.testing.assert_allclose(
            conv[self.col].to_numpy(dtype=float),
            raw[self.col].to_numpy(dtype=float) / 1000.0,
        )


# ---------------------------------------------------------------------------
# NWS forecast (units inferred from wmoUnit code: wmoUnit:degC)
# ---------------------------------------------------------------------------
class TestNWSUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import NWSForecastPointData
        fx = load_fixture("nws")

        def side_effect(url, *args, **kwargs):
            obj = MagicMock()
            if "/gridpoints" in url:
                obj.json.return_value = fx["data"]
            else:
                obj.json.return_value = fx["initial"]
            return obj

        with patch("requests.get", side_effect=side_effect):
            yield NWSForecastPointData(
                "test", None,
                initial_metadata=shapely.geometry.Point(-119, 43),
                metadata=shapely.geometry.Point(-119, 43, 1000),
            )

    def _get(self, station, desired_units=None):
        from metloom.variables import NWSForecastVariables
        return station.get_hourly_forecast(
            [NWSForecastVariables.TEMP], desired_units=desired_units,
        )

    def test_infer_units(self, station):
        # NWS parsing strips the wmoUnit: namespace, leaving degC
        df = self._get(station)
        assert df["AIR TEMP_units"].unique().tolist() == ["degC"]

    def test_conversion(self, station):
        raw = self._get(station)
        conv = self._get(station, desired_units="degF")
        assert conv["AIR TEMP_units"].unique().tolist() == ["degF"]
        np.testing.assert_allclose(
            conv["AIR TEMP"].to_numpy(dtype=float),
            raw["AIR TEMP"].to_numpy(dtype=float) * 9.0 / 5.0 + 32.0, atol=1e-6,
        )


# ---------------------------------------------------------------------------
# MetNorway (units inferred from observation 'unit': degC)
# ---------------------------------------------------------------------------
class TestNorwayUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata import MetNorwayPointData
        pt = shapely.geometry.Point(8.0, 61.0, 500)
        with patch(
            "metloom.pointdata.norway.MetNorwayPointData.auth_header",
            new_callable=PropertyMock, return_value={},
        ):
            yield MetNorwayPointData("SN47610", "x", metadata=pt)

    @pytest.fixture
    def mock_requests(self):
        payload = load_fixture("norway_hourly")
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = payload
        with patch("metloom.pointdata.norway.requests") as mock_requests:
            mock_requests.get.return_value = resp
            yield mock_requests

    def _get(self, station, desired_units=None):
        from metloom.variables import MetNorwayVariables
        return station.get_hourly_data(
            datetime(2023, 8, 1), datetime(2023, 8, 2),
            [MetNorwayVariables.TEMP], desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_requests):
        df = self._get(station)
        assert df["AIR TEMP_units"].unique().tolist() == ["degC"]

    def test_conversion(self, station, mock_requests):
        raw = self._get(station)
        conv = self._get(station, desired_units="degF")
        assert conv["AIR TEMP_units"].unique().tolist() == ["degF"]
        np.testing.assert_allclose(
            conv["AIR TEMP"].to_numpy(dtype=float),
            raw["AIR TEMP"].to_numpy(dtype=float) * 9.0 / 5.0 + 32.0, atol=1e-6,
        )


# ---------------------------------------------------------------------------
# SnowEx (static units from the SensorDescription: deg C / w/m^2)
# ---------------------------------------------------------------------------
class TestSnowExUnits:
    @pytest.fixture
    def station(self, tmp_path):
        from metloom.pointdata import SnowExMet
        return SnowExMet("LSOS", cache=str(tmp_path))

    @pytest.fixture
    def mock_download(self, tmp_path):
        text = load_fixture("snowex_lsos")

        def _download(self, urls):
            fp = tmp_path.joinpath("snowex.csv")
            fp.write_text(text)
            return [fp]

        with patch(
            "metloom.pointdata.files.CSVPointData._download", _download
        ):
            yield

    @property
    def temp_col(self):
        from metloom.variables import SnowExVariables
        return SnowExVariables.TEMP_20FT.name

    @property
    def rad_col(self):
        from metloom.variables import SnowExVariables
        return SnowExVariables.UPSHORTWAVE.name

    def _get(self, station, desired_units=None):
        from metloom.variables import SnowExVariables
        return station.get_hourly_data(
            datetime(2017, 1, 1), datetime(2017, 1, 1, 3),
            [SnowExVariables.TEMP_20FT, SnowExVariables.UPSHORTWAVE],
            desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_download):
        df = self._get(station)
        assert df[f"{self.temp_col}_units"].unique().tolist() == ["deg C"]
        assert df[f"{self.rad_col}_units"].unique().tolist() == ["w/m^2"]

    def test_conversion(self, station, mock_download):
        raw = self._get(station)
        conv = self._get(station, desired_units={
            self.temp_col: "degF",
            self.rad_col: "kW/m^2",
        })
        assert conv[f"{self.temp_col}_units"].unique().tolist() == ["degF"]
        np.testing.assert_allclose(
            conv[self.temp_col].to_numpy(dtype=float),
            raw[self.temp_col].to_numpy(dtype=float) * 9.0 / 5.0 + 32.0,
            atol=1e-6,
        )
        np.testing.assert_allclose(
            conv[self.rad_col].to_numpy(dtype=float),
            raw[self.rad_col].to_numpy(dtype=float) / 1000.0,
        )


# ---------------------------------------------------------------------------
# CSAS (static units from the SensorDescription: meters / deg C)
# ---------------------------------------------------------------------------
class TestCSASUnits:
    @pytest.fixture
    def station(self, tmp_path):
        from metloom.pointdata import CSASMet
        return CSASMet("SBSP", cache=str(tmp_path))

    @pytest.fixture
    def mock_download(self, tmp_path):
        text = load_fixture("csas_sbsp")

        def _download(self, urls):
            fp = tmp_path.joinpath("csas.csv")
            fp.write_text(text)
            return [fp]

        with patch(
            "metloom.pointdata.files.CSVPointData._download", _download
        ):
            yield

    def _get(self, station, desired_units=None):
        from metloom.variables import CSASVariables
        return station.get_hourly_data(
            datetime(2023, 3, 1), datetime(2023, 3, 1, 3),
            [CSASVariables.SNOWDEPTH, CSASVariables.SURF_TEMP],
            desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_download):
        df = self._get(station)
        assert df["SNOWDEPTH_units"].unique().tolist() == ["meters"]
        assert df["SURFACE TEMP_units"].unique().tolist() == ["deg C"]

    def test_conversion(self, station, mock_download):
        raw = self._get(station)
        conv = self._get(station, desired_units={"SNOWDEPTH": "inch",
                                                 "SURFACE TEMP": "degF"})
        assert conv["SNOWDEPTH_units"].unique().tolist() == ["inch"]
        assert conv["SURFACE TEMP_units"].unique().tolist() == ["degF"]
        np.testing.assert_allclose(
            conv["SNOWDEPTH"].to_numpy(dtype=float),
            raw["SNOWDEPTH"].to_numpy(dtype=float) / 0.0254, rtol=1e-6,
        )


# ---------------------------------------------------------------------------
# SAIL/ARM (static units from the SensorDescription extra: w/m^2)
# ---------------------------------------------------------------------------
class TestSAILUnits:
    @pytest.fixture
    def station(self):
        from metloom.pointdata.sail import SAILPointData
        return SAILPointData("GUC:M1")

    @pytest.fixture
    def mock_arm(self):
        df = load_fixture("sail_precip")
        with patch(
            "metloom.pointdata.sail.arm_utils.get_station_data",
            return_value=df,
        ) as mock:
            yield mock

    def _get(self, station, desired_units=None):
        from metloom.variables import SAILStationVariables
        return station.get_daily_data(
            datetime(2023, 1, 1), datetime(2023, 1, 2),
            [SAILStationVariables.PRECIPITATION], desired_units=desired_units,
        )

    def test_infer_units(self, station, mock_arm):
        df = self._get(station)
        assert df["PRECIPITATION_units"].unique().tolist() == ["mm"]

    def test_conversion(self, station, mock_arm):
        raw = self._get(station)
        conv = self._get(station, desired_units="inch")
        col = "PRECIPITATION"
        assert conv[f"{col}_units"].unique().tolist() == ["inch"]
        np.testing.assert_allclose(
            conv[col].to_numpy(dtype=float),
            raw[col].to_numpy(dtype=float) / 25.4, rtol=1e-6,
        )

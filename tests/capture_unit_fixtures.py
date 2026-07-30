"""
Dev-only helper: capture the raw payloads each metloom client parses and
pickle them under ``tests/data/unit_fixtures/`` so the unit-conversion tests
can run fully offline against real data shapes.

This is NOT collected by pytest (no ``test_`` prefix). Run it once to
(re)generate fixtures::

    ./venv/bin/python tests/capture_unit_fixtures.py

Strategy per the agreed plan:
  * Pull genuinely live data where it is cheap and credential-free (CDEC).
  * For everything else, source the payloads from the committed real-response
    mock files already in ``tests/data/*_mocks`` (these are captured real API
    responses) and re-serialize them as pickles. Payloads whose canonical
    form is not stored as a file (Mesowest / SNOTEL / SAIL) are reproduced
    here in the exact shape the live APIs return.

Every fixture is the *raw* payload (parsed JSON / CSV text / row lists), NOT a
finished DataFrame, so the client parsing + unit inference stays under test.
"""
import json
import pickle
from pathlib import Path

HERE = Path(__file__).resolve().parent
MOCKS = HERE / "data"
OUT = MOCKS / "unit_fixtures"


def _dump(name, obj):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"{name}.pkl", "wb") as fp:
        pickle.dump(obj, fp)
    print(f"wrote {name}.pkl")


def capture_cdec():
    """Live pull of real CDEC data for Tenaya Lake (TNY)."""
    try:
        import requests
        url = "http://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"
        out = {}
        for key, sensor in [("swe", "3"), ("temp", "30")]:
            resp = requests.get(url, params={
                "Stations": "TNY", "dur_code": "D", "SensorNums": sensor,
                "Start": "2021-05-15T00:00:00", "End": "2021-05-18T00:00:00",
            }, timeout=20)
            resp.raise_for_status()
            out[key] = resp.json()
        # sanity: make sure we got units in the payload
        assert out["swe"] and "units" in out["swe"][0]
        _dump("cdec", out)
    except Exception as e:  # pragma: no cover - network dependent
        print(f"CDEC live pull failed ({e}); writing static fallback")
        out = {
            "swe": [
                {"stationId": "TNY", "durCode": "D", "SENSOR_NUM": 3,
                 "date": f"2021-5-{d} 00:00", "obsDate": f"2021-5-{d} 00:00",
                 "value": v, "dataFlag": " ", "units": "INCHES"}
                for d, v in [(16, 12.1), (17, 12.0), (18, 11.8)]
            ],
            "temp": [
                {"stationId": "TNY", "durCode": "D", "SENSOR_NUM": 30,
                 "date": f"2021-5-{d} 00:00", "obsDate": f"2021-5-{d} 00:00",
                 "value": v, "dataFlag": " ", "units": "DEG F"}
                for d, v in [(16, 33.1), (17, 34.2), (18, 35.0)]
            ],
        }
        _dump("cdec", out)


def capture_usgs():
    with open(MOCKS / "usgs_mocks" / "daily_response.txt") as fp:
        _dump("usgs_daily", json.load(fp))


def capture_geosphere():
    """
    GeoSphere data responses are GeoJSON FeatureCollections with a top-level
    ``timestamps`` list and per-parameter ``unit``/``data`` under
    ``features[0].properties.parameters``. (klima_mock.json is the *metadata*
    endpoint, not the data endpoint, so it is not used here.)
    """
    payload = {
        "media_type": "application/json", "type": "FeatureCollection",
        "version": "v1",
        "timestamps": [
            "2023-01-20T00:00+00:00", "2023-01-21T00:00+00:00",
            "2023-01-22T00:00+00:00", "2023-01-23T00:00+00:00",
            "2023-01-24T00:00+00:00", "2023-01-25T00:00+00:00",
        ],
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [11.700833, 47.5075]},
            "properties": {
                "parameters": {
                    "schnee": {
                        "name": "Gesamtschneehoehe",
                        "unit": "cm",
                        "data": [3.0, 18.0, 22.0, 18.0, 18.0, 14.0],
                    }
                },
                "station": "8807",
            },
        }],
    }
    _dump("geosphere_hist", payload)


def capture_cues():
    with open(MOCKS / "cues_mocks" / "daily_response.txt") as fp:
        _dump("cues_daily", fp.read())


def capture_nws():
    with open(MOCKS / "nws_mocks" / "meta_and_data.json") as fp:
        meta_and_data = json.load(fp)
    with open(MOCKS / "nws_mocks" / "initial_meta.json") as fp:
        initial_meta = json.load(fp)
    _dump("nws", {"initial": initial_meta, "data": meta_and_data})


def capture_norway():
    with open(MOCKS / "frost_mocks" / "hourly_temp.json") as fp:
        _dump("norway_hourly", json.load(fp))


def _csv_subset(path, n_rows=48):
    """Header + first n_rows data lines of a csv, as text."""
    with open(path) as fp:
        lines = fp.readlines()
    return "".join(lines[: n_rows + 1])


def capture_snowex():
    _dump(
        "snowex_lsos",
        _csv_subset(MOCKS / "snowex_mocks" / "SNEX_Met_LSOS_final_output.csv"),
    )


def capture_csas():
    _dump(
        "csas_sbsp",
        _csv_subset(MOCKS / "csas_mocks" / "SBSP_1hr_2010-2024.csv"),
    )


def capture_mesowest():
    """
    Reproduce the Mesowest timeseries response shape (units=metric). Real
    responses nest observations under STATION[0]['OBSERVATIONS'] and report
    the per-variable units under 'UNITS'.
    """
    dts = [
        "2021-03-16T00:00:00Z", "2021-03-16T01:00:00Z", "2021-03-16T02:00:00Z",
    ]
    payload = {
        "SUMMARY": {"RESPONSE_MESSAGE": "OK"},
        "UNITS": {"air_temp": "Celsius", "snow_depth": "Millimeters"},
        "STATION": [{
            "STID": "INMTP",
            "OBSERVATIONS": {
                "date_time": dts,
                "air_temp_set_1": [-2.1, -1.4, -0.8],
                "snow_depth_set_1": [1200.0, 1210.0, 1205.0],
            },
        }],
    }
    _dump("mesowest", payload)


def capture_snotel():
    """
    SNOTEL data comes back from the AWDB SOAP service as lists of
    {'datetime','value'} rows; units are inferred from the station elements
    (storedUnitCd). Reproduce both.
    """
    payload = {
        "tz_hours": -8.0,
        "elements": [
            {"elementCd": "WTEQ", "duration": "DAILY", "storedUnitCd": "in"},
            {"elementCd": "TOBS", "duration": "DAILY", "storedUnitCd": "degF"},
        ],
        "data": {
            "WTEQ": [
                {"datetime": "2020-03-20", "value": 13.19},
                {"datetime": "2020-03-21", "value": 13.17},
                {"datetime": "2020-03-22", "value": 13.14},
            ],
            "TOBS": [
                {"datetime": "2020-03-20", "value": 30.2},
                {"datetime": "2020-03-21", "value": 31.5},
                {"datetime": "2020-03-22", "value": 33.8},
            ],
        },
    }
    _dump("snotel_daily", payload)


def capture_sail():
    """
    SAIL/ARM data is delivered as netCDF; arm_utils.get_station_data returns a
    DataFrame indexed on datetime with a column per variable code. Reproduce a
    small frame for the precipitation variable (mm).
    """
    import pandas as pd
    idx = pd.date_range("2023-01-01", periods=6, freq="h", name="datetime")
    df = pd.DataFrame(
        {"accum_rtnrt": [0.0, 0.1, 0.3, 0.0, 0.2, 0.5]}, index=idx
    )
    _dump("sail_precip", df)


def main():
    capture_cdec()
    capture_usgs()
    capture_geosphere()
    capture_cues()
    capture_nws()
    capture_norway()
    capture_snowex()
    capture_csas()
    capture_mesowest()
    capture_snotel()
    capture_sail()
    print(f"\nFixtures written to {OUT}")


if __name__ == "__main__":
    main()

from datetime import datetime
from os.path import join
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest

from metloom.pointdata import USGSPointData
from metloom.variables import USGSVariables
from tests.test_point_data import BasePointDataTest

DATA_DIR = str(Path(__file__).parent.joinpath("data"))


class TestUSGSStation(BasePointDataTest):

    @staticmethod
    def usgs_daily_discharge_response():
        return {
            "value": {
                "timeSeries": [
                    {
                        "values": [
                            {"value": [
                                {"value": '111', "dateTime": "2020-07-01T00:00:00.000"},
                                {"value": '112', "dateTime": "2020-07-02T00:00:00.000"}
                            ]}],
                        "sourceInfo": {
                            "geoLocation": {
                                "geogLocation":
                                    {"longitude": -106.54, "latitude": 37.35}
                            },
                            "timeZoneInfo":
                                {"defaultTimeZone": {"zoneOffset": "-5:00"}}
                        },
                        "variable": {"unit": {"unitCode": "cf/s"},
                                     "noDataValue": -9999.0},
                    }]}
        }

    @staticmethod
    def station_search_response():
        df = pd.DataFrame.from_records(
            [
                (
                    "USGS",
                    11276500,
                    "TUOLUMNE"
                    "R NR HETCH HETCHY CA",
                    "ST",
                    37.93742147,
                    -119.7982326,
                    "F",
                    "NAD83",
                    3430.00,
                    20,
                    "NGVD29",
                    18040009
                ),
                (
                    "USGS",
                    11274790,
                    "TUOLUMNE R A GRAND CYN OF TUOLUMNE AB HETCH HETCHY",
                    "ST",
                    37.9165884,
                    -119.6598938,
                    "S",
                    "NAD83",
                    3830,
                    20,
                    "NGVD29",
                    18040009
                )
            ],
            columns=[
                "agency_cd", "site_no", "station_nm", "site_tp_cd", "dec_lat_va",
                "dec_long_va", "coord_acy_cd,", "dec_coord_datum_cd", "alt_va",
                "alt_acy_va", "alt_datum_cd", "huc_cd"
            ],
        )
        return df, []

    @pytest.fixture(scope="function")
    def crp_station(self):
        return USGSPointData("08245000", "Conejos R bl Platoro Reservoir")

    @pytest.fixture(scope="class")
    def crp_daily_expected(self):
        points = gpd.points_from_xy([-106.54], [37.35], z=[1000.0])
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2020-07-01 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": '111',
                    "DISCHARGE_units": "cf/s",
                    "site": "08245000",
                    "datasource": "USGS"
                },
                {
                    "datetime": pd.Timestamp("2020-07-02 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": '112',
                    "DISCHARGE_units": "cf/s",
                    "site": "08245000",
                    "datasource": "USGS"
                },

            ],
            geometry=[points[0]] * 2,
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime",
                "geometry",
                "DISCHARGE",
                "site",
                "DISCHARGE_units",
                "datasource"
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

    @staticmethod
    def crp_meta_return():
        with open(join(DATA_DIR, "platoro_meta.txt")) as fp:
            data_text = fp.read()

        return data_text

    @staticmethod
    def failure_response():
        class Response:
            status_code = 404
            content = (
                b'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" '
                b'"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"><html xmlns='
                b'"http://www.w3.org/1999/xhtml"><head><title>Error report</title>'
                b'<style type="text/css"><!--H1 {font-family:Tahoma,Arial,sans-serif;'
                b'color:white;background-color:#525D76;font-size:22px;} H2 {font-family'
                b':Tahoma,Arial,sans-serif;color:white;background-color:#525D76;font-'
                b'size:16px;} H3 {font-family:Tahoma,Arial,sans-serif;color:white;'
                b'background-color:#525D76;font-size:14px;} BODY {font-family:Tahoma,'
                b'Arial,sans-serif;color:black;background-color:white;} B {font-family:'
                b'Tahoma,Arial,sans-serif;color:white;background-color:#525D76;} P '
                b'{font-family:Tahoma,Arial,sans-serif;background:white;color:black;'
                b'font-size:12px;}A {color : black;}HR {color : #525D76;}--></style> '
                b'</head><body><h1>HTTP Status 404 - No sites found matching this '
                b'request, server=[sdas01]</h1><hr/><p><b>type</b> Status report</p>'
                b'<p><b>message</b>No sites found matching this request, server='
                b'[sdas01]</p><p><b>description</b>The requested resource is not '
                b'available.</p><hr/><h3>Error Report</h3></body></html>')
        return Response

    @classmethod
    def crp_side_effect(cls, url, **kwargs):
        mock = MagicMock()
        params = kwargs["params"]

        if "startDT" not in params:
            mock.text = cls.crp_meta_return()
        elif "startDT" in params:
            mock.json.return_value = cls.usgs_daily_discharge_response()
        else:
            raise ValueError("unknown scenario")

        return mock

    def test_get_metadata(self, crp_station):
        with patch("metloom.pointdata.usgs.requests") as mock_requests:
            mock_requests.get.side_effect = self.crp_side_effect
            metadata = crp_station.metadata
            mock_get = mock_requests.get
            assert mock_get.call_count == 1
            mock_get.assert_called_with(
                "https://waterservices.usgs.gov/nwis/site/",
                params={
                    "format": "rdb",
                    "sites": "08245000",
                    "siteOutput": "expanded",
                    "siteStatus": "all"
                },
            )

        expected = gpd.points_from_xy([-106.54], [37.35], z=[1000.0])[0]
        assert expected == metadata

    def test_get_daily_data(self, crp_station, crp_daily_expected):
        with patch("metloom.pointdata.usgs.requests") as mock_requests:
            mock_get = mock_requests.get
            mock_get.side_effect = self.crp_side_effect
            response = crp_station.get_daily_data(
                datetime(2020, 7, 1),
                datetime(2020, 7, 2),
                [USGSVariables.DISCHARGE],
            )

            mock_get.assert_any_call(
                "https://waterservices.usgs.gov/nwis/dv/",
                params={
                    'startDT': datetime(2020, 7, 1).date().isoformat(),
                    'endDT': datetime(2020, 7, 2).date().isoformat(),
                    'sites': '08245000',
                    'parameterCd': '00060',
                    'format': 'json',
                    'siteType': 'ST',
                    'siteStatus': 'all'
                },
            )
            assert mock_get.call_count == 2
        pd.testing.assert_frame_equal(response, crp_daily_expected)

    def test_points_from_geometry(self, shape_obj):
        expected_url = (
            'https://waterservices.usgs.gov/nwis/site/?format=rdb&bBox=-119.8%2C37.7'
            '%2C-119.2%2C38.2&siteStatus=active&hasDataTypeCd=dv&parameterCd=00060'
        )
        names = [
            'TUOLUMNER NR HETCH HETCHY CA',
            'TUOLUMNE R A GRAND CYN OF TUOLUMNE AB HETCH HETCHY'
        ]
        with patch("metloom.pointdata.usgs.USGSPointData._get_url_response") as mock_tb:
            mock_tb.return_value = self.station_search_response()
            result = USGSPointData.points_from_geometry(
                shape_obj, [USGSVariables.DISCHARGE]
            )
            assert mock_tb.call_args[0][0] == expected_url
            assert len(result) == 2
            assert [x.name in names for x in result.points]

    def test_points_from_geometry_failure(self, shape_obj):
        expected_url = (
            'https://waterservices.usgs.gov/nwis/site/?format=rdb&bBox=-119.8%2C37.7'
            '%2C-119.2%2C38.2&siteStatus=active&hasDataTypeCd=dv&parameterCd=74082'
        )
        with patch("metloom.pointdata.usgs.requests.get") as mock_request:
            mock_request.return_value = self.failure_response()
            result = USGSPointData.points_from_geometry(
                shape_obj, [USGSVariables.STREAMFLOW]
            )
            assert result.points == []
            assert mock_request.call_args[0][0] == expected_url

    def test_geometry_failure_message(self, shape_obj):
        expected_error_msg = (
            "HTTP Status 404 - No sites found matching this request, server=[sdas01]"
        )
        with patch("metloom.pointdata.usgs.requests.get") as mock_request:
            mock_request.return_value = self.failure_response()
            point = USGSPointData("123", "test")
            result, error_msg = point._get_url_response("test")
            assert result == []
            assert error_msg == expected_error_msg


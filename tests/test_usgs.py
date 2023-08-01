from datetime import datetime
from os.path import join
from pathlib import Path
from unittest.mock import patch
import json

import geopandas as gpd
import pandas as pd
import pytest

from metloom.pointdata import USGSPointData
from metloom.variables import USGSVariables
from tests.test_point_data import BasePointDataTest

DATA_DIR = str(Path(__file__).parent.joinpath("data/usgs_mocks"))


class TestUSGSStation(BasePointDataTest):

    @staticmethod
    def station_search_response():
        with open(join(DATA_DIR, "station_search_response.txt")) as fp:
            data_text = fp.read()

        return data_text

    @pytest.fixture(scope="function")
    def crp_station(self):
        return USGSPointData("08245000", "Conejos R bl Platoro Reservoir")

    @pytest.fixture(scope="class")
    def crp_daily_expected(self):
        points = gpd.points_from_xy([-106.54], [37.35], z=[9866.6])
        df = gpd.GeoDataFrame.from_dict(
            [
                {
                    "datetime": pd.Timestamp("2020-07-01 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": 721.0,
                    "DISCHARGE_units": "ft3/s",
                    "site": "08245000",
                    "datasource": "USGS",
                },
                {
                    "datetime": pd.Timestamp("2020-07-02 07:00:00+0000", tz="UTC"),
                    "DISCHARGE": 664.0,
                    "DISCHARGE_units": "ft3/s",
                    "site": "08245000",
                    "datasource": "USGS",
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
                "datasource",
            ]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

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
    def get_url_response(cls, resp="daily"):
        if resp == 'daily':
            with open(join(DATA_DIR, "daily_response.txt")) as fp:
                data = json.load(fp)
        elif resp == 'metadata':
            with open(join(DATA_DIR, "platoro_meta.txt")) as fp:
                data = fp.read()
        elif resp == 'hourly':
            with open(join(DATA_DIR, "hourly_response.json")) as fp:
                data = json.load(fp)
        else:
            raise RuntimeError(f"{resp} is an unknown option")

        return data

    def test_get_metadata(self, crp_station):
        with patch("metloom.pointdata.usgs.USGSPointData._get_url_response") \
                as mock_request:
            mock_request.return_value = self.get_url_response(resp="metadata")
            metadata = crp_station.metadata

        expected = gpd.points_from_xy([-106.54], [37.35], z=[9866.6])[0]
        assert expected == metadata

    def test_get_daily_data(self, crp_station, crp_daily_expected):
        with patch("metloom.pointdata.usgs.USGSPointData._get_url_response") \
                as mock_requests:
            mock_requests.side_effect = [
                self.get_url_response(),
                self.get_url_response(resp='metadata')
            ]
            response = crp_station.get_daily_data(
                datetime(2020, 7, 1),
                datetime(2020, 7, 2),
                [USGSVariables.DISCHARGE],
            )
        pd.testing.assert_frame_equal(response, crp_daily_expected)

    def test_get_hourly_data(self, crp_station, crp_daily_expected):
        """
        Test that we resample from 15m to 1 hour correctly
        """
        with patch("metloom.pointdata.usgs.USGSPointData._get_url_response") \
                as mock_requests:
            mock_requests.side_effect = [
                self.get_url_response(resp='hourly'),
                self.get_url_response(resp='metadata')
            ]
            response = crp_station.get_hourly_data(
                datetime(2023, 1, 13),
                datetime(2023, 1, 13),
                [USGSVariables.DISCHARGE],
            )
        response = response.reset_index()
        assert response["datetime"].values[0] == pd.to_datetime("2023-01-13 07")
        assert response["datetime"].values[-1] == pd.to_datetime("2023-01-14 06")
        assert response["DISCHARGE"].values[0] == 300.0
        assert response["DISCHARGE"].values[-1] == 283.25
        assert all(response["site"].values == "08245000")

    def test_points_from_geometry(self, shape_obj):
        expected_url = (
            'https://waterservices.usgs.gov/nwis/site/?format=rdb&bBox=-119.8%2C37.7'
            '%2C-119.2%2C38.2&siteStatus=active&hasDataTypeCd=dv,iv&parameterCd=00060'
        )
        names = [
            'TUOLUMNER NR HETCH HETCHY CA',
            'TUOLUMNE R A GRAND CYN OF TUOLUMNE AB HETCH HETCHY',
            'MILL C BL LUNDY LK NR LEE VINING CA',
            'LEE VINING C BL SADDLEBAG LK NR LEE VINING CA'
        ]
        with patch("metloom.pointdata.usgs.USGSPointData._get_url_response") as mock_tb:
            mock_tb.return_value = self.station_search_response()
            result = USGSPointData.points_from_geometry(
                shape_obj, [USGSVariables.DISCHARGE]
            )
            assert mock_tb.call_args[0][0] == expected_url
            assert len(result) == 10
            assert [x.name in names for x in result.points]

    def test_points_from_geometry_failure(self, shape_obj):
        expected_url = (
            'https://waterservices.usgs.gov/nwis/site/?format=rdb&bBox=-119.8%2C37.7'
            '%2C-119.2%2C38.2&siteStatus=active&hasDataTypeCd=dv,iv&parameterCd=74082'
        )
        with patch("metloom.pointdata.usgs.requests.get") as mock_request:
            mock_request.return_value = self.failure_response()
            result = USGSPointData.points_from_geometry(
                shape_obj, [USGSVariables.STREAMFLOW]
            )
            assert result.points == []
            assert mock_request.call_args[0][0] == expected_url

    def test_geometry_failure_message(self, caplog, shape_obj):
        expected_error_msg = (
            "No data: HTTP Status 404 - No sites found matching this request, "
            "server=[sdas01]"
        )
        with patch("metloom.pointdata.usgs.requests.get") as mock_request:
            mock_request.return_value = self.failure_response()
            point = USGSPointData("123", "test")
            result = point._get_url_response("test")
            assert result == []

        assert expected_error_msg in caplog.text

    def test_check_dates(self, crp_station):
        start_date = datetime(2020, 7, 1, 1, 1, 1)
        end_date = datetime(2020, 7, 2, 1, 1, 1)
        checked_start, checked_end = crp_station._check_dates(
            start_date, end_date
        )

        assert start_date.date() == checked_start
        assert end_date.date() == checked_end

    def test_dates_fail(self, crp_station, caplog):
        start_date = datetime(2020, 7, 2, 1, 1, 1)
        end_date = datetime(2020, 7, 1, 1, 1, 1)
        error_msg = (
            f" end_date '{end_date.date()}' must be later than "
            f"start_date '{start_date.date()}'"
        )
        with pytest.raises(ValueError):
            checked_start, checked_end = crp_station._check_dates(
                start_date, end_date
            )

        assert error_msg in caplog.text

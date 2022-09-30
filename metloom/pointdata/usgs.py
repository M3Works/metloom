from datetime import datetime, timezone, timedelta
from typing import List
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import logging

from .base import PointData
from ..variables import USGSVariables, SensorDescription
from ..dataframe_utils import merge_df

LOG = logging.getLogger(__name__)


class USGSPointData(PointData):
    """
    Implement PointData methods for USGS data source.

    Currently only handling daily data.
    _check_response needs works
    negative time, line 56

    APIs:
        https://waterservices.usgs.gov/nwis/
        https://streamstats.usgs.gov/docs/streamstatsservices/#/
        https://waterservices.usgs.gov/rest/DV-Service.html#Service

    """

    ALLOWED_VARIABLES = USGSVariables
    BASE_URL = "https://waterservices.usgs.gov/nwis/"
    USGS_URL = BASE_URL + "dv/"
    META_URL = BASE_URL + "site/"
    DATASOURCE = "USGS"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(USGSPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None

    def _get_all_metadata(self, resp):
        """
        Use the full json response from site data url because it contains more info
        than the USGS 'metadata' url
        """
        base = resp.json()["value"]["timeSeries"][0]
        loc = base["sourceInfo"]["geoLocation"]["geogLocation"]
        str_time = base["sourceInfo"]['timeZoneInfo']['defaultTimeZone']['zoneOffset']
        self._units = base["variable"]["unit"]["unitCode"]
        offset = datetime.strptime(str_time.strip("-"), "%H:%M").hour
        if "-" in str_time:
            offset = -offset

        self._tzinfo = timezone(timedelta(hours=offset))

        self._metadata = gpd.points_from_xy([loc["longitude"]], [loc["latitude"]])[0]
        return gpd.points_from_xy([loc["longitude"]], [loc["latitude"]])[0]

    def _data_request(self, params):
        """
        Make request to USGS and return JSON
        Args:
            params: dictionary of request parameters
        Returns:
            dictionary of response values
        """

        resp = requests.get(self.USGS_URL, params=params)
        resp.raise_for_status()
        self._check_response(resp)
        self._get_all_metadata(resp)

        return resp.json()["value"]["timeSeries"][0]["values"][0]["value"]

    def _check_response(self, resp):
        """

        Args:
             resp: dict of request response
        """

        resp = resp.json()

        # this is not robust
        if "value" not in resp:
            raise ValueError("Empty response from url request")
            # LOG.warning(" Empty response from url request")

        if len(resp["value"]["timeSeries"]) < 1:
            raise ValueError("No data, requested site may not have requested sensor")
            # LOG.warning(" No data, requested site may not have requested sensor")

    def _sensor_response_to_df(self, response_data, sensor, final_columns, site_id):
        """
        Convert the response data from the API to a GeoDataFrame
        Format and map columns in the dataframe
        Args:
            response_data: JSON list response from API
            sensor: SensorDescription obj
            final_columns: List of columns used for filtering
            site_id: site id
        Returns:
            GeoDataFrame
        """
        sensor_df = gpd.GeoDataFrame.from_dict(
            response_data,
            geometry=[self.metadata] * len(response_data),
        )
        sensor_df.replace(-9999.0, np.nan, inplace=True)
        sensor_df["site"] = site_id
        sensor_df[f"{sensor.name}_units"] = self._units
        final_columns += [sensor.name, f"{sensor.name}_units"]
        column_map = {"dateTime": "datetime", "value": sensor.name}

        sensor_df.rename(
            columns=column_map,
            inplace=True,
        )

        sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
        sensor_df["datetime"] = sensor_df["datetime"].apply(self._handle_df_tz)

        # set index so joining works
        sensor_df.set_index("datetime", inplace=True)
        sensor_df = sensor_df.filter(final_columns)
        sensor_df = sensor_df.loc[pd.notna(sensor_df[sensor.name])]
        return sensor_df

    def _get_data_fallback(self, params, duration_list):
        """
        Allow for fallback on finer resolution API durations with resample
        if the desired duration does not return data
        Args:
            params: request params with or without dur_code
            duration_list: list of durations to try. First index is desired
                durations
        """
        if len(duration_list) < 1:
            raise ValueError("Duration list cannot be empty")
        response_data = []
        df_duration = duration_list[0]
        for duration in duration_list:
            response_data = self._data_request(params)
            if response_data:
                df_duration = duration
                break
        return response_data, df_duration

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        duration_list: List[str]
    ):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            duration_list: USGS duration code and fallbacks, currently only ["dv"]
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """

        if duration_list[0] == "dv":
            start_date = start_date.date().isoformat()
            end_date = end_date.date().isoformat()
        else:
            start_date = start_date.isoformat()
            end_date = end_date.isoformat()

        params = {
            'startDT': start_date,
            'endDT': end_date,
            'sites': self.id,
            'format': 'json',
            'siteType': 'ST',
            'siteStatus': 'all'
        }

        df = None
        final_columns = ["geometry", "site"]
        desired_duration = duration_list[0]

        for sensor in variables:
            params["parameterCd"] = sensor.code
            response_data, response_duration = self._get_data_fallback(
                params, duration_list
            )
            if response_data:
                # don't resample if we have the desired duration
                if response_duration == desired_duration:
                    resample_duration = None
                else:
                    resample_duration = desired_duration

                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns, self.id, sensor,
                    resample_duration=resample_duration
                )
                df = merge_df(df, sensor_df)

        if df is not None:
            if len(df.index) > 0:
                # Set the datasource
                df["datasource"] = [self.DATASOURCE] * len(df.index)
                df.reset_index(inplace=True)
                df.set_index(keys=["datetime", "site"], inplace=True)
                df.index.set_names(["datetime", "site"], inplace=True)
            else:
                df = None
        self.validate_sensor_df(df)
        return df

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data

        Currently just have daily values ["dv"], have not incorporated hourly ["iv"]
        """

        return self._get_data(start_date, end_date, variables, ["dv"])

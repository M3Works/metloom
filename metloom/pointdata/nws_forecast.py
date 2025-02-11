import logging
from datetime import timedelta, timezone
from typing import List
import pandas as pd
import geopandas as gpd
import requests
from geopandas import GeoDataFrame

from metloom.dataframe_utils import merge_df, resample_whole_df
from metloom.pointdata.base import GenericPoint
from metloom.variables import SensorDescription, NWSForecastVariables


LOG = logging.getLogger(__name__)


class NWSForecastPointData(GenericPoint):
    """
    Implementation for NWS forecast API
    https://www.weather.gov/documentation/services-web-api

    We can call the points api to get the URL for the forecast, i.e.
    https://api.weather.gov/points/42,-119

    In this example, 3 forecast URLs are available

    "forecast": "https://api.weather.gov/gridpoints/BOI/28,28/forecast",
    "forecastHourly": "https://api.weather.gov/gridpoints/BOI/28,28/forecast/hourly",
    "forecastGridData": "https://api.weather.gov/gridpoints/BOI/28,28",

    forecast can be used to return the 12 hour increments

    forecastHourly returns hourly data (hourly in local tz)

    forecastGridData returns the 'raw' grid data (hourly UTC).

    We will use the `forecastGridData` endpoint for this implementation

    The API returns data from a grid, meaning the `geometry` column
    in the returned geodataframes will represent the **CENTER** of
    the forecast grid cell.

    """
    DATASOURCE = "NWS Forecast"
    ALLOWED_VARIABLES = NWSForecastVariables
    URL = "https://api.weather.gov"
    POINTS_FROM_GEOM_DEFAULTS = {
        'within_geometry': True,
        'token_json': "~/.frost_token.json",
        'buffer': 0.0
    }

    def __init__(
        self, station_id, name,
        initial_metadata=None, metadata=None,
    ):
        """
        Args:
            station_id: id of station
            name: name of station
            initial_metadata: shapely point required to find the
                forecast grid cell
            metadata: optional metadata for the station (shapely point)
        """
        if initial_metadata is None:
            raise ValueError("Initial metadata is required for the NWS Forecast class")
        super(NWSForecastPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._inital_metadata = initial_metadata
        # default UTC time
        self._tzinfo = timezone(timedelta(hours=0))

        # set the forecast grid parameters
        self._gridx = None
        self._gridy = None
        self._office = None
        self._grid_outline = None

    def _get_initial_metadata(self):
        """
        Get all metadata from the API for one point.
        """
        # use the initial metadata to find the grid points for the forecast
        resp = requests.get(
            f"{self.URL}/points/{self._inital_metadata.y},{self._inital_metadata.x}"
        )
        resp.raise_for_status()
        data = resp.json()
        properties = data["properties"]

        return properties

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        This setts the _metadata and _gridx and _gridy parameters
        """
        properties = self._get_initial_metadata()
        self._gridx = properties["gridX"]
        self._gridy = properties["gridY"]
        self._office = properties["gridId"]

        # use the grid points to find the center of the forecast cell

        url = f"{self.URL}/gridpoints/" \
              f"{self._office}/{self._gridx},{self._gridy}"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        grid_properties = data["properties"]
        # Parse the polygon into a geodataframe
        df_loc = gpd.GeoDataFrame.from_features([data])
        self._grid_outline = df_loc.geometry.values[0]
        # find the center
        center = df_loc.centroid[0]

        return gpd.points_from_xy(
            [center.x],
            [center.y],
            z=[grid_properties["elevation"]["value"] * 3.28084]  # convert to ft
        )[0]

    def _get_observations(self):
        """
        Get the hourly data for a 7 day forecast
        Example request: https://api.weather.gov/gridpoints/BOI/28,28
        """
        # ensure we have office, gridx, and gridy set
        if self._metadata is None:
            self._get_metadata()
        url = f"{self.URL}/gridpoints/" \
              f"{self._office}/{self._gridx},{self._gridy}"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        return data["properties"]

    def _sensor_response_to_df(
        self, response_data, sensor, final_columns,
        resample_duration=None
    ):
        """
        Process the response from the API into a dataframe for 1 sensor

        Args:
            response_data: list of entries from the API
            sensor: single variable object
            final_columns: expected columns
            resample_duration: if a resample is desired, a duration that can
                be parsed by pandas

        Returns
            Geodataframe of data
        """
        # Get the list of response data
        sensor_data = response_data[sensor.code]
        unit_str = sensor_data["uom"].split("wmoUnit:")[-1]
        df = pd.DataFrame.from_records(sensor_data["values"])

        # Rename and resample
        column_map = {
            "validTime": "datetime",
            "value": sensor.name,
        }
        df.rename(
            columns=column_map,
            inplace=True,
        )

        # parse midway through dates
        # (example datetime is '2024-06-19T04:00:00+00:00/PT1H')
        date_starts = pd.to_datetime(
            df["datetime"].str.split("/").apply(lambda x: x[0])
        )
        date_durations = pd.to_timedelta(
            df["datetime"].str.split("/").apply(lambda x: x[1])
        )
        date_mids = date_starts + date_durations / 2.0
        df["datetime"] = date_mids
        df = df.set_index("datetime")

        # resample to the desired duration
        if resample_duration is not None:
            df = resample_whole_df(
                df, sensor,
                interval=resample_duration
            )

        # add other expected columns
        df[f"{sensor.name}_units"] = [unit_str] * len(df)
        df["site"] = [self.id] * len(df)

        # keep the column names
        final_columns += [
            sensor.name, f"{sensor.name}_units",
        ]

        df = GeoDataFrame(
            df, geometry=[self.metadata] * len(df)
        )

        # double check utc conversion
        df = df.tz_convert(self.desired_tzinfo)

        # set index so joining works
        df = df.filter(final_columns)
        df = df.loc[pd.notna(df[sensor.name])]
        return df

    def _get_data(
        self,
        variables: List[SensorDescription],
        desired_duration=None,
    ):
        """
        Args:
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
            desired_duration: desired resample duration ("D", "h"). Data is
                hourly be default
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """

        df = None
        final_columns = ["geometry", "site"]
        # Get data from the API
        response_data = self._get_observations()
        if response_data:
            # Parse data for each variable
            for sensor in variables:
                # TODO: how does resampling of non-hourly precip work
                sensor_df = self._sensor_response_to_df(
                    response_data, sensor, final_columns,
                    resample_duration=desired_duration
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

    def get_daily_forecast(
        self,
        variables: List[SensorDescription],
    ):
        """
        Get a geopandas dataframe with daily results for a 7 day forecast.
        The geometry column will be the center of the forecast gridcell

        Args:
            variables: list of variables to return
        """
        return self._get_data(variables, desired_duration="D")

    def get_hourly_forecast(
        self,
        variables: List[SensorDescription],
    ):
        """
        Get a geopandas dataframe with hourly results for a 7 day forecast.
        The geometry column will be the center of the forecast gridcell

        Args:
            variables: list of variables to return
        """
        return self._get_data(variables, desired_duration="h")

    def get_forecast(
        self,
        variables: List[SensorDescription],
    ):
        """
        Get a geopandas dataframe with hourly results for a 7 day forecast.
        The geometry column will be the center of the forecast gridcell

        Args:
            variables: list of variables to return
        """
        # Do not resample
        return self._get_data(variables)

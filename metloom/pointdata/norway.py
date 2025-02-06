import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import requests
from geopandas import GeoDataFrame

from metloom.dataframe_utils import append_df, merge_df, resample_whole_df
from metloom.pointdata.base import PointData
from metloom.variables import SensorDescription, MetNorwayVariables


LOG = logging.getLogger(__name__)


class MetNorwayPointData(PointData):
    """
    Class for the Norway Frost API
    https://frost.met.no/index.html

    To create a user, go here https://frost.met.no/auth/requestCredentials.html

    Data is provided by MET Norway, see license for details
    https://www.met.no/en/free-meteorological-data

    Element (variable) information can be found here
    https://frost.met.no/elementtable

    Observations/AvailableTimeSeries/ can be used to find out what types elements
    are available for a station or time range - we can use this to filter

    The Sources endpoint returns metadata. It can be used to filter
    based on geometry and variables

    For this class we will use default levels and timeoffsets. See more
    info here https://frost.met.no/concepts2.html#level-offset-filter

    It is important to note that this class does NOT implement all
    of the functionality of the frost API. The frost documentation
    is extensive and worth looking through.

    Read more about data quality codes here
    https://frost.met.no/dataclarifications.html

    Read more about general concepts: https://frost.met.no/concepts2.html

    """
    DATASOURCE = "MET Norway"
    ALLOWED_VARIABLES = MetNorwayVariables
    URL = "https://frost.met.no/"
    POINTS_FROM_GEOM_DEFAULTS = {
        'within_geometry': True,
        'token_json': "~/.frost_token.json",
        'buffer': 0.0
    }

    def __init__(
        self, station_id, name, token_json="~/.frost_token.json",
        metadata=None,
    ):
        """
        Args:
            station_id: id of station
            name: name of station
            token_json: path to file with authentication information
            metadata: optional metadata for the station
        """
        super(MetNorwayPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._token_path = token_json

        # track how long the token is valid
        self._token_expires = None
        self._auth_header = None

        # default UTC time
        self._tzinfo = timezone(timedelta(hours=0))

    @classmethod
    def _get_token(cls, token_json):
        """
        Get token for authorization
        Args:
            token_json: path to json with credentials
        Returns:
            (auth headers, timestamp of expire)
        """
        # read in credentials
        token_json = Path(token_json).expanduser().absolute()
        with open(token_json, "r") as fp:
            obj = json.load(fp)
            _client_id = obj["client_id"]
            _client_secret = obj["client_secret"]

        url = cls.URL + "auth/accessToken"
        params = {
            "client_id": _client_id,
            "client_secret": _client_secret,
            "grant_type": "client_credentials"
        }
        resp = requests.post(url, data=params)
        resp.raise_for_status()
        result = resp.json()
        # get the token
        token = result["access_token"]
        # set the time when it expires
        _token_expires = datetime.now() + timedelta(
            seconds=result["expires_in"]
        )
        return {
            "Authorization": f"Bearer {token}"
        }, _token_expires

    def _token_is_valid(self):
        """
        function to check if token is valid
        """
        if self._token_expires is None:
            return False
        else:
            return datetime.now() >= self._token_expires

    @property
    def auth_header(self):
        """
        Get the auth header and set the expiration time
        """
        # get a new header if we need to
        if self._auth_header is None or not self._token_is_valid():
            token, expires = self._get_token(self._token_path)
            # Save when the token expires
            self._token_expires = expires
            # set auth header
            self._auth_header = token
        return self._auth_header

    @classmethod
    def _get_sources(
        cls, token_json="~/.frost_token.json", ids=None, types="SensorSystem",
        elements=None, geometry=None, validtime=None, name=None,
    ):
        """
        Get metadata for the source entitites defined in the Frost API.
        Use the query parameters to filter the set of sources returned.
        Leave the query parameters blank to select all sources.

        Args:
            token_json: path to json file with credentials
            ids: The Frost API source ID(s) that you want metadata for.
                Enter a comma-separated list to select multiple sources.
                For sources of type SensorSystem or RegionDataset, the source
                ID must be of the form <prefix><int> where <prefix> is SN
                for SensorSystem and TR, NR, GR, or GF for RegionDataset.
                The integer following the prefix may contain wildcards,
                e.g. SN18*7* matches both SN18700 and SN18007.
            types: The type of Frost API source that you want metadata for.
                [SensorSystem, InterpolatedDataset, RegionalDataset]
            elements: If specified, only sources for which observations are
                available for all of these elements may be included in the
                result. Enter a comma-separated list of search filters.
            geometry: Get Frost API sources defined by a specified geometry.
                Geometries are specified as either nearest(POINT(...)) or
                POLYGON(...) using WKT; see the reference section on the
                Geometry Specification for documentation and examples.
                If the nearest() function is specified, the output will
                include the distance in kilometers from the reference point.
            validtime: If specified, only sources that have been, or still are,
                valid/applicable during some part of this interval may be
                included in the result. Specify <date>/<date>, <date>/now,
                <date>, or now, where <date> is of the form YYYY-MM-DD,
                e.g. 2017-03-06. The default is 'now', i.e. only currently
                valid/applicable sources are included.
            name: If specified, only sources whose 'name' attribute matches
                this search filter may be included in the result.
        """
        url = cls.URL + "sources/v0.jsonld"
        if geometry is not None:
            geo_info = str(geometry.iloc[0].geometry)
        else:
            geo_info = None

        params = dict(
            ids=ids, types=types, elements=elements, geometry=geo_info,
            validtime=validtime, name=name
        )
        auth_header, _ = cls._get_token(token_json)
        resp = requests.get(url, params=params, headers=auth_header)
        if resp.status_code == 404:
            if ids:
                raise RuntimeError(f"Could not find metadata for {ids}")
            else:
                # No point were found
                result = None
        else:
            resp.raise_for_status()
            result = resp.json()["data"]
        return result

    def _get_all_metadata(self):
        """
        Get all metadata from the API for one point
        """
        result = self._get_sources(
            ids=[self.id], token_json=self._token_path
        )
        if len(result) != 1:
            raise RuntimeError("No metadata returned")

        return result[0]

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        data = self._get_all_metadata()
        location_data = data["geometry"]["coordinates"]

        return gpd.points_from_xy(
            [location_data[0]],
            [location_data[1]],
        )[0]

    def _get_observations(self, ids, start_date, end_date, variables_names):
        """
        Args:
            ids: list of station ids
            start_date: datetime start date
            end_date: datetime end date
            variables_names: list of element names
        """
        url = self.URL + "observations/v0.jsonld"
        params = {
            "sources": ids,
            "referencetime": f"{start_date.isoformat()}/{end_date.isoformat()}",
            "elements": variables_names,
            # Defaults https://frost.met.no/concepts2.html#level-offset-filter
            "timeoffsets": "default",
            "levels": "default"
        }
        resp = requests.get(url, params=params, headers=self.auth_header)
        # 412 means there was no data found
        if resp.status_code == 412:
            # we could use /observations/availableTimeSeries/v0
            # to check this first
            LOG.debug(f"No data found for {ids}, {variables_names}")
            result = None
        else:
            resp.raise_for_status()
            result = resp.json()["data"]
        return result

    @staticmethod
    def _time_info_to_observation_time(
        reference_time, time_offset, time_resolution, timeseries_id
    ):
        """
        Get the observation time from the time info of an observation
        https://frost.met.no/concepts2.html#relationshipreftime

        Args:
            reference_time: string reference time
            time_offset: string time offset (PT1H)
            time_resolution: string time resolution (PT12H)
        Returns:
            observation_time
        """
        reference_time = pd.to_datetime(reference_time)
        time_offset = pd.to_timedelta(time_offset)
        time_resolution = pd.to_timedelta(time_resolution)
        observation_time = (
            reference_time + time_offset + time_resolution * timeseries_id
        )
        return observation_time

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
        records = []
        for obs in response_data:
            ref_time = obs["referenceTime"]
            # filter to the relevant responses
            relevant_obs = [
                o for o in obs["observations"] if o["elementId"] == sensor.code
            ]
            if len(relevant_obs) == 0:
                # skip no data
                continue
            if len(relevant_obs) > 1:
                raise RuntimeError("This case is not implemented")
            else:
                # this is our winner
                o = relevant_obs[0]
                observation_time = self._time_info_to_observation_time(
                    ref_time, o["timeOffset"], o["timeResolution"],
                    o["timeSeriesId"]
                )
                records.append({
                    "datetime": observation_time,
                    "site": self.id,
                    sensor.name: o["value"],
                    f"{sensor.name}_units": o["unit"],
                    "quality_code": o["qualityCode"]
                })
        # return None for no data
        if not records:
            return None

        # keep the column names
        final_columns += [
            sensor.name, f"{sensor.name}_units", "quality_code"
        ]

        # create df
        sensor_df = pd.DataFrame.from_records(records)
        frequency = pd.infer_freq(pd.DatetimeIndex(sensor_df["datetime"]))
        sensor_df = GeoDataFrame(
            sensor_df, geometry=[self.metadata] * len(sensor_df)
        ).set_index("datetime")

        # resample to the desired duration
        if frequency != resample_duration and resample_duration is not None:
            sensor_df = resample_whole_df(
                sensor_df, sensor,
                interval=resample_duration
            )
            # Overwrite quality code if we resampled it
            sensor_df["quality_code"] = ["resampled"] * len(sensor_df)
            sensor_df = GeoDataFrame(sensor_df, geometry=sensor_df["geometry"])

        # double check utc conversion
        sensor_df = sensor_df.tz_convert(self.desired_tzinfo)

        # set index so joining works
        sensor_df = sensor_df.filter(final_columns)
        sensor_df = sensor_df.loc[pd.notna(sensor_df[sensor.name])]
        return sensor_df

    def _get_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
        desired_duration=None,
    ):
        """
        Args:
            start_date: datetime object for start of data collection period
            end_date: datetime object for end of data collection period
            variables: List of metloom.variables.SensorDescription object
                from self.ALLOWED_VARIABLES
        Returns:
            GeoDataFrame of data, indexed on datetime, site
        """

        df = None
        final_columns = ["geometry", "site"]
        # Get data from the API
        response_data = self._get_observations(
            [self.id], start_date, end_date, [v.code for v in variables]
        )
        if response_data:
            # Parse data for each variable
            for sensor in variables:
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

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data
        """
        return self._get_data(
            start_date, end_date, variables, desired_duration="D"
        )

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        return self._get_data(
            start_date, end_date, variables, desired_duration="h"
        )

    def get_event_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        Get data in original frequency from API
        """
        return self._get_data(
            start_date, end_date, variables, desired_duration=None
        )

    @classmethod
    def points_from_geometry(
        cls,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        **kwargs
    ):
        """
        See docstring for PointData.points_from_geometry

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription
            within_geometry: filter the points to within the shapefile
                instead of just the extents. Default True
            buffer: buffer added to search box
            token_json: Path to the public token for the mesowest api
                        default = "~/.frost_token.json"

        Returns:
            PointDataCollection
        """
        kwargs = cls._add_default_kwargs(kwargs)

        token_json = kwargs['token_json']
        projected_geom = geometry.to_crs("EPSG:4326")
        # buffer the geometry
        buffer = kwargs["buffer"]
        projected_geom = gpd.GeoDataFrame(
            geometry=projected_geom.dissolve().buffer(buffer)
        )
        # Take the outer bounds if we are not within the geometry
        if not kwargs["within_geometry"]:
            projected_geom = gpd.GeoDataFrame(geometry=projected_geom.envelope)

        # Loop over each variable and create a set of points.
        # _get_sources returns only points that have ALL variables passed
        # in, so looping over allows us to not exclude any points
        points_df = pd.DataFrame()
        for v in variables:
            source_info = cls._get_sources(
                token_json=token_json, geometry=projected_geom,
                elements=[v.code]
            )
            if source_info is not None:
                df = pd.DataFrame.from_records(source_info)
            else:
                df = None
            # build our final list
            points_df = append_df(
                points_df, df
            ).drop_duplicates(subset=['id'])

        if len(points_df) > 0:
            gdf = gpd.GeoDataFrame(
                points_df,
                # We don't get elevation back
                geometry=gpd.points_from_xy(
                    [p['coordinates'][0] for p in points_df["geometry"].values],
                    [p['coordinates'][1] for p in points_df["geometry"].values],
                ),
            )
            points = [
                cls(
                    row[0], row[1],
                    metadata=row[2]
                )
                for row in zip(
                    gdf["id"],
                    gdf["name"],
                    gdf["geometry"],
                )
            ]
        else:
            points = []

        # return a points iterator
        return cls.ITERATOR_CLASS(points)

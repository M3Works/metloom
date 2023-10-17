import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import requests

from metloom.dataframe_utils import append_df
from metloom.pointdata.base import PointData
from metloom.variables import SensorDescription, MetNorwayVariables


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


    # TODO: look into data quality flags
    # TODO:
        Important note: If you only specify these 3 things, your request will return all the data that matches this. This can result in many similar timeseries, for example if there are multiple sensors at a station that measure the same thing. It also means you might get data that is lower quality, because the request will return all available data.
        If you want to try to limit the amount of timeseries the request returns it can be useful to use some defaults:
        timeoffsets=default
        levels=default

    # TODO: read concepts
    # TODO: implement scheme for getting observation times based
        on documentation of time calculation

    Concepts: https://frost.met.no/concepts2.html

    """
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
        super(MetNorwayPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._token_path = token_json

        # track how long the token is valid
        self._token_expires = None
        self._auth_header = None

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
        geo_info = str(geometry.iloc[0].geometry)

        params = dict(
            ids=ids, types=types, elements=elements, geometry=geo_info,
            validtime=validtime, name=name
        )
        auth_header, _ = cls._get_token(token_json)
        resp = requests.get(url, params=params, headers=auth_header)
        resp.raise_for_status()
        return resp.json()["data"]

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
            df = pd.DataFrame.from_records(source_info)
            # build our final list
            points_df = append_df(
                points_df, df
            ).drop_duplicates(subset=['id'])

        gdf = gpd.GeoDataFrame(
            points_df,
            # We don't get elevation back
            geometry=gpd.points_from_xy(
                [p['coordinates'][0] for p in points_df["geometry"].values],
                [p['coordinates'][1] for p in points_df["geometry"].values],
                # z=search_df[elev_key],
            ),
        )
        points = [
            cls(
                row[0], row[1],
                # For now, let's not pass in metadata
                # metadata=row[2]
            )
            for row in zip(
                gdf["id"],
                gdf["name"],
                gdf["geometry"],
            )
        ]
        # return a points iterator
        return cls.ITERATOR_CLASS(points)

import requests

from metloom.pointdata.base import PointData


class MetNorwayPointData(PointData):
    """
    Class for the Norway Frost API
    https://frost.met.no/index.html

    Data is provided by MET Norway, see license for details
    https://www.met.no/en/free-meteorological-data

    Element (variable) information can be found here
    https://frost.met.no/elementtable

    Observations/AvailableTimeSeries/ can be used to find out what types elements
    are available for a station or time range - we can use this to filter

    The Sources endpoint returns metadata. It can be used to filter
    based on geometry and variables

    # TODO: look into data quality flags
    # TODO:
        Important note: If you only specify these 3 things, your request will return all the data that matches this. This can result in many similar timeseries, for example if there are multiple sensors at a station that measure the same thing. It also means you might get data that is lower quality, because the request will return all available data.
        If you want to try to limit the amount of timeseries the request returns it can be useful to use some defaults:
        timeoffsets=default
        levels=default

    # TODO: read concepts
    # TODO: make user

    Concepts: https://frost.met.no/concepts2.html

    """

    URL = "https://frost.met.no/"

    def __init__(
        self, station_id, name, token_json="~/.frost_token.json",
        metadata=None,
    ):
        super(MetNorwayPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._token_path = token_json
        # TODO: read in credentials

    def _get_sources(
        self, ids=None, types="SensorSystem", elements=None, geometry=None,
        validtime=None, name=None
    ):
        """
        Args:
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
        url = self.URL + "sources"
        params = dict(
            ids=ids, types=types, elements=elements, geometry=geometry,
            validtime=validtime, name=name
        )
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

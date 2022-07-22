from datetime import datetime
import pandas as pd
import zeep

from metloom.request_utils import no_ssl_verification


class BaseSnotelClient:
    """
    Base snotel client class. Used for interacting with SNOTEL SOAP client.
    This is just a base class and not meant for direct use.

    Example use with extended class::

        MetaDataSnotelClient('TNY:CA:SNOW').get_data()

    """
    URL = "https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
    # map allowed params to API filter params
    PARAMS_MAP = {
        'station_triplet': 'stationTriplet',
        'station_triplets': 'stationTriplets',
        'begin_date': 'beginDate',
        'end_date': 'endDate',
        'max_longitude': 'maxLongitude',
        'min_longitude': 'minLongitude',
        'max_latitude': 'maxLatitude',
        'min_latitude': 'minLatitude',
        'network_cds': 'networkCds',
        "duration": "duration",
        'ordinal': 'ordinal',
        'element_cd': 'elementCd',
        'element_cds': 'elementCds',
        'parameter': 'parameter',
        'height_depth': 'heightDepth',
    }
    SERVICE_NAME = None
    DEFAULT_PARAMS = {}

    def __init__(self, **kwargs):
        self.params = self._get_params(**kwargs)
        if getattr(self, "DURATION", None) is not None:
            extra_params = {"duration": self.DURATION, **self.DEFAULT_PARAMS}
        else:
            extra_params = self.DEFAULT_PARAMS
        # add in default params to self.params, don't replace user entries
        for key, value in extra_params.items():
            if key not in self.params:
                self.params[key] = value

    def _get_params(self, **kwargs):
        """
        map input parameter keys to keys expected by the SOAP client
        """
        params = {}
        for key, value in kwargs.items():
            mapped_key = self.PARAMS_MAP.get(key)
            if mapped_key is None:
                raise ValueError(
                    f"Could not find valid mapped key for {key}"
                )
            if isinstance(value, datetime):
                value = value.date().isoformat()
            params.update({mapped_key: value})
        return params

    @classmethod
    def _make_request(cls, **params):
        """
        Make the request to the SOAP client for the implemented service.
        """
        with no_ssl_verification():
            client = zeep.Client(
                cls.URL,
            )
            service = getattr(client.service, cls.SERVICE_NAME)
            response = service(**params)
        return response

    def get_data(self):
        """
        Make the actual request and return data.
        """
        data = self._make_request(**self.params)
        return data


class MetaDataSnotelClient(BaseSnotelClient):
    """
    Read metadata from the metadata service for a particular station triplet
    """
    SERVICE_NAME = "getStationMetadata"

    def __init__(self, station_triplet: str, **kwargs):
        super(MetaDataSnotelClient, self).__init__(
            station_triplet=station_triplet, **kwargs
        )

    def get_data(self):
        """
        Returns a dictionary of metadata values
        """
        data = self._make_request(**self.params)
        # change ordered dict of values to regular dict
        return dict(data.__values__)


class ElementSnotelClient(BaseSnotelClient):
    """
    Get all station elements for a station triplet. Station triplets
    are descriptions of each sensor on the station

    get_data returns a list of zeep objects. Zeep objects are indexible
    or attributes can be accessed with getattr or ``.``
    """
    SERVICE_NAME = "getStationElements"

    def __init__(self, station_triplet: str, **kwargs):
        super(ElementSnotelClient, self).__init__(
            station_triplet=station_triplet, **kwargs
        )


class PointSearchSnotelClient(BaseSnotelClient):
    """
    Search for stations based on criteria. This search is default logical
    ``AND`` meaning all criteria need to be true.
    get_data returns a list of string station triplets
    """
    SERVICE_NAME = 'getStations'
    DEFAULT_PARAMS = {
        'logicalAnd': 'true',
    }

    def __init__(self, max_latitude: float, min_latitude: float,
                 max_longitude: float, min_longitude: float,
                 network_cds: str, element_cds: str, **kwargs):
        super(PointSearchSnotelClient, self).__init__(
            max_latitude=max_latitude, min_latitude=min_latitude,
            max_longitude=max_longitude, min_longitude=min_longitude,
            network_cds=network_cds, element_cds=element_cds,
            **kwargs
        )


class SeriesSnotelClient(BaseSnotelClient):
    """
    Base extension for services that return timseries data.
    """
    SERVICE_NAME = "getData"
    DURATION = "DAILY"
    DEFAULT_PARAMS = {
        "ordinal": 1,
        "getFlags": "true",
        "alwaysReturnDailyFeb29": "false",
    }

    def __init__(self, begin_date: datetime, end_date: datetime,
                 station_triplet: str, **kwargs):
        super(SeriesSnotelClient, self).__init__(
            begin_date=begin_date, end_date=end_date,
            station_triplets=[station_triplet], **kwargs)

    @staticmethod
    def _parse_data(raw_data):
        """
        Parse the return data to return a consistent format for timeseries
        data
        """
        data = raw_data[0]
        mapped_data = []
        collection_dates = getattr(data, "collectionDates", None)
        if len(data["values"]) == 0:
            return mapped_data
        if collection_dates:
            date_list = [pd.to_datetime(d) for d in collection_dates]
        else:
            date_list = pd.date_range(data["beginDate"], data["endDate"])

        for date_obj, flag, value in zip(date_list, data["flags"], data["values"]):
            if date_obj is not None:
                mapped_data.append({
                    "datetime": date_obj,
                    "flag": flag,
                    "value": float(value) if value is not None else None
                })
        return mapped_data

    def get_data(self, element_cd: str, **extra_params):
        """
        get the timeseires data

        Args:
            element_cd: the variable code from the allowed variable codes
                in the API
            extra_params: kwargs for any extra parameters. These override
                the default parameters
        """
        extra_params.update(element_cd=element_cd)
        mapped_params = self._get_params(**extra_params)
        params = {**self.params, **mapped_params}
        data = self._make_request(**params)
        return self._parse_data(data)


class DailySnotelDataClient(SeriesSnotelClient):
    """
    Class for getting daily data
    """
    pass


class SemiMonthlySnotelClient(SeriesSnotelClient):
    """
    Class for getting semi monthly (snow course) data
    """
    DURATION = "SEMIMONTHLY"


class HourlySnotelDataClient(SeriesSnotelClient):
    """
    Class for getting hourly data
    """
    DURATION = None
    SERVICE_NAME = "getHourlyData"
    DEFAULT_PARAMS = {
        "ordinal": 1,
    }

    @staticmethod
    def _parse_data(raw_data):
        """
        Clean the hourly data to be consistent with timeseries results
        """
        data = raw_data[0]
        mapped_data = []
        for row in data["values"]:
            value = row["value"]
            mapped_data.append({
                "datetime": pd.to_datetime(row["dateTime"]),
                "flag": row["flag"],
                "value": float(value) if value is not None else None
            })
        return mapped_data

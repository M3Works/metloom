from datetime import datetime
from typing import List
import pandas as pd
import zeep

from metloom.request_utils import no_ssl_verification


class BaseSnotelClient:

    URL = "https://www.wcc.nrcs.usda.gov/awdbWebService/services?wsdl"
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
        with no_ssl_verification():
            client = zeep.Client(
                cls.URL,
            )
            service = getattr(client.service, cls.SERVICE_NAME)
            response = service(**params)
        return response

    def get_data(self):
        data = self._make_request(**self.params)
        return data


class MetaDataSnotelClient(BaseSnotelClient):
    SERVICE_NAME = "getStationMetadata"

    def __init__(self, station_triplet: str, **kwargs):
        super(MetaDataSnotelClient, self).__init__(
            station_triplet=station_triplet, **kwargs
        )

    def get_data(self):
        data = self._make_request(**self.params)
        # change ordered dict of values to regular dict
        return dict(data.__values__)


class ElementSnotelClient(BaseSnotelClient):
    SERVICE_NAME = "getStationElements"

    def __init__(self, station_triplet: str, **kwargs):
        super(ElementSnotelClient, self).__init__(
            station_triplet=station_triplet, **kwargs
        )


class PointSearchSnotelClient(BaseSnotelClient):
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
        data = raw_data[0]
        collection_dates = getattr(data, "collectionDates", None)
        if collection_dates:
            date_list = [pd.to_datetime(d) for d in collection_dates]
        else:
            date_list = pd.date_range(data["beginDate"], data["endDate"])

        mapped_data = []
        for date_obj, flag, value in zip(date_list, data["flags"], data["values"]):
            mapped_data.append({
                "datetime": date_obj,
                "flag": flag,
                "value": float(value) if value is not None else None
            })
        return mapped_data

    def get_data(self, element_cd: str, **extra_params):
        extra_params.update(element_cd=element_cd)
        mapped_params = self._get_params(**extra_params)
        params = {**mapped_params, **self.params}
        data = self._make_request(**params)
        return self._parse_data(data)


class DailySnotelDataClient(SeriesSnotelClient):
    pass


class SemiMonthlySnotelClient(SeriesSnotelClient):
    DURATION = "SEMIMONTHLY"


class HourlySnotelDataClient(SeriesSnotelClient):
    DURATION = None
    SERVICE_NAME = "getHourlyData"
    DEFAULT_PARAMS = {
        "ordinal": 1,
    }

    @staticmethod
    def _parse_data(raw_data):
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

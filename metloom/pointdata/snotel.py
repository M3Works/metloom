from datetime import datetime, timezone, timedelta
from typing import List

import logging
import geopandas as gpd
import pandas as pd
from climata import snotel
from climata.base import FilterOpt, DateOpt, as_list
from wq.io.exceptions import NoData

from .base import PointData
from ..variables import SnotelVariables, VariableBase, SensorDescription
from ..dataframe_utils import join_df


LOG = logging.getLogger("metloom.pointdata.snotel")


class LoomStationIO(snotel.StationIO):
    """
    Extend climata.snotel.StationIO to allow filtering based on network
    and longitude
    """

    max_longitude = FilterOpt(url_param="maxLongitude")
    min_longitude = FilterOpt(url_param="minLongitude")
    networks = FilterOpt(url_param="networkCds")


class MonthlyDataIO(snotel.SnotelIO):
    """
    Extend climata.snotel.SnotelIO for sampling SEMIMONTHLY data.
    This allows sampling of snowcourses
    """

    data_function = "getData"

    # Applicable WebserviceLoader default options
    station = FilterOpt(required=True, url_param="stationTriplets")
    parameter = FilterOpt(required=True, url_param="elementCd")
    start_date = DateOpt(required=True, url_param="beginDate")
    end_date = DateOpt(required=True, url_param="endDate")

    default_params = {
        "ordinal": 1,
        "duration": "SEMIMONTHLY",
        "getFlags": "true",
        "alwaysReturnDailyFeb29": "false",
    }

    def parse(self):
        data = self.data[0]
        if not data or "values" not in data:
            raise NoData
        dates = as_list(data["collectionDates"])
        vals = as_list(data["values"])
        flags = as_list(data["flags"])

        self.data = [
            {
                "date": datetime.strptime(date_val, "%Y-%m-%d").date(),
                "value": val,
                "flag": flag,
            }
            for date_val, val, flag in zip(dates, vals, flags)
            if date_val is not None
        ]


class StationMonthlyDataIO(snotel.StationDataIO):
    """
    Extension of climata.snotel.StationDataIO to allow snowcourse sampling
    """

    inner_io_class = MonthlyDataIO
    duration = "SEMIMONTHLY"


class SnotelPointData(PointData):
    """
    Implement PointData methods for SNOTEL data source
    API documentation here:
    https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService/webServiceReference/
    Website has variable and network codes
    Possible testing resource:
    https://wcc.sc.egov.usda.gov/awdbWebService/webservice/testwebservice.jsf?webserviceName=/awdbWebService
    """

    ALLOWED_VARIABLES = SnotelVariables

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(SnotelPointData, self).__init__(station_id, name, metadata=metadata)
        self._raw_metadata = None
        self._tzinfo = None

    def _snotel_response_to_df(
        self,
        response: snotel.StationDataIO,
        variables: List[VariableBase],
        date_key="date",
    ):
        """
        Convert the response from climata.snotel classes into
        Args:
            response: climata.snotel data response
            variables: list of variables to extract
            date_key: which key the response uses to store date(time)s
        """
        df = None
        # TODO: possible DRY oportunity here too
        final_columns = ["geometry", "site", "measurementDate"]
        for param in response:
            for variable in variables:
                if param.elementcd == variable.code:
                    data = param.data
                    transformed = [
                        {
                            "datetime": getattr(v, date_key),
                            "site": self.id,
                            "measurementDate": getattr(v, date_key),
                            variable.name: v.value,
                            f"{variable.name}_units": param.storedunitcd,
                        }
                        for v in data
                    ]
                    final_columns += [variable.name, f"{variable.name}_units"]
                    sensor_df = gpd.GeoDataFrame.from_dict(
                        transformed, geometry=[self.metadata] * len(transformed)
                    )
                    # TODO: possibly an opportunity for DRY here (see CDEC)
                    sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
                    sensor_df["measurementDate"] = pd.to_datetime(
                        sensor_df["measurementDate"]
                    )
                    sensor_df["datetime"] = sensor_df["datetime"].apply(
                        self._handle_df_tz
                    )
                    sensor_df["measurementDate"] = sensor_df["measurementDate"].apply(
                        self._handle_df_tz
                    )
                    # set index so joining works
                    sensor_df.set_index("datetime", inplace=True)
                    sensor_df = sensor_df.filter(final_columns)
                    df = join_df(df, sensor_df)

        if df is not None and len(df.index) > 0:
            df.reset_index(inplace=True)
            df.set_index(keys=["datetime", "site"], inplace=True)
            df.index.set_names(["datetime", "site"], inplace=True)
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
        params = snotel.StationDailyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
        )
        return self._snotel_response_to_df(params, variables)

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        params = snotel.StationHourlyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
        )
        return self._snotel_response_to_df(params, variables, date_key="datetime")

    def get_snow_course_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_snow_course_data
        """
        params = StationMonthlyDataIO(
            station=self.id,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
        )
        return self._snotel_response_to_df(params, variables, date_key="date")

    def _get_all_metadata(self):
        """
        Set _raw_metadata once using climata.snotel
        """
        if self._raw_metadata is None:
            self._raw_metadata = snotel.StationMetaIO(station=self.id)
        return self._raw_metadata

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        all_metadata = self._get_all_metadata()
        data = all_metadata[0]
        return gpd.points_from_xy(
            [data.longitude], [data.latitude], z=[data.elevation]
        )[0]

    def _get_tzinfo(self):
        """
        Return timezone info that pandas can use from the raw_metadata
        """
        all_metadata = self._get_all_metadata()
        metadata = all_metadata[0]
        # Snow courses might not have a timezone attached
        if hasattr(metadata, "stationdatatimezone"):
            tz_hours = metadata.stationdatatimezone
        else:
            LOG.error(f"Could not find timezone info for {self.id} ({self.name})")
            tz_hours = 0
        return timezone(timedelta(hours=tz_hours))

    @property
    def tzinfo(self):
        if self._tzinfo is None:
            self._tzinfo = self._get_tzinfo()
        return self._tzinfo

    @classmethod
    def points_from_geometry(
        cls,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        snow_courses=False,
    ):
        """
        See docstring for PointData.points_from_geometry
        """
        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        # TODO: network may need to change to get streamflow
        network = "SNOW" if snow_courses else "SNTL"
        df = None
        for variable in variables:
            # this search is default AND on all parameters
            # so search for each variable seperately
            response = LoomStationIO(
                max_latitude=bounds["maxy"],
                min_latitude=bounds["miny"],
                max_longitude=bounds["maxx"],
                min_longitude=bounds["minx"],
                networks=network,  # only snotel, not
                parameter=variable.code,
                # ordinals=  # TODO: what are ordinals?
            )
            if len(response.data) > 0:
                result_df = pd.DataFrame.from_records(response.data)
                result_df["index_id"] = result_df["stationTriplet"]
                df = join_df(df, result_df, how="outer")
        # TODO: possible DRYing out here

        if df is None:
            return cls.ITERATOR_CLASS([])

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df["longitude"], df["latitude"], z=df["elevation"]
            ),
        )
        filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf["stationTriplet"],
                filtered_gdf["name"],
                filtered_gdf["geometry"],
            )
        ]
        return cls.ITERATOR_CLASS(points)

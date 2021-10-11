from datetime import datetime, timezone, timedelta
from typing import List, Dict
import logging
import geopandas as gpd
import pandas as pd
from functools import reduce


from .base import PointData
from ..variables import SnotelVariables, SensorDescription
from ..dataframe_utils import join_df, append_df

from .snotel_client import (
    DailySnotelDataClient, MetaDataSnotelClient, HourlySnotelDataClient,
    SemiMonthlySnotelClient, PointSearchSnotelClient, SeriesSnotelClient,
    ElementSnotelClient
)

LOG = logging.getLogger("metloom.pointdata.snotel")


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
        super(SnotelPointData, self).__init__(
            station_id, name, metadata=metadata
        )
        self._raw_metadata = None
        self._raw_elements = None
        self._tzinfo = None

    def _snotel_response_to_df(self, result_map: Dict[SensorDescription, List[dict]],
                               duration: str):
        """
        Convert the response from climata.snotel classes into
        Args:
           result_map: map of the sensors to the list of API results
        """
        df = None
        # TODO: possible DRY opportunity here too
        final_columns = ["geometry", "site", "measurementDate"]
        for variable, data in result_map.items():
            transformed = [
                {
                    "datetime": row["datetime"],
                    "site": self.id,
                    "measurementDate": row["datetime"],
                    variable.name: row["value"],
                    f"{variable.name}_units": self._get_units(variable, duration),
                }
                for row in data
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

    def _fetch_data_for_variables(self, client: SeriesSnotelClient,
                                  variables: List[SensorDescription],
                                  duration: str):
        result_map = {}
        for variable in variables:
            data = client.get_data(element_cd=variable.code)
            if len(data) > 0:
                result_map[variable] = data
            else:
                LOG.warning(f"No {variable.name} found for {self.name}")
        return self._snotel_response_to_df(result_map, duration)

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_daily_data
        """
        client = DailySnotelDataClient(
            station_triplet=self.id,
            begin_date=start_date,
            end_date=end_date,
        )
        return self._fetch_data_for_variables(client, variables, client.DURATION)

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_hourly_data
        """
        client = HourlySnotelDataClient(
            station_triplet=self.id,
            begin_date=start_date,
            end_date=end_date,
        )
        return self._fetch_data_for_variables(client, variables, "HOURLY")

    def get_snow_course_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription],
    ):
        """
        See docstring for PointData.get_snow_course_data
        """
        client = SemiMonthlySnotelClient(
            station_triplet=self.id,
            begin_date=start_date,
            end_date=end_date,
        )
        return self._fetch_data_for_variables(client, variables, client.DURATION)

    def _get_all_metadata(self):
        """
        Set _raw_metadata once using Snotel API
        """
        if self._raw_metadata is None:
            client = MetaDataSnotelClient(station_triplet=self.id)
            self._raw_metadata = client.get_data()
        return self._raw_metadata

    def _get_all_elements(self):
        """
        Set _raw_metadata once using Snotel API
        """
        if self._raw_elements is None:
            client = ElementSnotelClient(station_triplet=self.id)
            self._raw_elements = client.get_data()
        return self._raw_elements

    def _get_units(self, variable: SensorDescription, duration: str):
        units = None
        for meta in self._get_all_elements():
            if meta["elementCd"] == variable.code and meta["duration"] == duration:
                units = meta["storedUnitCd"]
                break
        if units is None:
            raise ValueError(f"Could not find units for {variable}")
        return units

    def _get_metadata(self):
        """
        See docstring for PointData._get_metadata
        """
        all_metadata = self._get_all_metadata()
        if isinstance(all_metadata, list):
            data = all_metadata[0]
        else:
            data = all_metadata
        return gpd.points_from_xy(
            [data["longitude"]], [data["latitude"]], z=[data["elevation"]]
        )[0]

    def _get_tzinfo(self):
        """
        Return timezone info that pandas can use from the raw_metadata
        """
        metadata = self._get_all_metadata()
        # Snow courses might not have a timezone attached
        tz_hours = metadata.get("stationDataTimeZone")
        if tz_hours is None:
            LOG.error(f"Could not find timezone info for {self.id} ({self.name})")
            tz_hours = 0
        else:
            tz_hours = float(tz_hours)
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
        point_codes = []
        for variable in variables:
            # this search is default AND on all parameters
            # so search for each variable seperately
            response = PointSearchSnotelClient(
                max_latitude=bounds["maxy"],
                min_latitude=bounds["miny"],
                max_longitude=bounds["maxx"],
                min_longitude=bounds["minx"],
                network_cds=network,
                element_cds=variable.code
                # ordinals=  # TODO: what are ordinals?
            ).get_data()
            if len(response) > 0:
                point_codes += response

        # no duplicate codes
        point_codes = list(set(point_codes))
        dfs = [
            pd.DataFrame.from_records(
                [MetaDataSnotelClient(station_triplet=code).get_data()]
            ).set_index("stationTriplet") for code in point_codes
        ]

        if len(dfs) > 0:
            df = reduce(lambda a, b: append_df(a, b), dfs)
        else:
            return cls.ITERATOR_CLASS([])

        df.reset_index(inplace=True)
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

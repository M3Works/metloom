from datetime import datetime, timezone, timedelta
from typing import List, Dict
import logging
import geopandas as gpd
import pandas as pd
from functools import reduce

from .base import PointData
from ..variables import SnotelVariables, SensorDescription
from ..dataframe_utils import append_df, merge_df

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
    DATASOURCE = "NRCS"

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
                               duration: str, include_measurement_date=False):
        """
        Convert the response from climata.snotel classes into
        Args:
        result_map: map of the sensors to the list of API results
        duration: string representation of the duration tag for the
            API (i.e. HOURLY)
        include_measurement_date: boolean for including the
            'measurementDate' column in the resulting dataframe. This column
            is only relevant for snow courses

        """
        df = None
        # TODO: possible DRY opportunity here too
        final_columns = ["geometry", "site"]
        if include_measurement_date:
            final_columns += ["measurementDate"]

        for variable, data in result_map.items():
            transformed = []
            for row in data:
                row_obj = {
                    "datetime": row["datetime"],
                    "site": self.id,
                    variable.name: row["value"],
                    f"{variable.name}_units": self._get_units(variable, duration),
                }
                if include_measurement_date:
                    row_obj["measurementDate"] = row["datetime"]
                transformed.append(row_obj)

            final_columns += [variable.name, f"{variable.name}_units"]
            sensor_df = gpd.GeoDataFrame.from_dict(
                transformed, geometry=[self.metadata] * len(transformed)
            )
            # TODO: possibly an opportunity for DRY here (see CDEC)
            sensor_df["datetime"] = pd.to_datetime(sensor_df["datetime"])
            sensor_df["datetime"] = sensor_df["datetime"].apply(
                self._handle_df_tz
            )
            if include_measurement_date:
                sensor_df["measurementDate"] = pd.to_datetime(
                    sensor_df["measurementDate"]
                )
                sensor_df["measurementDate"] = sensor_df["measurementDate"].apply(
                    self._handle_df_tz
                )
            # set index so joining works
            sensor_df.set_index("datetime", inplace=True)
            sensor_df = sensor_df.filter(final_columns)
            # filter to rows that have value
            sensor_df = sensor_df.loc[pd.notna(sensor_df[variable.name])]
            df = merge_df(df, sensor_df)

        if df is not None and len(df.index) > 0:
            df["datasource"] = [self.DATASOURCE] * len(df.index)
            df.reset_index(inplace=True)
            df.set_index(keys=["datetime", "site"], inplace=True)
            df.index.set_names(["datetime", "site"], inplace=True)
        self.validate_sensor_df(df)
        return df

    def _fetch_data_for_variables(self, client: SeriesSnotelClient,
                                  variables: List[SensorDescription],
                                  duration: str,
                                  include_measurement_date=False,
                                  extra_params=None
                                  ):
        result_map = {}
        extra_params = extra_params or {}
        for variable in variables:
            # need to add extra_params for ground temp call, this may not be the
            # best logic
            if 'GROUND' in variable.name or 'SOIL' in variable.name:
                params = extra_params[variable.name]
            else:
                params = {}
            data = client.get_data(element_cd=variable.code, **params)
            if len(data) > 0:
                result_map[variable] = data
            else:
                LOG.warning(f"No {variable.name} found for {self.name}")
        return self._snotel_response_to_df(
            result_map, duration, include_measurement_date=include_measurement_date
        )

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
        extra_params = self._add_fixed_params(variables)
        return self._fetch_data_for_variables(client, variables,
                                              client.DURATION,
                                              extra_params=extra_params)

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: List[SensorDescription]
    ):
        """
        See docstring for PointData.get_hourly_data
        """

        client = HourlySnotelDataClient(
            station_triplet=self.id,
            begin_date=start_date,
            end_date=end_date,
        )
        extra_params = self._add_fixed_params(variables)
        return self._fetch_data_for_variables(
            client, variables, "HOURLY", extra_params=extra_params
        )

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
        return self._fetch_data_for_variables(
            client, variables, client.DURATION, include_measurement_date=True
        )

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

    def _add_fixed_params(self, variables):
        """
        Get additional necessary fixed arguments for sensors that need heightDepth
        params (soil moisture and soil temp)
        """
        # TODO: this could be refactored into properties of the variable
        extra_params_map = {
            self.ALLOWED_VARIABLES.TEMPGROUND2IN: {
                'height_depth': {"value": -2, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.TEMPGROUND4IN: {
                'height_depth': {"value": -4, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.TEMPGROUND8IN: {
                'height_depth': {"value": -8, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.TEMPGROUND20IN: {
                'height_depth': {"value": -20, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.SOILMOISTURE2IN: {
                'height_depth': {"value": -2, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.SOILMOISTURE4IN: {
                'height_depth': {"value": -4, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.SOILMOISTURE8IN: {
                'height_depth': {"value": -8, "unitCd": "in"}
            },
            self.ALLOWED_VARIABLES.SOILMOISTURE20IN: {
                'height_depth': {"value": -20, "unitCd": "in"}
            },
        }

        extra_params = {}
        for variable in variables:
            result = extra_params_map.get(variable)
            if result:
                extra_params[variable.name] = result

        return extra_params or None

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
        **kwargs
    ):
        """
        See docstring for PointData.points_from_geometry

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription
            snow_courses: boolean for including only snowcourse data or no
                snowcourse data
            within_geometry: filter the points to within the shapefile
                instead of just the extents. Default True
            buffer: buffer added to search box
        Returns:
            PointDataCollection
        """
        # assign defaults
        kwargs = cls._add_default_kwargs(kwargs)

        projected_geom = geometry.to_crs(4326)
        bounds = projected_geom.bounds.iloc[0]
        # TODO: network may need to change to get streamflow
        network = "SNOW" if kwargs['snow_courses'] else [
            "SNTL", "USGS", "BOR", "COOP", "SNTLT"
        ]
        point_codes = []
        buffer = kwargs["buffer"]
        search_kwargs = {
            "max_latitude": bounds["maxy"] + buffer,
            "min_latitude": bounds["miny"] - buffer,
            "max_longitude": bounds["maxx"] + buffer,
            "min_longitude": bounds["minx"] - buffer,
            "network_cds": network,
        }
        for variable in variables:
            # this search is default AND on all parameters
            # so search for each variable seperately
            response = PointSearchSnotelClient(
                element_cds=variable.code,
                **search_kwargs
                # ordinals=  # TODO: what are ordinals?
                # ordinals are NRCS descriptors for parameters that may have multiple
                # measurements, such as two pillows at a site, or two measurements of
                # SWE from the same pillow. Fairly comfortable with the idea that we
                # can assume ordinal=1
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
        if kwargs['within_geometry']:
            filtered_gdf = gdf[gdf.within(projected_geom.iloc[0]["geometry"])]
        else:
            filtered_gdf = gdf
        points = [
            cls(row[0], row[1], metadata=row[2])
            for row in zip(
                filtered_gdf["stationTriplet"],
                filtered_gdf["name"],
                filtered_gdf["geometry"],
            )
        ]
        return cls.ITERATOR_CLASS(points)

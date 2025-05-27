import os
from typing import Union
from datetime import date, datetime
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import logging

from .base import PointData, PointDataCollection
from .. import arm_utils
from ..dataframe_utils import shp_to_box
from ..variables import SAILStationVariables, SensorDescription
from ..dataframe_utils import resample_series

LOG = logging.getLogger("metloom.pointdata.sail")


class SAILPointData(PointData):
    """
    https://adc.arm.gov/discovery/#/results/site_code::guc
    """

    ALLOWED_VARIABLES = SAILStationVariables
    DATASOURCE = "SAIL"

    def __init__(
        self,
        station_id: str,
        metadata: dict = None,
        cache: Union[str, Path] = Path(".cache"),
        token_json: Union[str, Path] = Path("~/.arm_token.json"),
    ):
        assert station_id.upper() in ("GUC:M1", "GUC:S1", "GUC:S2", "GUC:S3", "GUC:S4"), (
            f"Invalid station_id: {station_id}"
        )

        super().__init__(
            station_id=station_id.upper(),
            name="Surface Atmosphere Integrated Field Laboratory (SAIL)",
            metadata=metadata,
        )
        self._cache = cache
        self._token_json = Path(token_json).expanduser() if token_json else None

        site = station_id.split(":")
        self._site = site[0].upper()
        self._facility_code = site[1].upper()

        # ARM data requires a user id and access token to download, these must be
        # provided in environment variables
        if (os.getenv("M3W_ARM_USER_ID", None) is None) or (os.getenv("M3W_ARM_ACCESS_TOKEN", None) is None):
            raise ValueError(
                "ARM data requires a user id and access token to download, "
                "these must be provided in environment variables: M3W_ARM_USER_ID and M3W_ARM_ACCESS_TOKEN"
            )

    def get_daily_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: list[SensorDescription],
    ):
        self._check_start_end_dates(start_date, end_date)
        return self._download_sail_data(start_date, end_date, variables, interval="D")

    def get_hourly_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: list[SensorDescription],
    ):
        self._check_start_end_dates(start_date, end_date)
        return self._download_sail_data(start_date, end_date, variables, interval="h")

    def _download_sail_data(
        self,
        start_date: datetime,
        end_date: datetime,
        variables: list[SensorDescription],
        interval: str,
    ) -> pd.DataFrame:
        """
        The ARM data is stored in a series of files based on the sensors at the location.

        This function will download the data for the specified variables and
        return a dataframe with the data. If the files already exist, they will not download
        again.

        NOTE: arm_utils.get_station_data function returns hourly data.
        """
        assert isinstance(variables, list), "variables must be a list of SensorDescription objects"

        columns = []
        for variable in variables:
            sta_site = variable.extra["site"].upper()
            sta_facility_code = variable.extra["facility_code"].upper()
            if not hasattr(self.ALLOWED_VARIABLES, variable.name):
                raise ValueError(f"Variable {variable} is not allowed. Allowed variables are: {self.ALLOWED_VARIABLES}")

            if sta_site != self._site or sta_facility_code != self._facility_code:
                raise ValueError(
                    (
                        f"Variable {variable.code} is not defined for the SAIL site "
                        f"({self._site}:{self._facility_code}), but {sta_site}:{sta_facility_code} provided."
                    )
                )

            df = arm_utils.get_station_data(
                site=variable.extra["site"],
                measurement=variable.extra["measurement"],
                facility_code=variable.extra["facility_code"],
                data_level=variable.extra["data_level"],
                start=start_date,
                end=end_date,
                variables=[variable.code],
                destination=self._cache,
                token_json=self._token_json,
            )
            if df is not None:
                columns.append(pd.Series(resample_series(df[variable.code], variable, interval), name=variable.name))
                units = variable.extra.get("units", None)
                if units is not None:
                    columns.append(pd.Series(units, index=columns[-1].index, name=f"{variable.name}_units"))

        if columns:
            df = pd.concat(columns, axis="columns")
            df["site"] = f"{self._site}:{self._facility_code}"
            df["datasource"] = "ARM"
            df.reset_index(inplace=True)
            df = df.set_index(["datetime", "site"])
            return df
        else:
            LOG.error(
                f"No data found for the specified variables: {', '.join(v.name for v in variables)}.\n"
                f"Please check the variable names and the date range."
            )
        return pd.DataFrame()

    @classmethod
    def points_from_geometry(
        cls,
        geometry: gpd.GeoDataFrame,
        variables: list[SensorDescription],
        snow_courses=None,
        within_geometry=True,
        buffer=0.0,
    ):
        if snow_courses is not None:
            LOG.warning("The snow_courses argument is not used in SAILPointData.points_from_geometry")

        # get geometry object to use for searching within
        boundary = geometry.to_crs(4326) if within_geometry else shp_to_box(geometry).to_crs(4326)
        if buffer > 0:
            boundary = boundary.to_crs(4326).buffer(buffer)

        # get the geometry of the points to check
        stations = list()
        for variable in variables:
            station_id = f"{variable.extra['site']}:{variable.extra['facility_code']}"
            lat, lon, _ = SAILPointData.get_location(station_id, variable)
            stations.append(Point(lon, lat))
        stations = gpd.GeoSeries(stations, crs="EPSG:4326")
        indices = stations[stations.within(boundary)].index.to_list()

        points = [
            SAILPointData(station_id=f"{variables[idx].extra['site']}:{variables[idx].extra['facility_code']}")
            for idx in indices
        ]
        return PointDataCollection(points)

    def get_snow_course_data(
        self,
        start_date: date,
        end_date: date,
        variables: list[SensorDescription],
    ):
        raise NotImplementedError("SAILPointData.get_snow_course_data not implemented")

    def _check_start_end_dates(self, start_date: date, end_date: date):
        """
        Check that the start and end dates are valid
        """
        # get the start and end dates to be date objects for comparison
        start = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
        end = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
        start = start.date() if hasattr(start, "date") else start
        end = end.date() if hasattr(end, "date") else end

        # check that the start and end dates are valid
        if start > end:
            raise ValueError("Start date must be before end date")
        if start < date(2021, 9, 1):
            raise ValueError(f"Start date, {start}, must be after 2021-09-01, the first date of data available")
        if end > date(2023, 6, 16):
            raise ValueError(f"End date, {end}, must be before 2023-06-16, the last date of data available")

    @staticmethod
    def get_location(station_id: str, variable: SensorDescription = None) -> tuple[float, float, float]:
        """
        Get the location of the site and facility code.

        The Gunnison SAIL site has 3 supplemental sites (S1, S2, S3, S4) and one main site (M1). The S4 site
        is atmospheric measurements made with a teathered balloon, thus the location is not constant and
        it is excluded from the hard-coded locations.

        Returns a tuple of (latitude, longitude, elevation [m])
        https://www.arm.gov/capabilities/observatories/guc/locations
        """

        if station_id == "GUC:M1":
            LOG.debug(f"Using known GUC M1 location for {station_id}")
            return (38.956158, -106.987856, 2886.0 * 3.28084)

        elif station_id == "GUC:S1":
            LOG.debug(f"Using known GUC S1 location for {station_id}")
            return (38.956158, -106.987856, 2886.0 * 3.28084)

        elif station_id == "GUC:S2":
            LOG.debug(f"Using known GUC S2 location for {station_id}")
            return (38.898361, -106.94314, 3137.0 * 3.28084)

        elif station_id == "GUC:S3":
            LOG.debug(f"Using known GUC S3 location for {station_id}")
            return (38.941556, -106.973128, 2857.0 * 3.28084)

        elif station_id == "GUC:S4":
            LOG.debug(f"Using known GUC S4 location for {station_id}")
            return (38.922019, -106.9509, 2764.0 * 3.28084)
        else:
            LOG.warning(f"Unexpected site information, attmpting to retrieve location for {station_id}")
            if variable is None:
                raise ValueError("Variable must be provided to get location")
            loc = arm_utils.get_station_location(
                site=variable.extra["site"],
                measurement=variable.extra["measurement"],
                facility_code=variable.extra["facility_code"],
                data_level=variable.extra["data_level"],
            )
            return loc

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
        station_id: str = "GUC",
        metadata: dict = None,
        cache: Union[str, Path] = Path(".cache"),
        token_json: Union[str, Path] = Path("~/.arm_token.json"),
    ):
        super().__init__(
            station_id=station_id,
            name="Surface Atmosphere Integrated Field Laboratory (SAIL)",
            metadata=metadata,
        )
        # The SAIL data is specific to the GUC site in Gunnison, CO
        self._sites = ("GUC",)
        self._cache = cache
        self._token_json = Path(token_json).expanduser() if token_json else None

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
        return self._download_sail_data(
            start_date,
            end_date,
            variables,
            interval="D",
        )

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
            if not hasattr(self.ALLOWED_VARIABLES, variable.name):
                raise ValueError(f"Variable {variable} is not allowed. Allowed variables are: {self.ALLOWED_VARIABLES}")

            sites = {s.upper() for s in self._sites}
            if variable.extra["site"].upper() not in sites:
                raise ValueError(
                    f"Variable {variable} is not from a SAIL site ({variable.extra['site']}). "
                    f"Allowed site(s) are: {', '.join(sites)}"
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
            return pd.concat(columns, axis="columns")
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

        if within_geometry:
            print(geometry)
        # get geometry object to use for searching within
        boundary = geometry.to_crs(4326) if within_geometry else shp_to_box(geometry)
        if buffer > 0:
            boundary = boundary.buffer(buffer)

        # get the geometry of the points to check
        stations = list()
        for variable in variables:
            lat, lon, _ = SAILPointData.get_location(variable)
            stations.append(Point(lon, lat))
        stations = gpd.GeoSeries(stations, crs="EPSG:4326")
        indices = stations[stations.within(boundary)].index.to_list()

        points = [SAILPointData(station_id=variables[idx].extra["site"]) for idx in indices]
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
    def get_location(variable: SensorDescription) -> tuple[float, float, float]:
        """
        Get the location of the site and facility code.

        The Gunnison SAIL site has 3 supplemental sites (S2, S3, S4) and one main site (M1). The S4 site
        is atmospheric measurements made with a teathered balloon, thus the location is not constant and
        it is excluded from the hard-coded locations.

        Returns a tuple of (latitude, longitude, elevation [m])
        """
        site = variable.extra["site"].upper()
        facility_code = variable.extra["facility_code"].upper()
        if (site, facility_code) == ("GUC", "M1"):
            LOG.debug(f"Using known GUC M1 location for {site} {facility_code}")
            # m1 = arm_utils.get_station_location(
            #     site="GUC",
            #     measurement="wbpluvio2",
            #     facility_code="M1",
            #     data_level="a1",
            # )
            # print('GUC:M1', m1)
            return (38.956158, -106.987854, 2886.0)

        elif (site, facility_code) == ("GUC", "S2"):
            LOG.debug(f"Using known GUC S2 location for {site} {facility_code}")
            # s2 = arm_utils.get_station_location(
            #     site="GUC",
            #     measurement="ld",
            #     facility_code="S2",
            #     data_level="b1",
            # )
            # print("GUC:S2", s2)
            return (38.89836, -106.94314, 3137.0)
        elif (site, facility_code) == ("GUC", "S3"):
            LOG.debug(f"Using known GUC S3 location for {site} {facility_code}")
            # s3 = arm_utils.get_station_location(
            #     site="GUC",
            #     measurement="sebs",
            #     facility_code="S3",
            #     data_level="b1",)
            # print('GUC:S3', s3)
            return (38.941555, -106.97313, 2857.0)
        else:
            LOG.warning(f"Unepected site information, attmpting to retrieve location for {site} {facility_code}")
            loc = arm_utils.get_station_location(
                site=site,
                measurement=variable.extra["measurement"],
                facility_code=facility_code,
                data_level=variable.extra["data_level"],
            )
            return loc

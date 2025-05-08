import os
from datetime import date
from typing import List

import geopandas as gpd
import pandas as pd
import logging

from .base import PointData
from .. import arm_utils
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
    ):
        # The SAIL data seems specific to the GUC site, but just in case
        # we want to add more sites in the future, we will keep this as a tuple
        # of sites
        self._sites = ("GUC",)

        # ARM data requires a user id and access token to download, these must be
        # provided in environment variables
        if (os.getenv("M3W_ARM_USER_ID", None) is None) or (os.getenv("M3W_ARM_ACCESS_TOKEN", None) is None):
            raise ValueError(
                "ARM data requires a user id and access token to download, "
                "these must be provided in environment variables: M3W_ARM_USER_ID and M3W_ARM_ACCESS_TOKEN"
            )

    def get_daily_data(
        self,
        start_date: date,
        end_date: date,
        variables: List[SensorDescription],
    ):
        self._check_start_end_dates(start_date, end_date)
        return self._download_sail_raw_data(start_date, end_date, variables, interval="D")

    def get_hourly_data(
        self,
        start_date: date,
        end_date: date,
        variables: List[SensorDescription],
    ):
        self._check_start_end_dates(start_date, end_date)
        return self._download_sail_raw_data(start_date, end_date, variables, interval="h")

    def _download_sail_raw_data(
        self,
        start_date: date,
        end_date: date,
        variables: List[SensorDescription],
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

    def points_from_geometry(
        self,
        geometry: gpd.GeoDataFrame,
        variables: List[SensorDescription],
        snow_courses=False,
        within_geometry=True,
        buffer=0.0,
    ):
        raise NotImplementedError("SAILPointData.points_from_geometry not implemented")

    def get_snow_course_data(
        self,
        start_date: date,
        end_date: date,
        variables: List[SensorDescription],
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

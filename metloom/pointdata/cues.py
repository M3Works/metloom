"""
A reader for the Mammoth CUES site
https://snow.ucsb.edu/index.php/description/

"""
from datetime import datetime
from io import StringIO
from typing import List

import geopandas as gpd
import pandas as pd
import requests

from metloom.pointdata import PointData
from metloom.variables import CuesLevel1Variables, SensorDescription


class CuesLevel1(PointData):
    """
    Implement PointData methods for CUES level 1 data
    https://snow.ucsb.edu/index.php/description/
    https://snow.ucsb.edu/index.php/query-db/

    """
    ALLOWED_VARIABLES = CuesLevel1Variables
    URL = "https://snow.ucsb.edu/index.php/query-db/"
    DATASOURCE = "UCSB CUES"

    def __init__(self, station_id, name, metadata=None):
        """
        See docstring for PointData.__init__
        """
        super(CuesLevel1, self).__init__(
            station_id or "CUES",
            name or "CUES",
            metadata=metadata
        )
        self._raw_metadata = None
        self._tzinfo = None

    def _get_one_vaiable(
        self, start_date, end_date, variables: SensorDescription,
        period, method
    ):
        dt_fmt = "%Y-%m-%d"
        data = dict(
            # table="downward looking solar radiation",
            table=variables.code, start=start_date.strftime(dt_fmt),
            end=end_date.strftime(dt_fmt), interval=period,
            method=method, output="CSV",
            category="Measurement"
        )
        resp = requests.post(self.URL, data=data)
        resp.raise_for_status()
        return resp.content.decode()

    def _sensor_response_to_df(self, data, variable):
        # Parse the 'csv' string returned
        df = pd.read_csv(
            StringIO(data), delimiter=",", skip_blank_lines=True,
            comment="#"
        )
        columns = list(df.columns.values)
        if len(columns) > 2:
            raise RuntimeError(
                f"Expected 2 columns, got {columns}"
            )
        column_map = {
            columns[0]: "datetime",
            columns[1]: variable.name
        }
        # Parse the units out of the returned column name
        units = columns[1].split(";")[-1].replace("(", "").replace(")", "")
        # Rename to desired columns and add a units column
        df.rename(columns=column_map, inplace=True)
        df[f"{variable.name}_units"] = [units] * len(df)

        return df.set_index("datetime")

    def _get_data(
        self, start_date, end_date, variables: List[SensorDescription],
        period,
    ):
        df = pd.DataFrame()
        for variable in variables:
            method = "sum" if variable.accumulated else "average"
            data = self._get_one_vaiable(
                start_date, end_date, variable, period, method
            )
            df_var = self._sensor_response_to_df(data, variable)
            # TODO: crashing here
            # for c in df_var.columns.values:
            df[df_var.columns] = df_var
        # Set the site info
        df["site"] = [self.id] * len(df)
        df["datasource"] = [self.DATASOURCE] * len(df)
        # TODO: handle tzinfo
        # Make this a geodataframe
        df = gpd.GeoDataFrame(df, geometry=[self.metadata] * len(df))
        df = df.reset_index().set_index(["datetime", "site"])
        self.validate_sensor_df(df)
        return df

    def get_daily_data(self, start_date: datetime, end_date: datetime,
                       variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "day"
        )

    def get_hourly_data(self, start_date: datetime, end_date: datetime,
                        variables: List[SensorDescription]):
        return self._get_data(
            start_date, end_date, variables, "hr"
        )

    def get_snow_course_data(self, start_date: datetime, end_date: datetime,
                             variables: List[SensorDescription]):
        raise NotImplementedError("Not implemented")

    def _get_metadata(self):
        pass

    def points_from_geometry(self, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription],
                             snow_courses=False, within_geometry=True,
                             buffer=0.0):
        raise NotImplementedError("Not implemented")

    @property
    def metadata(self):
        """
        Hardcode the metadata
        """
        return gpd.points_from_xy(
            [-119.029128], [37.643093], [9661]
        )[0]

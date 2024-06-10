from datetime import datetime, timezone, timedelta
from typing import List
import logging
import geopandas as gpd
import pandas as pd
from enum import Enum
from metloom.pointdata import CSVPointData, StationInfo
from metloom.variables import SnowExVariables
from pathlib import Path
import os


LOG = logging.getLogger(__name__)


class SnowExMetInfo(StationInfo):
    # Name, id, lat, long, http filename
    GM_STUDY_PLOT = "Grand Mesa Study Plot", "GMSP",  39.05084, 108.06144,"2017.06.21/SNEX_Met_GMSP2_final_output.csv"
    LS_OBS_SITE = "Local Scale Observation Site", "LSOS", 39.05225, 108.09792, "2016.10.09/SNEX_Met_LSOS_final_output.csv"
    MESA_EAST = "Mesa East", "ME", 39.10358, 107.88383, "2016.10.10/SNEX_Met_ME_final_output.csv"
    MESA_MIDDLE = "Mesa Middle", "MM", 39.03954, 107.94174, "2016.10.10/SNEX_Met_MM_final_output.csv"
    MESA_WEST = "Mesa West", "MW",  39.03388, 108.21399, "2016.10.09/SNEX_Met_MW_final_output.csv"


class SnowExMet(CSVPointData):
    """
    These data are stored in csv data formats
    """
    ALLOWED_VARIABLES = SnowExVariables
    ALLOWED_STATIONS = SnowExMetInfo
    DATETIME_COLUMN = 'TIMESTAMP'
    # Data is in UTC
    UTC_OFFSET_HOURS = 0
    URL = "https://n5eil01u.ecs.nsidc.org/SNOWEX/SNEX_Met.001/"
    DATASOURCE = "NSIDC"

    def _file_url(self):
        return os.path.join(self.URL, self._station_info.path)


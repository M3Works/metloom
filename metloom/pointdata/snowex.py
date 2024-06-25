import logging
import pandas as pd
from metloom.pointdata import CSVPointData, StationInfo
from metloom.variables import SnowExVariables
import os


LOG = logging.getLogger(__name__)


class SnowExMetInfo(StationInfo):
    # Name, id, lat, long, elevation, http filename
    GM_STUDY_PLOT = ("Grand Mesa Study Plot", "GMSP", 39.05084, -108.06144, 10626,
                     "2017.06.21/SNEX_Met_GMSP2_final_output.csv")
    LS_OBS_SITE = ("Local Scale Observation Site", "LSOS", 39.05225, -108.09792, 9791,
                   "2016.10.09/SNEX_Met_LSOS_final_output.csv")
    MESA_EAST = ("Mesa East", "ME", 39.10358, -107.88383, 10105,
                 "2016.10.10/SNEX_Met_ME_final_output.csv")
    MESA_MIDDLE = ("Mesa Middle", "MM", 39.03954, -107.94174, 10286,
                   "2016.10.10/SNEX_Met_MM_final_output.csv")
    MESA_WEST = ("Mesa West", "MW", 39.03388, -108.21399, 9950,
                 "2016.10.09/SNEX_Met_MW_final_output.csv")


class SnowExMet(CSVPointData):
    """
    These data are stored in csv data formats
    """
    ALLOWED_VARIABLES = SnowExVariables
    ALLOWED_STATIONS = SnowExMetInfo

    # Data is in UTC
    UTC_OFFSET_HOURS = 0

    URL = "https://n5eil01u.ecs.nsidc.org/SNOWEX/SNEX_Met.001/"
    DATASOURCE = "NSIDC"
    DOI = "https://doi.org/10.5067/497NQVJ0CBEX"

    def _file_urls(self, *args):
        return [os.path.join(self.URL, self._station_info.path)]

    def _assign_datetime(self, resp_df):
        resp_df['datetime'] = pd.to_datetime(resp_df['TIMESTAMP'])
        resp_df = resp_df.drop(columns=['TIMESTAMP']).set_index('datetime')
        return resp_df

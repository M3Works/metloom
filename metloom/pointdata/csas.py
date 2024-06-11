"""
Data reader for the Center for Snow and Avalanche Studies
"""
from metloom.pointdata import CSVPointData, StationInfo
from metloom.variables import CSASVariables
import os


class CSASStationInfo(StationInfo):
    # Name, id, lat, long, elevation, http path
    SENATOR_BECK = "Senator Beck Study Plot", "SBSP",  37.90688, -107.72627, 12186, "2023/11/SBSP_1hr_2003-2009.csv"
    SWAMP_ANGLE = "Swamp Angel Study Plot", "SASP", 37.90691, -107.71132, 11060, "2023/11/SASP_1hr_2003-2009.csv"
    PUTNEY = "Putney Study Plot", "PTSP", 37.89233, -107.69577, 12323, "2023/11/PTSP_1hr_2003-2009.csv"
    SENATOR_BECK_STREAM_GAUGE = "Senator Beck Stream Gauge", "SBSG", 37.90678, -107.70943, 11030, "2023/11/SBSG_1hr.csv"


class CSASMet(CSVPointData):
    """
    """
    ALLOWED_VARIABLES = CSASVariables
    ALLOWED_STATIONS = CSASStationInfo
    DATETIME_COLUMN = '---'
    # Data is in Mountain time
    UTC_OFFSET_HOURS = -7

    URL = "https://snowstudies.org/wp-content/uploads/"
    DATASOURCE = "CSAS"
    DOI = ""

    def _file_url(self):
        """
        Navigate the system using dates
        """
        return os.path.join(self.URL, self._station_info.path)




"""
Data reader for the Center for Snow and Avalanche Studies
"""
from metloom.pointdata import CSVPointData, StationInfo
from metloom.variables import CSASVariables
import os
from datetime import datetime, timedelta


class InvalidDateRange(Exception):
    """
    Exception to indicate there is no know data for the available date range
    """


class CSASStationInfo(StationInfo):
    # Name, id, lat, long, elevation, http path
    SENATOR_BECK = ("Senator Beck Study Plot", "SBSP", 37.90688, -107.72627, 12186,
                    "2023/11/SBSP_1hr_2003-2009.csv")
    SWAMP_ANGEL = ("Swamp Angel Study Plot", "SASP", 37.90691, -107.71132, 11060,
                   "2023/11/SASP_1hr_2003-2009.csv")
    PUTNEY = ("Putney Study Plot", "PTSP", 37.89233, -107.69577, 12323,
              "2023/11/PTSP_1hr.csv")
    SENATOR_BECK_STREAM_GAUGE = ("Senator Beck Stream Gauge", "SBSG", 37.90678,
                                 -107.70943, 11030, "2023/11/SBSG_1hr.csv")


class CSASMet(CSVPointData):
    """
    """
    CURRENT_AVAILABLE_YEAR = 2023
    ALLOWED_VARIABLES = CSASVariables
    ALLOWED_STATIONS = CSASStationInfo

    # Data is in Mountain time
    UTC_OFFSET_HOURS = -7

    URL = "https://snowstudies.org/wp-content/uploads/"
    DATASOURCE = "CSAS"
    DOI = ""

    def _file_urls(self, station_id, start, end):
        """
        Navigate the system using dates. Data for SASP and SBSP is stored in
        two csvs. 2003-2009 and 2010-2023. Not sure what happens when the
        next year is made available. This function will grab the necessary urls
        depending on the requested data
        """
        urls = []

        if station_id in ['SASP', 'SBSP']:
            current_available_year = self.CURRENT_AVAILABLE_YEAR

            if start.year <= 2009:
                urls.append(os.path.join(self.URL, self._station_info.path))

            # Account for later file use or even straddling thge data
            if start.year > 2009 or end.year > 2009:  # TODO: add to the info enum?
                partial = str(self._station_info.path).replace("2003", "2010")

                filename = partial.replace('2009', str(current_available_year))
                urls.append(os.path.join(self.URL, filename))

            if start.year < 2003 or end.year > current_available_year:
                raise InvalidDateRange(f"CSAS data is only available from 2003-"
                                       f"{current_available_year}")
        else:
            urls.append(os.path.join(self.URL, self._station_info.path))

        return urls

    @staticmethod
    def _parse_datetime(row):
        # Julian day is not zero based Jan 1 == DOY 1
        dt = timedelta(days=int(row['DOY']) - 1, hours=int(row['Hour'] / 100))
        return datetime(int(row['Year']), 1, 1) + dt

    def _assign_datetime(self, resp_df):
        resp_df['datetime'] = resp_df.apply(lambda row: self._parse_datetime(row),
                                            axis=1)
        return resp_df.set_index('datetime')

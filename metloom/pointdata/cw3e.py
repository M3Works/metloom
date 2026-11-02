"""
Data reader for the Center for Western Weather and Water Extremes (CW3E)
surface met stations.
https://cw3e-datashare.ucsd.edu/CW3E_SurfaceMetObs/

Hourly data is stored one file per day per station. Column layouts vary
by station and sometimes change through time, so each station's hourly README
is parsed to determine the columns of each file.
"""
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import requests

from metloom.pointdata import CSVPointData, PointDataCollection, StationInfo
from metloom.variables import CW3EVariables, SensorDescription

LOG = logging.getLogger(__name__)


class CW3EStationInfo(StationInfo):
    """
    Locations from https://cw3e.ucsd.edu/metadata/sites
    """
    # Name, id, lat, long, elevation (ft), path to the hourly README
    ANTLER_POINT = (
        "Antler Point",
        "ATP", 39.338966, -123.16979, 1348, "ATP/ATP_HourlyData_README.txt"
    )
    ALPINE_ZEPHYR = (
        "Alpine Zephyr",
        "AZR", 40.66945, -107.0487, 7890, "AZR/AZR_HourlyData_README.txt"
    )
    BODEGA_BAY = (
        "Bodega Bay",
        "BBY", 38.318327, -123.07286, 43, "BBY/BBY_HourlyData_README.txt"
    )
    BOYES_CREEK_CANYON = (
        "Boyes Creek Canyon",
        "BCC", 39.340284, -123.16301, 1040, "BCC/BCC_HourlyData_README.txt"
    )
    BEAUMONT_CHERRY_VALLEY = (
        "Beaumont Cherry Valley",
        "BCV", 34.012778, -116.96, 1752, "BCV/BCV_HourlyData_README.txt"
    )
    BEER_CAN = (
        "Beer Can",
        "BEE", 37.529, -118.2843, 9081, "BEE/BEE_HourlyData_README.txt"
    )
    UC_BAKER = (
        "UC Baker",
        "BKR", 39.91611, -121.0628, 3901, "BKR/BKR_HourlyData.txt"
    )
    BOX_SPRINGS_RESERVE = (
        "Box Springs Reserve",
        "BSR", 33.96905, -117.29729, 2438, "BSR/BSR_HourlyData.txt"
    )
    BUCKS_LAKE_NODE_1 = (
        "Bucks Lake Node 1",
        "BU1", 39.8524, -121.254, 5761, "BU1/BU1_HourlyData.txt"
    )
    BUCKS_LAKE_NODE_2 = (
        "Bucks Lake Node 2",
        "BU2", 39.854, -121.251, 5784, "BU2/BU2_HourlyData.txt"
    )
    BUCKS_LAKE_NODE_3 = (
        "Bucks Lake Node 3",
        "BU3", 39.854, -121.249002, 5751, "BU3/BU3_HourlyData.txt"
    )
    BUCKS_LAKE = (
        "Bucks Lake",
        "BUB", 39.855, -121.252, 5807, "BUB/BUB_HourlyData.txt"
    )
    BROWNS_VALLEY_SCHOOL = (
        "Browns Valley School",
        "BVS", 39.23586, -121.40621, 233, "BVS/BVS_HourlyData_README.txt"
    )
    CATALINA_ISLAND = (
        "Catalina Island",
        "CAT", 33.44617, -118.48276, 115, "CAT/CAT_HourlyData_README.txt"
    )
    COLORADO_MOUNTAIN_COLLEGE_STEAMBOAT_SPRINGS = (
        "Colorado Mountain College Steamboat Springs",
        "CMC", 40.49582, -106.84171, 7123, "CMC/CMC_HourlyData_README.txt"
    )
    COW_PIE_PASTURE = (
        "Cow Pie Pasture",
        "CPP", 39.279452, -123.10986, 1033, "CPP/CPP_HourlyData_README.txt"
    )
    CARPENTER_RANCH = (
        "Carpenter Ranch",
        "CPR", 40.50224, -107.1727, 6401, "CPR/CPR_HourlyData_README.txt"
    )
    RANCHO_CUCAMONGA = (
        "Rancho Cucamonga",
        "CUC", 34.16889, -117.63306, 2411, "CUC/CUC_HourlyData_README.txt"
    )
    DOWNIEVILLE = (
        "Downieville",
        "DLA", 39.5634, -120.8242, 2956, "DLA/DLA_HourlyData_README.txt"
    )
    DEVILS_POSTPILE = (
        "Devils Postpile",
        "DPP", 37.6293, -119.0847, 7556, "DPP/DPP_HourlyData_README.txt"
    )
    PASTURE_RIDGE = (
        "Pasture Ridge",
        "DPR", 39.244819, -123.02199, 1709, "DPR/DPR_HourlyData_README.txt"
    )
    DEERWOOD = (
        "Deerwood",
        "DRW", 39.197574, -123.15985, 919, "DRW/DRW_HourlyData_README.txt"
    )
    ETIWANDA_DOWNSTREAM = (
        "Etiwanda Downstream",
        "EDS", 34.1585, -117.5138, 1775, "EDS/EDS_HourlyData_README.txt"
    )
    LOWER_ELK = (
        "Lower Elk",
        "ELK", 40.57138, -106.91629, 7244, "ELK/ELK_HourlyData_README.txt"
    )
    FORTY_MILE = (
        "Forty Mile",
        "FMI", 37.7737, -119.8619, 5643, "FMI/FMI_HourlyData_README.txt"
    )
    FEATHER_RIVER_COLLEGE = (
        "Feather River College",
        "FRC", 39.945873, -120.969701, 3428, "FRC/FRC_HourlyData_README.txt"
    )
    POTTER_VALLEY_EAST = (
        "Potter Valley East",
        "FRS", 39.3048, -123.082, 1017, "FRS/FRS_HourlyData_README.txt"
    )
    FISH_SLOUGH = (
        "Fish Slough",
        "FSL", 37.4792, -118.403, 4229, "FSL/FSL_HourlyData_README.txt"
    )
    GLIDER_PORT = (
        "Glider Port",
        "GDP", 37.5211, -118.295, 8123, "GDP/GDP_HourlyData_README.txt"
    )
    GIN_FLAT = (
        "Gin Flat",
        "GFT", 37.7669, -119.7749, 7044, "GFT/GFT_HourlyData_README.txt"
    )
    GREENHOUSE = (
        "Greenhouse",
        "GNH", 32.8862, -117.2297, 299, "GNH/GNH_HourlyData_README.txt"
    )
    GREEN_POND = (
        "Green Pond",
        "GPN", 39.240534, -123.009751, 2277, "GPN/GPN_HourlyData_README.txt"
    )
    GRANITE_PORTAL = (
        "Granite Portal",
        "GPO", 37.907655, -119.953053, 4304, "GPO/GPO_HourlyData_README.txt"
    )
    GRIZZLY_RIDGE_NODE_1 = (
        "Grizzly Ridge Node 1",
        "GR1", 39.92, -120.643, 6814, "GR1/GR1_HourlyData.txt"
    )
    GRIZZLY_RIDGE_NODE_2 = (
        "Grizzly Ridge Node 2",
        "GR2", 39.918, -120.643, 6837, "GR2/GR2_HourlyData.txt"
    )
    GRIZZLY_RIDGE_NODE_3 = (
        "Grizzly Ridge Node 3",
        "GR3", 39.919, -120.645, 6893, "GR3/GR3_HourlyData.txt"
    )
    GRIZZLY_RIDGE = (
        "Grizzly Ridge",
        "GRB", 39.919, -120.642, 6808, "GRB/GRB_HourlyData.txt"
    )
    HELLS_DELIGHT_CANYON = (
        "Hells Delight Canyon",
        "HDC", 39.267621, -123.14721, 2005, "HDC/HDC_HourlyData_README.txt"
    )
    HEADWORKS = (
        "Headworks",
        "HDW", 47.30888, -121.85569, 883, "HDW/HDW_HourlyData_README.txt"
    )
    HEART_K_RANCH = (
        "Heart K Ranch",
        "HKR", 40.067822, -120.693752, 3773, "HKR/HKR_HourlyData_README.txt"
    )
    HODGDON_MEADOW = (
        "Hodgdon Meadow",
        "HMD", 37.7964, -119.8586, 4577, "HMD/HMD_HourlyData_README.txt"
    )
    HOWE_PROPERTY = (
        "Howe Property",
        "HOW", 40.65278, -107.26171, 6667, "HOW/HOW_HourlyData_README.txt"
    )
    HENNESS_RIDGE_DRIVE = (
        "Henness Ridge Drive",
        "HRD", 37.6455, -119.70376, 6122, "HRD/HRD_HourlyData_README.txt"
    )
    INGRAM = (
        "Ingram",
        "IGM", 38.88623, -123.15132, 784, "IGM/IGM_HourlyData_README.txt"
    )
    KNOTT_RANCH = (
        "Knott Ranch",
        "KNT", 40.24779, -107.10279, 8481, "KNT/KNT_HourlyData_README.txt"
    )
    KETTLE_ROCK_NODE_1 = (
        "Kettle Rock Node 1",
        "KT1", 40.13982, -120.71201, 7349, "KT1/KT1_HourlyData.txt"
    )
    KETTLE_ROCK_NODE_2 = (
        "Kettle Rock Node 2",
        "KT2", 40.13919, -120.71203, 7320, "KT2/KT2_HourlyData.txt"
    )
    KETTLE_ROCK_NODE_3 = (
        "Kettle Rock Node 3",
        "KT3", 40.13675, -120.71622, 7083, "KT3/KT3_HourlyData.txt"
    )
    KETTLE_ROCK = (
        "Kettle Rock",
        "KTB", 40.14004, -120.71661, 7267, "KTB/KTB_HourlyData.txt"
    )
    LOWER_BATH_HOUSE_SFSU_SIERRA_NEVADA_FIELD_CAMPUS = (
        "Lower Bath House - SFSU Sierra Nevada Field Campus",
        "LBH", 39.624073, -120.577654, 5528, "LBH/LBH_HourlyData.txt"
    )
    LEE_VINING = (
        "Lee Vining",
        "LEE", 37.9396, -119.1284, 7162, "LEE/LEE_HourlyData_README.txt"
    )
    MARTIS_CREEK = (
        "Martis Creek",
        "MAK", 39.32852, -120.12229, 5869, "MAK/MAK_HourlyData_README.txt"
    )
    MINA = (
        "Mina",
        "MNA", 39.97912, -123.354488, 1749, "MNA/MNA_HourlyData.txt"
    )
    MERCED_GROVE = (
        "Merced Grove",
        "MRG", 37.7641, -119.8418, 5938, "MRG/MRG_HourlyData_README.txt"
    )
    MAYWOOD = (
        "Maywood",
        "MYD", 47.229853, -121.608979, 1401, "MYD/MYD_HourlyData_README.txt"
    )
    NEW_BULLARDS_BAR = (
        "New Bullards Bar",
        "NBB", 39.396359, -121.14377, 2073, "NBB/NBB_HourlyData_README.txt"
    )
    NORTH_COW_HILLSIDE = (
        "North Cow Hillside",
        "NCH", 39.178497, -123.07806, 3504, "NCH/NCH_HourlyData_README.txt"
    )
    NORTH_COW_MOUNTAIN = (
        "North Cow Mountain",
        "NCM", 39.179667, -123.08009, 3415, "NCM/NCM_HourlyData_README.txt"
    )
    NORTH_STAR_MEADOW = (
        "North Star Meadow",
        "NSM", 39.605249, -121.071594, 4052, "NSM/NSM_HourlyData.txt"
    )
    OVER_THE_HILL = (
        "Over The Hill",
        "OVH", 37.5156, -118.3167, 6086, "OVH/OVH_HourlyData_README.txt"
    )
    PIUTE_CREEK_OVERLOOK = (
        "Piute Creek Overlook",
        "PCC", 37.5149, -118.2996, 7165, "PCC/PCC_HourlyData_README.txt"
    )
    PORTOLA = (
        "Portola",
        "POR", 39.8175, -120.4969, 4951, "POR/POR_HourlyData_README.txt"
    )
    PRIEST_RESERVOIR = (
        "Priest Reservoir",
        "PST", 37.8056, -120.268, 2303, "PST/PST_HourlyData_README.txt"
    )
    POTTER_VALLEY_CENTRAL = (
        "Potter Valley Central",
        "PVC", 39.320908, -123.10282, 948, "PVC/PVC_HourlyData_README.txt"
    )
    POTTER_VALLEY_NORTH = (
        "Potter Valley North",
        "PVN", 39.36141, -123.11312, 1345, "PVN/PVN_HourlyData_README.txt"
    )
    RED_CREEK = (
        "Red Creek",
        "RED", 40.77179, -106.98333, 8228, "RED/RED_HourlyData_README.txt"
    )
    REDWOOD_VALLEY = (
        "Redwood Valley",
        "RVY", 39.284997, -123.30486, 1224, "RVY/RVY_HourlyData_README.txt"
    )
    SCRIPPS_PIER = (
        "Scripps Pier",
        "SIO", 32.8667, -117.256, 33, "SIO/SIO_HourlyData_README.txt"
    )
    STAGECOACH_SKI = (
        "Stagecoach Ski",
        "SKI", 40.22171, -106.86308, 9488, "SKI/SKI_HourlyData_README.txt"
    )
    SKYLINE_HARVEST = (
        "Skyline Harvest",
        "SKY", 39.470969, -121.091673, 2743, "SKY/SKY_HourlyData_README.txt"
    )
    SOLAR_PANEL = (
        "Solar Panel",
        "SLR", 39.281423, -123.12411, 1355, "SLR/SLR_HourlyData_README.txt"
    )
    SANTA_LUCIA = (
        "Santa Lucia",
        "SLU", 36.486134, -121.79388, 1883, "SLU/SLU_HourlyData_README.txt"
    )
    SMITH_PEAK = (
        "Smith Peak",
        "SMH", 37.8006, -120.1009, 3871, "SMH/SMH_HourlyData_README.txt"
    )
    SEVEN_OAKS_DAM = (
        "Seven Oaks Dam",
        "SOD", 34.115555, -117.093611, 2615, "SOD/SOD_HourlyData_README.txt"
    )
    STAMPEDE = (
        "Stampede",
        "STP", 47.25495, -121.3963, 2310, "STP/STP_HourlyData_README.txt"
    )
    SORE_THUMB = (
        "Sore Thumb",
        "SUM", 37.5108, -118.3248, 5177, "SUM/SUM_HourlyData_README.txt"
    )
    SUNSET_INN = (
        "Sunset Inn",
        "SUN", 37.8064, -119.8965, 4462, "SUN/SUN_HourlyData_README.txt"
    )
    TODD_CREEK = (
        "Todd Creek",
        "TCS", 40.13519, -106.84897, 8845, "TCS/TCS_HourlyData_README.txt"
    )
    UPPER_POND = (
        "Upper Pond",
        "UPP", 39.278335, -123.14313, 1739, "UPP/UPP_HourlyData_README.txt"
    )
    VALLEY_VIEW = (
        "Valley View",
        "VLV", 37.5292, -118.2628, 10253, "VLV/VLV_HourlyData_README.txt"
    )
    WINDY_GAP = (
        "Windy Gap",
        "WDG", 39.234289, -123.00498, 2736, "WDG/WDG_HourlyData_README.txt"
    )
    WHALE_WATCH = (
        "Whale Watch",
        "WHA", 32.6707, -117.2407, 390, "WHA/WHA_HourlyData_README.txt"
    )
    WILDHORSE_MEADOWS = (
        "Wildhorse Meadows",
        "WHS", 34.182639, -116.768111, 8373, "WHS/WHS_HourlyData_README.txt"
    )
    WEST_PORTAL = (
        "West Portal",
        "WPO", 37.813267, -120.287444, 2096, "WPO/WPO_HourlyData_README.txt"
    )
    YELLOW_JACKET_RIDGE = (
        "Yellow Jacket Ridge",
        "YJR", 39.331519, -123.184, 2313, "YJR/YJR_HourlyData_README.txt"
    )
    Y_LINE_LESTER = (
        "Y-Line (Lester)",
        "YLI", 47.21082, -121.48793, 1581, "YLI/YLI_HourlyData_README.txt"
    )
    YELLOW_POST = (
        "Yellow Post",
        "YLP", 34.216112, -116.978563, 7749, "YLP/YLP_HourlyData_README.txt"
    )
    YUCAIPA_VALLEY = (
        "Yucaipa Valley",
        "YVW", 34.0074, -117.0916, 2103, "YVW/YVW_HourlyData_README.txt"
    )


class CW3EPointData(CSVPointData):
    """
    CW3E surface met stations. Data are read from the hourly files which are
    in UTC with timestamps at the start of the sampling period. Daily data
    is resampled from the hourly data on UTC days.
    """
    ALLOWED_VARIABLES = CW3EVariables
    ALLOWED_STATIONS = CW3EStationInfo

    # Data is in UTC
    UTC_OFFSET_HOURS = 0

    URL = "https://cw3e-datashare.ucsd.edu/CW3E_SurfaceMetObs/"
    DATASOURCE = "CW3E"
    DOI = ""

    MISSING_VALUES = [-99.99, -999, -7999, -9999, -99999]
    # Convert README units to the units in CW3EVariables
    UNIT_CONVERSIONS = {"inches": 25.4, "cm": 0.01}
    # Files this many days old or newer may still be updated by CW3E
    REFRESH_DAYS = 2
    # Re-download the README after this long in case the layouts change
    README_MAX_AGE = timedelta(days=1)

    def __init__(self, station_id, name=None, metadata=None, cache='./cache'):
        """
        See docstring for CSVPointData.__init__
        """
        super(CW3EPointData, self).__init__(
            station_id, name, metadata=metadata, cache=cache
        )
        self._layouts = None

    @property
    def _station_cache(self):
        return self._cache.joinpath(self.DATASOURCE, self.id)

    def _file_urls(self, station_id, start, end):
        """
        One file per day, e.g. SKI/2023/Hourly/SKI_20231001.txt
        """
        start = pd.Timestamp(start).normalize()
        # End is exclusive
        end = (pd.Timestamp(end) - timedelta(microseconds=1)).normalize()
        return [
            f"{self.URL}{station_id}/{d.year}/Hourly/{station_id}_{d:%Y%m%d}.txt"
            for d in pd.date_range(start, end, freq="D")
        ]

    @staticmethod
    def _date_from_filename(filename):
        return datetime.strptime(Path(filename).stem.split("_")[-1], "%Y%m%d")

    def _is_recent(self, dt):
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None)
        return dt >= cutoff - timedelta(days=self.REFRESH_DAYS)

    def _download(self, urls):
        """
        Download the daily files to the cache. Missing days are cached as
        empty files unless they are recent enough to still show up.
        """
        self._station_cache.mkdir(parents=True, exist_ok=True)
        filenames = []
        with requests.Session() as session:
            for url in urls:
                filename = self._station_cache.joinpath(Path(url).name)
                recent = self._is_recent(self._date_from_filename(filename))
                if not filename.exists() or recent:
                    LOG.info(f'Downloading {Path(url).name}...')
                    resp = session.get(url)
                    if resp.status_code == 404:
                        LOG.debug(f"No data found at {url}")
                        if recent:
                            continue
                        content = b""
                    else:
                        resp.raise_for_status()
                        content = resp.content
                    filename.write_bytes(content)
                filenames.append(filename)
        return filenames

    @staticmethod
    def _parse_column(column):
        """
        Split a README column like 'Soil Temperature - 5 cm (C)' into a code
        and units, e.g. ('soil temperature - 5 cm', 'C')
        """
        match = re.match(r"^(.*?)\s*\(([^)]*)\)\s*$", column)
        name, units = match.groups() if match else (column, None)
        return re.sub(r"\s+", " ", name).strip().lower(), units

    @classmethod
    def _parse_readme(cls, text):
        """
        Parse the hourly README into a list of column layouts. Each layout
        starts with '1. Year' and may be preceded by the date range it is valid
        for, e.g. '2023-11-08 - 2024-05-28' or '2024-05-29 - Present'

        Returns:
            list of dicts with keys start, end, columns
        """
        layouts = []
        date_range = None
        for line in text.splitlines():
            dates = re.search(
                r"(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2}|present|current)",
                line, re.IGNORECASE
            )
            column = re.match(r"^\s*(\d+)\.\s*(.+?)\s*$", line)
            if dates:
                start, end = dates.groups()
                end = None if end.lower() in ["present", "current"] else end
                date_range = (pd.Timestamp(start), end and pd.Timestamp(end))
            elif column:
                if column.group(1) == "1":
                    start, end = date_range or (None, None)
                    layouts.append(dict(start=start, end=end, columns=[]))
                    date_range = None
                if layouts:
                    layouts[-1]["columns"].append(
                        cls._parse_column(column.group(2))
                    )
        return layouts

    def _get_layouts(self):
        """
        Download (or use the cached) hourly README and parse it
        """
        if self._layouts is None:
            self._station_cache.mkdir(parents=True, exist_ok=True)
            filename = self._station_cache.joinpath(self._station_info.path.name)
            max_age = self.README_MAX_AGE.total_seconds()
            if not filename.exists() or (
                datetime.now().timestamp() - filename.stat().st_mtime > max_age
            ):
                resp = requests.get(f"{self.URL}{self._station_info.path}")
                resp.raise_for_status()
                filename.write_text(resp.text)
            self._layouts = self._parse_readme(filename.read_text())
        return self._layouts

    def _select_layout(self, dt, ncolumns):
        """
        Pick the README layout for a file. Layouts that match the number of
        columns in the file are preferred in order of: valid for the date,
        no date range listed, then any. The READMEs date ranges are not always
        accurate so the number of columns takes precedence.
        """
        layouts = [
            lyt for lyt in self._get_layouts() if len(lyt["columns"]) == ncolumns
        ]

        def rank(layout):
            if layout["start"] is None:
                return 1
            if layout["start"] <= dt and (layout["end"] is None or dt <= layout["end"]):
                return 0
            return 2

        if not layouts:
            return None
        return sorted(layouts, key=rank)[0]

    def _read_one_file(self, filename):
        if filename.stat().st_size == 0:
            return None
        df = pd.read_csv(filename, header=None)
        dt = self._date_from_filename(filename)
        layout = self._select_layout(dt, len(df.columns))
        if layout is None:
            LOG.debug(f"No README column layout matches {filename.name} "
                      f"with {len(df.columns)} columns")
            return None

        codes = [c[0] for c in layout["columns"]]
        df.columns = codes
        df["datetime"] = pd.to_datetime(df[["year", "month", "day", "hour"]])
        df = df.drop(columns=["year", "month", "day", "hour"])
        df = df.loc[:, [not c.startswith("not used") for c in df.columns]]
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.mask(df.isin(self.MISSING_VALUES))

        for code, units in layout["columns"]:
            if code in df.columns and units in self.UNIT_CONVERSIONS:
                df[code] = df[code] * self.UNIT_CONVERSIONS[units]
        return df

    def _read_files(self, files):
        dfs = []
        skipped = []
        for f in files:
            df = self._read_one_file(f)
            if df is not None:
                dfs.append(df)
            elif f.stat().st_size > 0:
                skipped.append(f.name)
        if skipped:
            LOG.warning(f"Skipped {len(skipped)} {self.id} file(s) with columns "
                        f"not described in the README ({skipped[0]} to "
                        f"{skipped[-1]})")
        if not dfs:
            return None
        return pd.concat(dfs, ignore_index=True)

    def _assign_datetime(self, resp_df):
        return resp_df.set_index("datetime").sort_index()

    def _get_one_variable(self, resp_df, period, variable: SensorDescription):
        """
        Resample a single variable. Precip is summed, max wind is the max
        and wind direction is a circular mean.
        """
        if not self._verify_sensor(resp_df, variable):
            LOG.debug(f"No data returned for {variable.name}")
            return None

        data = resp_df[variable.code]
        agg = (variable.extra or {}).get("agg")
        if variable.accumulated:
            return data.resample(period).sum(min_count=1)
        elif agg == "max":
            return data.resample(period).max()
        elif agg == "circular":
            radians = np.deg2rad(data)
            sin = np.sin(radians).resample(period).mean()
            cos = np.cos(radians).resample(period).mean()
            return np.rad2deg(np.arctan2(sin, cos)) % 360
        else:
            return data.resample(period).mean()

    @classmethod
    def points_from_geometry(cls, geometry: gpd.GeoDataFrame,
                             variables: List[SensorDescription], within_geometry=True,
                             buffer=0.0, **kwargs):
        """
        Find CW3E stations within the geometry. Variables are not checked
        since that requires downloading each station's README.

        Args:
            geometry: GeoDataFrame for shapefile from gpd.read_file
            variables: List of SensorDescription (not used for filtering)
            within_geometry: filter the points to within the shapefile
                instead of just the extents. Default True
            buffer: buffer added to search box
        Returns:
            PointDataCollection
        """
        projected_geom = geometry.dissolve().buffer(buffer).to_crs(4326)
        if within_geometry:
            search_area = projected_geom.iloc[0]
        else:
            search_area = projected_geom.envelope.iloc[0]

        points = [
            cls(s.station_id, s.station_name, metadata=s.point, **kwargs)
            for s in cls.ALLOWED_STATIONS if s.point.within(search_area)
        ]
        return PointDataCollection(points)

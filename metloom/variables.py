from dataclasses import dataclass, field, make_dataclass
import typing


@dataclass(eq=True, frozen=True)
class SensorDescription:
    """
    data class for describing a snow sensor
    """

    code: str = "-1"  # code used within the applicable API
    name: str = "basename"  # desired name for the sensor
    description: str = None  # description of the sensor
    accumulated: bool = False  # whether the data is accumulated
    units: str = None  # Optional units kwarg
    extra: typing.Any = field(
        default=None, hash=False
    )  # Optional extra data for sub-class specific information


@dataclass(eq=True, frozen=True)
class InstrumentDescription(SensorDescription):
    """
    Extend the Sensor Description to include instrument
    """

    # description of the specific instrument for the variable
    instrument: str = None


class VariableBase:
    """
    Base class to store all variables for a specific datasource. Each
    datasource should implement the class. The goal is that the variables
    are synonymous across implementations.(i.e. PRECIPITATION should have the
    same meaning in each implementation).
    Additionally, variables with the same meaning should have the same
    `name` attribute of the SensorDescription. This way, if multiple datsources
    are used to sample the same variable, they can be written to the same
    column in a csv.

    Variables in this base class should ideally be implemented by all classes
    and cannot be directly used from the base class.
    """

    PRECIPITATION = SensorDescription()
    SWE = SensorDescription()
    SNOWDEPTH = SensorDescription()

    @staticmethod
    def _validate_sensor(sensor: SensorDescription):
        """
        Validate that a sensor is not using the default values since they
        are meaningless
        """
        default = SensorDescription()
        if sensor.name == default.name and sensor.code == default.code:
            raise ValueError(f"{sensor.name} is the default implementation")

    @classmethod
    def from_code(cls, code):
        """
        Get the correct sensor description from the code
        """
        for k, v in cls.__dict__.items():
            if isinstance(v, SensorDescription) and v.code == str(code):
                cls._validate_sensor(v)
                return v
        raise ValueError(f"Could not find sensor for code {code}")


class CdecStationVariables(VariableBase):
    """
    Available sensors from CDEC.
    Exhaustive list:
    http://cdec4gov.water.ca.gov/reportapp/javareports?name=SensList
    """

    PRECIPITATIONACCUM = SensorDescription(
        "2", "ACCUMULATED PRECIPITATION", "PRECIPITATION, ACCUMULATED", False
    )
    PRECIPITATION = SensorDescription(
        "45", "PRECIPITATION", "PRECIPITATION, INCREMENTAL", True
    )
    SNOWDEPTH = SensorDescription("18", "SNOWDEPTH", "SNOW DEPTH")
    SWE = SensorDescription("3", "SWE", "SNOW, WATER CONTENT", False)
    TEMP = SensorDescription("4", "AIR TEMP", "TEMPERATURE, AIR")
    TEMPAVG = SensorDescription("30", "AVG AIR TEMP", "TEMPERATURE, AIR AVERAGE")
    TEMPMIN = SensorDescription("32", "MIN AIR TEMP", "TEMPERATURE, AIR MINIMUM")
    TEMPMAX = SensorDescription("31", "MAX AIR TEMP", "TEMPERATURE, AIR MAXIMUM")
    RH = SensorDescription("12", "Relative Humidity", "RELATIVE HUMIDITY")
    # TODO confirm with CDWR if these depths are standard, no metadata available
    TEMPGROUND = SensorDescription(
        "52", "GROUND TEMPERATURE INT", "GROUND TEMPERATURE SNOW/SOIL INTERFACE"
    )
    TEMPGROUND25CM = SensorDescription(
        "194", "GROUND TEMPERATURE -25CM", "GROUND TEMPERATURE OBS -25CM"
    )
    TEMPGROUND50CM = SensorDescription(
        "195", "GROUND TEMPERATURE -50CM", "GROUND TEMPERATURE OBS -50CM"
    )
    TEMPGROUND100CM = SensorDescription(
        "196", "GROUND TEMPERATURE -100CM", "GROUND TEMPERATURE OBS -100CM"
    )
    SOLARRAD = SensorDescription("103", "SOLAR RADIATION", "SOLAR RADIATION")
    WINDSPEED = SensorDescription("9", "WIND SPEED", "WIND SPEED")
    WINDDIR = SensorDescription("10", "WIND DIRECTION", "WIND DIRECTION")


# Available sensors from Snotel
SnotelVariables = make_dataclass(
    "SnotelVariables",
    [
        (
            "SNOWDEPTH",
            SensorDescription,
            field(default=SensorDescription("SNWD", "SNOWDEPTH")),
        ),
        ("SWE", SensorDescription, field(default=SensorDescription("WTEQ", "SWE"))),
        (
            "TEMP",
            SensorDescription,
            field(default=SensorDescription("TOBS", "AIR TEMP")),
        ),
        (
            "TEMPAVG",
            SensorDescription,
            field(
                default=SensorDescription(
                    "TAVG", "AVG AIR TEMP", "AIR TEMPERATURE AVERAGE"
                )
            ),
        ),
        (
            "TEMPMIN",
            SensorDescription,
            field(
                default=SensorDescription(
                    "TMIN", "MIN AIR TEMP", "AIR TEMPERATURE MINIMUM"
                )
            ),
        ),
        (
            "TEMPMAX",
            SensorDescription,
            field(
                default=SensorDescription(
                    "TMAX", "MAX AIR TEMP", "AIR TEMPERATURE MAXIMUM"
                )
            ),
        ),
        (
            "PRECIPITATION",
            SensorDescription,
            field(
                default=SensorDescription(
                    "PRCPSA", "PRECIPITATION", "PRECIPITATION INCREMENT SNOW-ADJUSTED"
                )
            ),
        ),
        (
            "PRECIPITATIONACCUM",
            SensorDescription,
            field(
                default=SensorDescription(
                    "PREC", "ACCUMULATED PRECIPITATION", "PRECIPITATION ACCUMULATION"
                )
            ),
        ),
        # TODO for the SCAN network this appears to be "RHUM", we may need a new class
        (
            "RH",
            SensorDescription,
            field(
                default=SensorDescription(
                    "RHUMV", "Relative Humidity", "RELATIVE HUMIDITY"
                )
            ),
        ),
        (
            "STREAMVOLUMEOBS",
            SensorDescription,
            field(
                default=SensorDescription(
                    "SRVO", "STREAM VOLUME OBS", "STREAM VOLUME OBS"
                )
            ),
        ),
        (
            "STREAMVOLUMEADJ",
            SensorDescription,
            field(
                default=SensorDescription(
                    "SRVOX", "STREAM VOLUME ADJ", "STREAM VOLUME ADJ"
                )
            ),
        ),
    ] + [
        (
            f"TEMPGROUND{abs(d)}IN",
            SensorDescription,
            field(
                default=SensorDescription(
                    "STO",
                    f"GROUND TEMPERATURE -{d}IN",
                    f"GROUND TEMPERATURE OBS -{d}IN",
                    extra={"height_depth": {"value": -d, "unitCd": "in"}},
                )
            ),
        )
        for d in [2, 4, 8, 20]
    ] + [
        (
            f"SOILMOISTURE{abs(d)}IN",
            SensorDescription,
            field(
                default=SensorDescription(
                    "SMS",
                    f"SOIL MOISTURE -{d}IN",
                    f"SOIL MOISTURE PERCENT -{d}IN",
                    extra={"height_depth": {"value": -d, "unitCd": "in"}},
                )
            ),
        )
        for d in [2, 4, 8, 20]
    ] + [
        (
            f"TEMPPROFILENEG{abs(d)}IN" if d < 0 else f"TEMPPROFILE{d}IN",
            SensorDescription,
            field(
                default=SensorDescription(
                    "PTEMP",
                    f"PROFILE TEMPERATURE {d}IN",
                    f"PROFILE TEMPERATURE OBS{d}IN",
                    extra={"height_depth": {"value": d, "unitCd": "in"}},
                )
            ),
        )
        for d in [-8, 0, 8, 16, 24, 31, 39, 47, 55, 63, 71, 79, 87, 94, 102, 110, 118, 126]  # noqa: E501
    ],
    bases=(VariableBase,),
)


class MesowestVariables(VariableBase):
    """
    Available sensors from Mesowest
    There are a lot of variables here. Feel free to PR to add some
    https://developers.synopticdata.com/mesonet/v2/api-variables/
    """

    TEMP = SensorDescription("air_temp", "AIR TEMP")
    DEWPOINT = SensorDescription("dew_point_temperature", "DEW POINT TEMPERATURE")
    RH = SensorDescription("relative_humidity", "RELATIVE HUMIDITY")
    WINDSPEED = SensorDescription("wind_speed", "WIND SPEED")
    WINDDIRECTION = SensorDescription("wind_direction", "WIND DIRECTION")
    PRESSURE = SensorDescription("pressure", "PRESSURE")
    SNOWDEPTH = SensorDescription("snow_depth", "SNOWDEPTH")
    SOLARRADIATION = SensorDescription("solar_radiation", "SOLAR RADIATION")
    WETBULBTEMPERATURE = SensorDescription(
        "wet_bulb_temperature", "WET BULB TEMPERATURE"
    )
    SOILTEMP = SensorDescription("soil_temp", "SOIL TEMPERATURE")
    SOILTEMPIR = SensorDescription("soil_temp_ir", "SOIL TEMPERATURE IR")
    SWE = SensorDescription("snow_water_equiv", "SWE")
    NETSHORTWAVE = SensorDescription("net_radiation_sw", "NET SHORTWAVE RADIATION")
    NETLONGWAVE = SensorDescription("net_radiation_lw", "NET LONGWAVE RADIATION")
    STREAMFLOW = SensorDescription("stream_flow", "STREAMFLOW")


class USGSVariables(VariableBase):
    """
    To add more sensors:
    https://help.waterdata.usgs.gov/codes-and-parameters/parameters
    """

    DISCHARGE = SensorDescription("00060", "DISCHARGE", "DISCHARGE (CFS)")
    STREAMFLOW = SensorDescription(
        "74082", "STREAMFLOW", "STREAMFLOW, DAILY VOLUME (AC-FT)"
    )
    SNOWDEPTH = SensorDescription("72189", "SNOWDEPTH", "Snow depth, Meters")
    SWE = SensorDescription("72341", "SWE", "Water content of snow, millimeters")
    SOLARRADIATION = SensorDescription(
        "72179", "SOLAR RADIATION", "Shortwave solar radiation, watts per square meter"
    )
    UPSHORTWAVE = SensorDescription(
        "72185",
        "UPWARD SHORTWAVE RADIATION",
        "Shortwave radiation, upward intensity, watts per square meter",
    )
    DOWNSHORTWAVE = SensorDescription(
        "72186",
        "DOWNWARD SHORTWAVE RADIATION",
        "Shortwave radiation, downward intensity, watts per square meter",
    )
    NETSHORTWAVE = SensorDescription(
        "72201",
        "NET SHORTWAVE RADIATION",
        "Net incident shortwave radiation, watts per square meter",
    )
    NETLONGWAVE = SensorDescription(
        "72202",
        "NET LONGWAVE RADIATION",
        "Net emitted longwave radiation, watts per square meter",
    )
    DOWNLONGWAVE = SensorDescription(
        "72175",
        "DOWNWARD LONGWAVE RADIATION",
        "Longwave radiation, downward intensity, watts per square meter",
    )
    UPLONGWAVE = SensorDescription(
        "72174",
        "UPWARD LONGWAVE RADIATION",
        "Longwave radiation, upward intensity, watts per square meter",
    )
    SURFACETEMP = SensorDescription(
        "72405",
        "SURFACE TEMPERATURE",
        "Surface temperature, non-contact, degrees Celsius",
    )


class GeoSphereCurrentVariables(VariableBase):
    TEMP = SensorDescription("TL", "Air Temperature")
    SNOWDEPTH = SensorDescription("SCHNEE", "Snowdepth")
    PRECIPITATION = SensorDescription(
        "RR", "Rainfall in the last 10 minutes", accumulated=True
    )
    TEMPGROUND10CM = SensorDescription("TB1", "Soil temperature at a depth of 10cm")
    TEMPGROUND20CM = SensorDescription("TB2", "Soil temperature at a depth of 20cm")
    TEMPGROUND50CM = SensorDescription("TB3", "Soil temperature at a depth of 50cm")


class GeoSphereHistVariables(VariableBase):
    """
    Variables that correspond to the DAILY historical Klima dataset

    Daily and hourly have different variable names
    https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v1-1h/metadata
    https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v1-1d/metadata
    """

    TEMP = SensorDescription("t7", "Air temperature 2m on observation date")
    SNOWDEPTH = SensorDescription("schnee", "Snowdepth")
    PRECIPITATION = SensorDescription("nied", "Precipitation Total", accumulated=True)


class CuesLevel1Variables(VariableBase):
    """
    Variables for CUES level1 data
    https://snow.ucsb.edu/index.php/query-db/

    Some variables report back with multiple instruments. See `UPSHORTWAVE`
    and `UPSHORTWAVE2` for two instrument specific implementations
    of the same variable.

    """

    TEMP = InstrumentDescription("air temperature", "AIR TEMP")
    RH = InstrumentDescription("RH", "RELATIVE HUMIDITY")
    LASERSNOWDEPTH = InstrumentDescription("laser snow depth", "LASER SNOWDEPTH")
    SNOWDEPTH = InstrumentDescription("snow depth", "SNOWDEPTH")
    NEWSNOWDEPTH = InstrumentDescription("new snow depth", "NEW SNOWDEPTH")
    SWE = InstrumentDescription("Snow Pillow (DWR) SWE", "SWE")
    # PRECIPITATION = InstrumentDescription(
    #     "nied", "Precipitation Total", accumulated=True
    # )
    TEMPSURFSNOW = InstrumentDescription(
        "snow surface temperature", "SNOW SURFACE TEMPERATURE"
    )
    DOWNSHORTWAVE = InstrumentDescription(
        "downward looking solar radiation",
        "DOWNWARD SHORTWAVE RADIATION",
    )
    UPSHORTWAVE = InstrumentDescription(
        "upward looking solar radiation",
        "UPWARD SHORTWAVE RADIATION",
        instrument="Eppley Lab precision spectral pyranometer",
    )
    UPSHORTWAVE2 = InstrumentDescription(
        "upward looking solar radiation",
        "UPWARD SHORTWAVE RADIATION 2",
        instrument="uplooking Sunshine pyranometer  direct and diffus",
    )
    DOWNSHORTWAVEIR = InstrumentDescription(
        "downward looking near-IR radiation",
        "DOWNWARD NIR SHORTWAVE RADIATION",
    )
    UPSHORTWAVEIR = InstrumentDescription(
        "upward looking near-IR radiation",
        "UPWARD NIR SHORTWAVE RADIATION",
    )


class MetNorwayVariables(VariableBase):
    """
    See https://frost.met.no/concepts2.html#calculationmethod
    for explanation of variable naming.
    All available variables are
    https://frost.met.no/elementtable
    """

    TEMP = SensorDescription(
        "air_temperature",
        "AIR TEMP",
        "Air temperature (default 2 m above ground), present value",
    )
    TEMPAVG = SensorDescription(
        "best_estimate_mean(air_temperature P1D)",
        "AVG AIR TEMP",
        "Homogenised daily mean temperature."
        " The mean is an arithmetic mean of 24 hourly values (00-00 UTC),",
    )
    SNOWDEPTH = SensorDescription(
        "surface_snow_thickness",
        "SNOWDEPTH",
        "The depth of the snow is measured in cm from the ground to the top of"
        " the snow cover. (Code=-1 means 'no snow'"
        " and can be presented as '.')",
    )
    # SWE is only available as regional and interpolated datasets, so
    # metloom will NOT return data
    SWE = SensorDescription(
        "liquid_water_content_of_surface_snow",
        "SWE",
        "Snow water equivalent is a measure of the amount of water obtained"
        " if the snow is melted (as height in mm of a water column)",
    )
    PRECIPITATIONACCUM = SensorDescription(
        "accumulated(precipitation_amount)",
        "ACCUMULATED PRECIPITATION",
        "Total precipitation amount in gauge"
        " (accumulated since last emptying). Timing for emptying and"
        " algorithm for calculating the precipitation"
        " amount depends on sensortype",
    )
    PRECIPITATION = SensorDescription(
        "precipitation_amount",
        "PRECIPITATION",
        "Tipping bucket. The gauge tips for every 0.1 mm."
        " Each tip is registered along with the time stamp for the tip."
        " This is the basis for calcutation of precipitation sum per minute",
    )


class NWSForecastVariables(VariableBase):
    """
    See https://api.weather.gov/gridpoints/BOI/28,28
    for examples of variables
    """

    # Precipitation is not returned hourly
    PRECIPITATIONACCUM = SensorDescription(
        "quantitativePrecipitation", "ACCUMULATED PRECIPITATION", accumulated=True
    )
    # PRECIPITATION = SensorDescription(
    #     "quantitativePrecipitation", "PRECIPITATION",
    #     accumulated=False
    # )
    TEMP = SensorDescription(
        "temperature",
        "AIR TEMP",
    )
    DEWPOINT = SensorDescription("dewpoint", "DEW POINT TEMPERATURE")
    RH = SensorDescription("relativeHumidity", "RELATIVE HUMIDITY")


class SnowExVariables(VariableBase):
    """
    Variables for SnowEx met stations data, refer to user guide for adding more
    variables

    Metadata:
    https://nsidc.org/sites/default/files/documents/user-guide/snex_met-v001-userguide.pdf
    """

    TEMP_20FT = SensorDescription(
        "AirTC_20ft_Avg",
        "AIR TEMP @20ft",
        units="deg C",
        description="Air temperature measured at 20 ft",
    )

    TEMP_10FT = SensorDescription(
        "AirTC_10ft_Avg",
        "AIR TEMP @10ft",
        units="deg C",
        description="Air temperature measured at 10 ft",
    )

    UPSHORTWAVE = SensorDescription(
        "SUp_Avg",
        "UPWARD SHORTWAVE RADIATION",
        units="w/m^2",
        description="Shortwave radiation measured with upward-facing sensor",
    )
    DOWNSHORTWAVE = SensorDescription(
        "SDn_Avg",
        "DOWNWARD SHORTWAVE RADIATION",
        description="Shortwave radiation measured with downward-facing sensor",
    )
    SNOWDEPTH = SensorDescription(
        "SnowDepthFilter(m)",
        "SNOWDEPTH",
        description="Snow surface height in meters w/ filtering",
    )

    TEMPGROUND5CM = SensorDescription("TC_5cm_Avg", "SOIL TEMP @ 5cm")
    TEMPGROUND20CM = SensorDescription("TC_20cm_Avg", "SOIL TEMP @ 20cm")
    TEMPGROUND50CM = SensorDescription("TC_50cm_Avg", "SOIL TEMP @ 50cm")


class CSASVariables(VariableBase):
    """
    Variable meta for the stations:
    SASP - https://snowstudies.org/wp-content/uploads/2023/11/SASP_Variable_Table.xlsx
    SBSP - https://snowstudies.org/wp-content/uploads/2023/11/SBSP_Variable_Table.xlsx
    PTSP - https://snowstudies.org/wp-content/uploads/2023/11/PTSP_Variable_Table.xlsx
    SGSB - https://snowstudies.org/wp-content/uploads/2023/11/SBSG_Variable_Table.xlsx
    """

    SNOWDEPTH = SensorDescription("Sno_Height_M", "SNOWDEPTH", units="meters")
    RH = SensorDescription("RH", "RELATIVE HUMIDITY", units="%")
    STREAMFLOW_CFS = SensorDescription("Discharge_CFS", "STREAMFLOW", units="CFS")
    SURF_TEMP = SensorDescription(
        "Sno_IR_C",
        "SURFACE TEMP",
        units="deg C",
        description="Snow surface temperature",
    )
    UPPER_WINDSPEED = SensorDescription(
        "UpWind_Uavg_MS",
        "UPPER WIND SPEED",
        units="m/s",
        description="Wind speed at the upper location",
    )
    UPPER_WINDDIR = SensorDescription(
        "UpWind_Dir_Uavg",
        "UPPER WIND DIRECTION",
        units="degrees",
        description="Wind direction at the upper location",
    )
    LOWER_WINDSPEED = SensorDescription(
        "LoWind_Uavg_MS",
        "LOWER WIND SPEED",
        units="m/s",
        description="Wind speed at the lower location",
    )
    LOWER_WINDDIR = SensorDescription(
        "LoWind_Dir_Uavg",
        "LOWER WIND DIRECTION",
        units="degrees",
        description="Wind direction at the lower location",
    )
    DOWN_BROADBAND = SensorDescription(
        "PyDwn_Unfilt_W",
        "DOWNWARD BROADBAND RADIATION",
        units="w/m^2",
        description="Reflected Broadband radiation",
    )
    DOWN_NIR_SWIR = SensorDescription(
        "PyDwn_Filt_W",
        "DOWNWARD NIR/SWIR RADIATION",
        units="w/m^2",
        description="Reflected NIR/SWIR radiation",
    )
    UP_BROADBAND = SensorDescription(
        "PyUp_Unfilt_W",
        "UPWARD BROADBAND RADIATION",
        units="w/m^2",
        description="Incoming Broadband radiation",
    )

    UP_NIR_SWIR = SensorDescription(
        "PyUp_Filt_W",
        "UPWARD NIR/SWIR RADIATION",
        units="w/m^2",
        description="Incoming NIR/SWIR radiation",
    )

    PRECIPITATION = SensorDescription(
        "Day_H2O_mm",
        "DAILY PRECIP",
        accumulated=False,
        units="mm",
        description="Daily accumulated precipitation in mm",
    )

    TEMPGROUND = SensorDescription(
        "Soil_Surf_C",
        "GROUND TEMPERATURE",
        units="deg C",
        description="Temperature at soil interface",
    )
    TEMPGROUND10CM = SensorDescription(
        "Soil_10cm_C",
        "GROUND TEMPERATURE -10CM",
        units="deg C",
        description="Soil temperature at a depth of 10cm",
    )
    TEMPGROUND20CM = SensorDescription(
        "Soil_20cm_C",
        "GROUND TEMPERATURE -20CM",
        units="deg C",
        description="Soil temperature at a depth of 20cm",
    )
    TEMPGROUND40CM = SensorDescription(
        "Soil_40cm_C",
        "GROUND TEMPERATURE -40CM",
        units="deg C",
        description="Soil temperature at a depth of 40cm",
    )

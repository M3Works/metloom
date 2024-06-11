from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class SensorDescription:
    """
    data class for describing a snow sensor
    """

    code: str = "-1"  # code used within the applicable API
    name: str = "basename"  # desired name for the sensor
    description: str = None  # description of the sensor
    accumulated: bool = False  # whether or not the data is accumulated


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


class SnotelVariables(VariableBase):
    """
    Available sensors from SNOTEL
    """

    SNOWDEPTH = SensorDescription("SNWD", "SNOWDEPTH")
    SWE = SensorDescription("WTEQ", "SWE")
    TEMP = SensorDescription("TOBS", "AIR TEMP")
    TEMPAVG = SensorDescription("TAVG", "AVG AIR TEMP", "AIR TEMPERATURE AVERAGE")
    TEMPMIN = SensorDescription("TMIN", "MIN AIR TEMP", "AIR TEMPERATURE MINIMUM")
    TEMPMAX = SensorDescription("TMAX", "MAX AIR TEMP", "AIR TEMPERATURE MAXIMUM")
    PRECIPITATION = SensorDescription(
        "PRCPSA", "PRECIPITATION", "PRECIPITATION INCREMENT SNOW-ADJUSTED"
    )
    PRECIPITATIONACCUM = SensorDescription(
        "PREC", "ACCUMULATED PRECIPITATION", "PRECIPITATION ACCUMULATION"
    )
    TEMPGROUND2IN = SensorDescription(
        "STO", "GROUND TEMPERATURE -2IN", "GROUND TEMPERATURE OBS -2IN"
    )
    TEMPGROUND4IN = SensorDescription(
        "STO", "GROUND TEMPERATURE -4IN", "GROUND TEMPERATURE OBS -4IN"
    )
    TEMPGROUND8IN = SensorDescription(
        "STO", "GROUND TEMPERATURE -8IN", "GROUND TEMPERATURE OBS -8IN"
    )
    TEMPGROUND20IN = SensorDescription(
        "STO", "GROUND TEMPERATURE -20IN", "GROUND TEMPERATURE OBS -20IN"
    )
    SOILMOISTURE2IN = SensorDescription(
        "SMS", "SOIL MOISTURE -2IN", "SOIL MOISTURE PERCENT -2IN"
    )
    SOILMOISTURE4IN = SensorDescription(
        "SMS", "SOIL MOISTURE -4IN", "SOIL MOISTURE PERCENT -4IN"
    )
    SOILMOISTURE8IN = SensorDescription(
        "SMS", "SOIL MOISTURE -8IN", "SOIL MOISTURE PERCENT -8IN"
    )
    SOILMOISTURE20IN = SensorDescription(
        "SMS", "SOIL MOISTURE -20IN", "SOIL MOISTURE PERCENT -20IN"
    )
    # TODO for the SCAN network this appears to be "RHUM", we may need a new class
    RH = SensorDescription("RHUMV", "RELATIVE HUMIDITY", "RELATIVE HUMIDITY")
    STREAMVOLUMEOBS = SensorDescription(
        "SRVO", "STREAM VOLUME OBS", "STREAM VOLUME OBS"
    )
    STREAMVOLUMEADJ = SensorDescription(
        "SRVOX", "STREAM VOLUME ADJ", "STREAM VOLUME ADJ"
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
    SWE = SensorDescription(
        "72341", "SWE", "Water content of snow, millimeters"
    )
    SOLARRADIATION = SensorDescription(
        "72179", "SOLAR RADIATION",
        "Shortwave solar radiation, watts per square meter"
    )
    UPSHORTWAVE = SensorDescription(
        "72185", "UPWARD SHORTWAVE RADIATION",
        "Shortwave radiation, upward intensity, watts per square meter"
    )
    DOWNSHORTWAVE = SensorDescription(
        "72186", "DOWNWARD SHORTWAVE RADIATION",
        "Shortwave radiation, downward intensity, watts per square meter"
    )
    NETSHORTWAVE = SensorDescription(
        "72201", "NET SHORTWAVE RADIATION",
        "Net incident shortwave radiation, watts per square meter",
    )
    NETLONGWAVE = SensorDescription(
        "72202", "NET LONGWAVE RADIATION",
        "Net emitted longwave radiation, watts per square meter"
    )
    DOWNLONGWAVE = SensorDescription(
        "72175", "DOWNWARD LONGWAVE RADIATION",
        "Longwave radiation, downward intensity, watts per square meter"
    )
    UPLONGWAVE = SensorDescription(
        "72174", "UPWARD LONGWAVE RADIATION",
        "Longwave radiation, upward intensity, watts per square meter"
    )
    SURFACETEMP = SensorDescription(
        "72405", "SURFACE TEMPERATURE",
        "Surface temperature, non-contact, degrees Celsius"
    )


class GeoSphereCurrentVariables(VariableBase):
    TEMP = SensorDescription("TL", "Air Temperature")
    SNOWDEPTH = SensorDescription(
        "SCHNEE", "Snowdepth"
    )
    PRECIPITATION = SensorDescription(
        "RR", "Rainfall in the last 10 minutes", accumulated=True
    )
    TEMPGROUND10CM = SensorDescription(
        "TB1", "Soil temperature at a depth of 10cm"
    )
    TEMPGROUND20CM = SensorDescription(
        "TB2", "Soil temperature at a depth of 20cm"
    )
    TEMPGROUND50CM = SensorDescription(
        "TB3", "Soil temperature at a depth of 50cm"
    )


class GeoSphereHistVariables(VariableBase):
    """
    Variables that correspond to the DAILY historical Klima dataset

    Daily and hourly have different variable names
    https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v1-1h/metadata
    https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v1-1d/metadata
    """
    TEMP = SensorDescription("t7", "Air temperature 2m on observation date")
    SNOWDEPTH = SensorDescription(
        "schnee", "Snowdepth"
    )
    PRECIPITATION = SensorDescription(
        "nied", "Precipitation Total", accumulated=True
    )


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
        "downward looking solar radiation", "DOWNWARD SHORTWAVE RADIATION",
    )
    UPSHORTWAVE = InstrumentDescription(
        "upward looking solar radiation", "UPWARD SHORTWAVE RADIATION",
        instrument="Eppley Lab precision spectral pyranometer"
    )
    UPSHORTWAVE2 = InstrumentDescription(
        "upward looking solar radiation", "UPWARD SHORTWAVE RADIATION 2",
        instrument="uplooking Sunshine pyranometer  direct and diffus"
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
        "air_temperature", "AIR TEMP",
        "Air temperature (default 2 m above ground), present value"
    )
    TEMPAVG = SensorDescription(
        "best_estimate_mean(air_temperature P1D)", "AVG AIR TEMP",
        "Homogenised daily mean temperature."
        " The mean is an arithmetic mean of 24 hourly values (00-00 UTC),"
    )
    SNOWDEPTH = SensorDescription(
        "surface_snow_thickness", "SNOWDEPTH",
        "The depth of the snow is measured in cm from the ground to the top of"
        " the snow cover. (Code=-1 means 'no snow'"
        " and can be presented as '.')"
    )
    # SWE is only available as regional and interpolated datasets, so
    # metloom will NOT return data
    SWE = SensorDescription(
        "liquid_water_content_of_surface_snow", "SWE",
        "Snow water equivalent is a measure of the amount of water obtained"
        " if the snow is melted (as height in mm of a water column)"
    )
    PRECIPITATIONACCUM = SensorDescription(
        "accumulated(precipitation_amount)", "ACCUMULATED PRECIPITATION",
        "Total precipitation amount in gauge"
        " (accumulated since last emptying). Timing for emptying and"
        " algoritm for calculating the precipitation"
        " amount depends on sensortype"
    )
    PRECIPITATION = SensorDescription(
        "precipitation_amount", "PRECIPITATION",
        "Tipping bucket. The gauge tips for every 0.1 mm."
        " Each tip is registered along with the time stamp for the tip."
        " This is the basis for calcutation of precipitation sum per minute"
    )


class SnowExVariables(VariableBase):
    """
    Variables for SnowEx met stations data, refer to user guide for adding more variables

    Metadata:
    https://nsidc.org/sites/default/files/documents/user-guide/snex_met-v001-userguide.pdf
    """

    TEMP_20FT = InstrumentDescription(
        'AirTC_20ft_Avg', "AIR TEMP @20ft",
        description="Air temperature measured at 20 ft tower levelin deg C")

    TEMP_10FT = InstrumentDescription(
        'AirTC_10ft_Avg', "AIR TEMP @10ft",
        description="Air temperature measured at 10 ft tower levelin deg C")

    UPSHORTWAVE = InstrumentDescription(
        "SUp_Avg", "UPWARD SHORTWAVE RADIATION",
        description="Shortwave radiation measured with upward-facing sensor",
        instrument="CNR4 Net Radiometer"
    )
    DOWNSHORTWAVE = InstrumentDescription(
        "SDn_Avg", "DOWNWARD SHORTWAVE RADIATION",
        description="Shortwave radiation measured with downward-facing sensor",
        instrument="CNR4 Net Radiometer"
    )
    SNOWDEPTH = InstrumentDescription(
        "SnowDepthFilter(m)", "SNOW DEPTH",
        description="Snow surface height in meters w/ filtering")

    TEMPGROUND5CM = InstrumentDescription("TC_5cm_Avg", "SOIL TEMP @ 5cm")
    TEMPGROUND20CM = InstrumentDescription("TC_20cm_Avg", "SOIL TEMP @ 20cm")
    TEMPGROUND50CM = InstrumentDescription("TC_50cm_Avg", "SOIL TEMP @ 50cm")


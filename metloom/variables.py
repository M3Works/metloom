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
    SOILTEMP = SensorDescription("194", "Soil Temperature", "SOIL TEMPERATURE 1")
    SOILTEMP2 = SensorDescription("195", "Soil Temperature", "SOIL TEMPERATURE 2")
    SOILTEMP3 = SensorDescription("196", "Soil Temperature", "SOIL TEMPERATURE 3")
    SOLARRAD = SensorDescription("103", "Solar Radiation", "SOLAR RADIATION")
    WINDSPEED = SensorDescription("9", "Wind Speed", "WIND SPEED")
    WINDDIR = SensorDescription("10", "Wind Direction", "WIND DIRECTION")


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
        "PRCPSA", "PRECIPITATON", "PRECIPITATION INCREMENT SNOW-ADJUSTED"
    )
    PRECIPITATIONACCUM = SensorDescription(
        "PREC", "ACCUMULATED PRECIPITATION", "PRECIPITATION ACCUMULATION"
    )
    TEMPGROUND2 = SensorDescription(
        "STO", "GROUND TEMPERATURE -2", "GROUND TEMPERATURE OBS -2in"
    )   # STV?
    # SOILMOIS = SensorDescription("SMS", "SOIL MOISTURE", "SOIL MOISTURE")
    # for the SCAN network this appears to be "RHUM", we may need a new class
    RH = SensorDescription("RHUMV", "RELATIVE HUMIDITY", "RELATIVE HUMIDITY")


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
    WETBULBTEMPERATURE = SensorDescription("wet_bulb_temperature",
                                           "WET BULB TEMPERATURE")
    SOILTEMP = SensorDescription("soil_temp", "SOIL TEMPERATURE")
    SOILTEMPIR = SensorDescription("soil_temp_ir", "SOIL TEMPERATURE IR")
    SWE = SensorDescription("snow_water_equiv", "SWE")
    NETSHORTWAVE = SensorDescription("net_radiation_sw", "NET SHORTWAVE RADIATION")
    NETLONGWAVE = SensorDescription("net_radiation_lw", "NET LONGWAVE RADIATION")


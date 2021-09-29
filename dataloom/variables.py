from dataclasses import dataclass


@dataclass
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
    PRECIPITATION = SensorDescription("2", "PRECIPITATION",
                                      "PRECIPITATION, ACCUMULATED", True)
    SNOWDEPTH = SensorDescription("18", "SNOWDEPTH", "SNOW DEPTH")
    SWE = SensorDescription("3", "SWE", "SNOW, WATER CONTENT", True)
    TEMP = SensorDescription("4", "AIR TEMP", "TEMPERATURE, AIR")
    AVGTEMP = SensorDescription("30", "AVG AIR TEMP",
                                "TEMPERATURE, AIR AVERAGE")
    MINTEMP = SensorDescription("32", "MIN AIR TEMP",
                                "TEMPERATURE, AIR MINIMUM")
    MAXTEMP = SensorDescription("31", "MAX AIR TEMP",
                                "TEMPERATURE, AIR MAXIMUM")
    # seems like the most useful
    MEANDAILYFLOW = SensorDescription("41", "MEAN FLOW", "FLOW, MEAN DAILY")
    RIVERDISCHARGE = SensorDescription("20", "River Discharge",
                                       "FLOW, RIVER DISCHARGE")  # hourly?
    RESERVOIRINFLOW = SensorDescription("76", "Reservoir Inflow",
                                        "RESERVOIR INFLOW")
    FORESCASTAJ10 = SensorDescription("260", "A-J 10% Exceedance Forecast",
                                      "A-J 10% FORECAST EXCEEDANCE")
    FORESCASTAJ50 = SensorDescription("261", "A-J 50% Exceedance Forecast",
                                      "A-J 50% FORECAST EXCEEDANCE")
    FORESCASTAJ90 = SensorDescription("262", "A-J 90% Exceedance Forecast",
                                      "A-J 90% FORECAST EXCEEDANCE")
    SensorDescription("32", "MIN AIR TEMP", "TEMPERATURE, AIR MINIMUM")


class SnotelVariables(VariableBase):
    """
    Available sensors from SNOTEL
    """
    SNOWDEPTH = SensorDescription("SNWD", "SNOWDEPTH")
    SWE = SensorDescription("WTEQ", "SWE")
    TEMP = SensorDescription("TOBS", "AIR TEMP")
    AVGTEMP = SensorDescription("TAVG", "AVG AIR TEMP")
    PRECIPITATION = SensorDescription("PRCPSA", "PRECIPITATON")

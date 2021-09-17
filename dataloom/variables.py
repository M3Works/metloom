from dataclasses import dataclass


@dataclass
class SensorDescription:
    code: str = "-1"
    name: str = "basename"
    description: str = None


class VariableBase:
    PRECIPITATION = SensorDescription()
    SWE = SensorDescription()

    @staticmethod
    def _validate_sensor(sensor: SensorDescription):
        default = SensorDescription()
        if sensor.name == default.name and sensor.code == default.code:
            raise ValueError(f"{sensor.name} is the default implementation")

    @classmethod
    def from_code(cls, code):
        for k, v in cls.__dict__.items():
            if isinstance(v, SensorDescription) and v.code == str(code):
                cls._validate_sensor(v)
                return v
        raise ValueError(f"Could not find sensor for code {code}")


class CdecStationVariables(VariableBase):
    """
    Available sensors from CDEC.
    Exhaustive list: http://cdec4gov.water.ca.gov/reportapp/javareports?name=SensList
    """
    PRECIPITATION = SensorDescription("2", "PRECIPITATION", "PRECIPITATION, ACCUMULATED")
    SWE = SensorDescription("3", "SWE", "SNOW, WATER CONTENT")
    # TEMPERATURE = SensorDescription("4", "TEMPERATURE", "TEMPERATURE, AIR")
    TEMPERATURE = SensorDescription("30", "TEMPERATURE", "TEMPERATURE, AIR AVERAGE")
    #     PRECIPITATION_INC = 45, "PPT INC"
    #     TEMPERATURE = 4, "TEMP",
    #     TEMP_AVG = 30, "TEMP AV"
    #     TEMP_MIN = 32, "TEMP MN"
    #     TEMP_MAX = 31, "TEMP MX"
    #     SWE = 3, "SNOW WC"

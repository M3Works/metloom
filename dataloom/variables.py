from dataclasses import dataclass


@dataclass
class SensorDescription:
    # TODO: Could we add an accumulated boolean here so that we can do accumulated vs non accumulated gets?
    code: str = "-1"
    name: str = "basename"
    description: str = None
    accumulated: bool = False


class VariableBase:
    PRECIPITATION = SensorDescription()
    SWE = SensorDescription()
    SNOW_DEPTH = SensorDescription()

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
    Exhaustive list:
    http://cdec4gov.water.ca.gov/reportapp/javareports?name=SensList
    """
    PRECIPITATION = SensorDescription("2", "PRECIPITATION",
                                      "PRECIPITATION, ACCUMULATED", True)
    SWE = SensorDescription("3", "SWE", "SNOW, WATER CONTENT", True)
    # TEMPERATURE = SensorDescription("4", "TEMPERATURE", "TEMPERATURE, AIR")
    TEMPERATURE = SensorDescription("30", "TEMPERATURE",
                                    "TEMPERATURE, AIR AVERAGE")
    SNOWDEPTH = SensorDescription("18", "SNOWDEPTH", "SNOW DEPTH")
    MEANDAILYFLOW = SensorDescription("41", "MEAN FLOW", "FLOW, MEAN DAILY")  # seems like the most useful
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
    # 263	WY 10%	Y1	WY 10% FORECAST EXCEEDANCE	AF
    # 264	WY 50%	Y5	WY 50% FORECAST EXCEEDANCE	AF
    # 265	WY 90%	Y9	WY 90% FORECAST EXCEEDANCE	AF
    # SWEADJ = SensorDescription("82", "SWE", "WATER CONTENT(REVISED)")
    # 82	SNO ADJ	SM	SNOW, WATER CONTENT(REVISED)	INCHES
    # 237	SNWCMIN	S5	SNOW WATER CONTENT, MIN	INCHES
    # 238	SNWCMAX	S6	SNOW WATER CONTENT, MAX	INCHES
    # 80	PPT ADJ	PF	PRECIPITATION, REVISED	INCHES
    # 103	SLRR AV	RV	SOLAR RADIATION AVG	W/M^2
    # 16	RAINTIP	PU	PRECIPITATION, TIPPING BUCKET	INCHES
    #     PRECIPITATION_INC = 45, "PPT INC"
    #     TEMPERATURE = 4, "TEMP",
    #     TEMP_AVG = 30, "TEMP AV"
    #     TEMP_MIN = 32, "TEMP MN"
    #     TEMP_MAX = 31, "TEMP MX"
    #     SWE = 3, "SNOW WC"

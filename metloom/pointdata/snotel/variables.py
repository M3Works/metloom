from dataclasses import field, make_dataclass

from metloom.sensors import SensorDescription, VariableBase

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
            field(default=SensorDescription("TAVG", "AVG AIR TEMP", "AIR TEMPERATURE AVERAGE")),
        ),
        (
            "TEMPMIN",
            SensorDescription,
            field(default=SensorDescription("TMIN", "MIN AIR TEMP", "AIR TEMPERATURE MINIMUM")),
        ),
        (
            "TEMPMAX",
            SensorDescription,
            field(default=SensorDescription("TMAX", "MAX AIR TEMP", "AIR TEMPERATURE MAXIMUM")),
        ),
        (
            "PRECIPITATION",
            SensorDescription,
            field(default=SensorDescription("PRCPSA", "PRECIPITATION", "PRECIPITATION INCREMENT SNOW-ADJUSTED")),
        ),
        (
            "PRECIPITATIONACCUM",
            SensorDescription,
            field(default=SensorDescription("PREC", "ACCUMULATED PRECIPITATION", "PRECIPITATION ACCUMULATION")),
        ),
        # TODO for the SCAN network this appears to be "RHUM", we may need a new class
        (
            "RH",
            SensorDescription,
            field(default=SensorDescription("RHUMV", "Relative Humidity", "RELATIVE HUMIDITY")),
        ),
        (
            "STREAMVOLUMEOBS",
            SensorDescription,
            field(default=SensorDescription("SRVO", "STREAM VOLUME OBS", "STREAM VOLUME OBS")),
        ),
        (
            "STREAMVOLUMEADJ",
            SensorDescription,
            field(default=SensorDescription("SRVOX", "STREAM VOLUME ADJ", "STREAM VOLUME ADJ")),
        ),
    ]
    + [
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
    ]
    + [
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
    ]
    + [
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

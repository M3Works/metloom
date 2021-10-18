=====
Usage
=====

Use metloom to find data for a station::

    from datetime import datetime
    from metloom.pointdata import SnotelPointData

    snotel_point = SnotelPointData("713:CO:SNTL", "MyStation")
    df = snotel_point.get_daily_data(
        datetime(2020, 1, 2), datetime(2020, 1, 20),
        [snotel_point.ALLOWED_VARIABLES.SWE]
    )
    print(df)

Use metloom to find snow courses within a geometry::

    from metloom.pointdata import CDECPointData
    import geopandas as gpd

    fp = <path to shape file>
    obj = gpd.read_file(fp)

    vrs = [
        CdecStationVariables.SWE,
        CdecStationVariables.SNOWDEPTH
    ]
    points = CDECPointData.points_from_geometry(obj, vrs, snow_courses=True)
    df = points.to_dataframe()
    print(df)

Not all of the available variables for each datasource are implemented
within this package. It is easy to extend the classes to add more variables

.. code-block:: python

    from datetime import datetime
    from metloom.variables import CDECStationVariables:
    from metloom.pointdata import CDECPointData


    class MyVariables(CDEcStationVariables):
        DEWPT = SensorDescription("36", "Dew Point", "TEMPERATURE, DEW POINT")


    class MyCDECPointData(CDECPointData):
        ALLOWED_VARIABLES = MyVariables


    MyCDECPointData("TNY", "Tenaya Lake").get_daily_data(
        datetime(2020, 1, 3), datetime(2020, 1, 7), [MyVariables.DEWPT]
    )

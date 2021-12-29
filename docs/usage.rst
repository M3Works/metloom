=====
Usage
=====

SNOTEL
------
Use metloom to find data for a SNOTEL station::

    from datetime import datetime
    from metloom.pointdata import SnotelPointData

    snotel_point = SnotelPointData("713:CO:SNTL", "MyStation")
    df = snotel_point.get_daily_data(
        datetime(2020, 1, 2), datetime(2020, 1, 20),
        [snotel_point.ALLOWED_VARIABLES.SWE]
    )
    print(df)


CDEC
----

Use metloom to find snow courses within a geometry from CDEC::

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

Mesowest
--------
You can also use the Mesowest network if you sign up for an API token which is
free!

1. Create/Copy token from `synoptics labs <https://developers.synopticdata.com/signup/>`_
2. Create :code:`~/.synoptic_token.json`
3. Copy your token and place it in the file like :code:`{"token":"<MY_TOKEN_HERE>"}` and save it.
4. Protect the file using :code:`chmod 600 ~/.synoptic_token.json`

To pull stations using Mesowest::

    from datetime import datetime
    from metloom.pointdata import MesowestPointData

    meso_point = MesowestPointData("ITD48", "IDAHOME")
    df = snotel_point.get_daily_data(
        datetime(2021, 12, 25), datetime(2021, 12, 26),
        [meso_point.ALLOWED_VARIABLES.TEMP]
    )
    print(df)



My variables aren't here
------------------------
Not all of the available variables for each datasource are implemented
within this package. It is easy to extend the classes to add more variables
Below is an example on how to add more variables. This is also a great to
get started contributing to metloom!

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

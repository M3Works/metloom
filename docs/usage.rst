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

USGS
----

You can use ``point.get_daily_data()`` similarly to the SNOTEL example above to get
daily data, and ``USGSPointData.points_from_geometry()`` similarly to the CDEC example above. To
use metloom to find 15-minute streamflow from USGS::

    from metloom.pointdata import USGSPointData
    from datetime import datetime

    stn_code = "13206000"
    stn_name = "BOISE RIVER AT GLENWOOD BRIDGE NR BOISE ID"

    point = USGSPointData(stn_code, stn_name)

    df = point.get_instantaneous_data(
        datetime(2020, 1, 2), datetime(2020, 1, 20),
        [point.ALLOWED_VARIABLES.DISCHARGE]
    )
    print(df)


National Weather Service (NWS) Forecast
---------------------------------------

The NWS forecast pulls the current forecast starting from today. When defining
a point, give it your own name and id, and make sure to provide the latitude
and longitude as a ``shapely point`` for the initial metadata.

Then you can use ``get_daily_forecast`` or ``get_hourly_forecast``
to retrive data.

**Note: the data will be aggregated to hourly or daily using mean or sum depending**
**on ``accumulated=True`` on the variable description**

Also - the point metadata is the **center of the NWS pixel** containing
your initial input point.

Example of pulling the daily forecast::

    from metloom.pointdata import NWSForecastPointData
    from metloom.variables import NWSForecastVariables
    from shapely.geometry import Point

    inintial_point = Point(-119, 43)
    pt = NWSForecastPointData(
        "my_point_id", "my_point_name", initial_metadata=inintial_point
    )

    df = pt.get_daily_forecast([
        NWSForecastVariables.TEMP,
        NWSForecastVariables.PRECIPITATIONACCUM,
    ]



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

Center for Snow and Avalanche Studies (CSAS)
--------------------------------------------
There are 4 stations of interest maintained by the CSAS. Senator Beck Study plot,
Swamp Angel Study Plot, Senator Beck Stream Gauge and Putney Study plot. These four stations
contain a wealth of data useful for studying and validating snow processes. The files exist as a
flat csv file so requests using this will simply download the file, interpret the datetime
index and crop according to your request. Since it is a CSV the file will be stored in a local cache
in the same directory you ran your code. This way the download times are reduced.

Additionally, the CSAS data is not available in realtime (at least as of June 2024).
Data is updated annually and stored on the website. Metloom will try to stay as up to date as
possible when the files are updated. Please feel free to submit a PR if you know the data has been
updated. Checkout the `facilities page <https://snowstudies.org/csas-facilities/>`_ on CSAS to see more about the stations.

To pull stations using CSAS::

    from metloom.pointdata import CSASMet
    from metloom.variables import CSASVariables
    from datetime import datetime

    start = datetime(2023, 1, 1)
    end = datetime(2023, 6, 1)
    sbsp = CSASMet('SBSP')
    df_sbsp = sbsp.get_daily_data(start, end, [CSASVariables.SNOWDEPTH])

If you use these data, please use the `appropriate citations <https://snowstudies.org/data-use-policy/>`_ and give credit to the
institution.

Center for Western Weather and Water Extremes (CW3E)
----------------------------------------------------
CW3E maintains surface met stations across many western US watersheds and
shares them on their `datashare <https://cw3e-datashare.ucsd.edu/CW3E_SurfaceMetObs/>`_.
Metloom reads the hourly files, which are stored as one file per station per day
in UTC. Each requested day is downloaded to a local cache (``./cache/CW3E/<station id>``)
so repeat requests are fast. Files from the last couple days are re-downloaded
since they may still be updating. Daily data is resampled from the hourly data on UTC days.

Column layouts differ between stations and sometimes change through time, so
metloom reads each station's hourly README to find the columns. Files with a column
layout not described in the README are skipped with a warning. Not every station has
every variable. These data have not been quality controlled by CW3E. Station locations
come from the `CW3E site metadata <https://cw3e.ucsd.edu/metadata/sites>`_.

To pull stations using CW3E::

    from metloom.pointdata import CW3EPointData
    from metloom.variables import CW3EVariables
    from datetime import datetime

    start = datetime(2024, 10, 1)
    end = datetime(2025, 4, 1)
    ski = CW3EPointData('SKI')
    df = ski.get_daily_data(
        start, end, [CW3EVariables.TEMP, CW3EVariables.SNOWDEPTH]
    )

SnowEx
------
During the `NASA SnowEx campaign <https://snow.nasa.gov/campaigns/snowex>`_
there were a handful of met stations deployed which are now published on the
`NSIDC <https://nsidc.org/data/snex_met/versions/1>`_. These stations have been
mapped into metloom to increase the utility/convenience of these data. The SnowEx
data is in a csv file format and thus any queries will download the appropriate
files to a local cache to reduce download times. For this to work you need to have
a `.netrc` and an account with the NSIDC. See the
`access guide <https://nsidc.org/data/user-resources/help-center/programmatic-data-access-guide>`_
for more help.

To pull stations using SnowEx::

    from metloom.pointdata import SnowExMet
    from metloom.variables import SnowExVariables
    from datetime import datetime

    start = datetime(2020, 1, 1)
    end = datetime(2020, 6, 1)

    # Grand Mesa Study Plot
    gmsp = SnowExMet('GMSP')
    df_gmsp = gmsp.get_daily_data(start, end, [SnowExVariables.SNOWDEPTH])

My variables aren't here
------------------------
Not all of the available variables for each datasource are implemented
within this package. It is easy to extend the classes to add more variables
Below is an example on how to add more variables. This is also a great to
get started contributing to metloom!

.. code-block:: python

    from datetime import datetime
    from metloom.variables import CdecStationVariables, SensorDescription
    from metloom.pointdata import CDECPointData


    class MyVariables(CdecStationVariables):
        """
        SensorDescription("<variable code>", "Desired Name", "Description")
        CDEC variable codes are available with CDEC station metadata
        """
        RH = SensorDescription("12", "Relative Humidity", "RELATIVE HUMIDITY [%]")
        WINDSP = SensorDescription("9", "Wind Speed", "WIND SPEED [mph]")


    class MyCDECPointData(CDECPointData):
        ALLOWED_VARIABLES = MyVariables


    variables = [MyVariables.RH, MyVariables.WINDSP]
    stn = MyCDECPointData("TNY", "Tenaya Lake")
    df = stn.get_daily_data(datetime(2021, 12, 25), datetime(2021, 12, 26), variables)

    print(df[['Relative Humidity', 'Wind Speed']])

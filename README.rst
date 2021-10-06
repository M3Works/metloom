========
metloom
========


.. image:: https://img.shields.io/pypi/v/metloom.svg
        :target: https://pypi.python.org/pypi/metloom
.. image:: https://github.com/M3Works/metloom/actions/workflows/testing.yml/badge.svg
        :target: https://github.com/M3Works/metloom/actions/workflows/testing.yml
        :alt: Testing Status
.. image:: https://readthedocs.org/projects/metloom/badge/?version=latest
        :target: https://metloom.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status
.. image:: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/micah-prime/04da387b53bdb4a3aa31253789550a9f/raw/metloom__heads_main.json
        :target: https://github.com/M3Works/metloom
        :alt: Code Coverage


Location Oriented Observed Meteorology

metloom is a python library created with the goal of consistent, simple sampling of
meteorology and snow related point measurments from a variety of datasources across the
Western US.

Warning - This software is provided as is (see the license), so use at your own risk.
This is an opensource package with the goal of making data wrangling easier. We make
no guarantees about the quality or accuracy of the data and any interpretation of the meaning
of the data is up to you.


* Free software: BSD license


Features
--------

* Sampling of daily, hourly, and snow course data
* Searching for stations from a datasource within a shapefile
* Current data sources:
    * `CDEC <https://cdec.water.ca.gov/>`_
    * `SNOTEL <https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService/webServiceReference/>`_

Requirements
------------
python >= 3.7

Install
-------
.. code-block:: bash

    python3 -m pip install metloom


Local install for dev
---------------------
The recommendation is to use virtualenv, but other local python
environment isolation tools will work (pipenv, conda)
.. code-block:: bash

    python3 -m pip install --upgrade pip
    python3 -m pip install -r requirements_dev
    python3 -m pip install .

Testing
-------

.. code-block:: bash

    pytest

If contributing to the codebase, code coverage should not decrease
from the contributions. Make sure to check code coverage before
opening a pull request.

.. code-block:: bash

    pytest --cov=metloom

Documentation
-------------
readthedocs coming soon

https://metloom.readthedocs.io.

Usage Notes
-----------
See usage documentation https://metloom.readthedocs.io/en/latest/usage.html

**NOTES:**
PointData methods that get point data return a GeoDataFrame indexed
on *both* datetime and station code. To reset the index simply run
``df.reset_index(inplace=True)``

Extending classes for your own variables
----------------------------------------
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


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

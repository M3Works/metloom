========
dataloom
========


.. image:: https://img.shields.io/pypi/v/dataloom.svg
        :target: https://pypi.python.org/pypi/dataloom

.. image:: https://img.shields.io/travis/M3Works/dataloom.svg
        :target: https://travis-ci.com/M3Works/dataloom

.. image:: https://readthedocs.org/projects/dataloom/badge/?version=latest
        :target: https://dataloom.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status
.. image:: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/micah-prime/04da387b53bdb4a3aa31253789550a9f/raw/dataloom__pull_##.json
        :target: https://github.com/M3Works/dataloom
        :alt: Code Coverage





Location Oriented Observed Meteorology


* Free software: BSD license
* Documentation: https://dataloom.readthedocs.io.


Features
--------

* TODO

In progress
---------
* QA/QC based on data source
    Example needs QCd
    ::
        CDECStation("DAN", "Dana Meadows").get_snow_course_data(datetime(2021, 1, 1), datetime(2021, 5, 1), [CdecStationVariables.SWE, CdecStationVariables.SNOWDEPTH])
        CDECStation("DAN", "Dana Meadows").get_daily_data(datetime(2021, 1, 1), datetime(2021, 5, 1), [CdecStationVariables.SWE, CdecStationVariables.SNOWDEPTH])

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

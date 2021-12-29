=======
History
=======

0.1.1 (2021-10-05)
------------------

* This is the first release!
* Create the package
* Add CDEC functionality
* Add SNOTEL functionality
* Add CLI to find stations from shapefile

0.1.2 (2021-10-15)
------------------

* Remove dependency on climata to fix suds-jurko issue when building with newest setuptools
* Write a custom Snotel client using zeep

0.1.3 (2021-10-25)
------------------

* Filter out _unused columns resulting from joining multiple sensor result dataframes in CDEC and SNOTEL point data
* Change CDEC timezone to avoid impossible date logic when datetimes returned from the sensors don't exist in US/Pacific timezone
* Account for nodata returns when validating dataframes
* Include datasource in dataframes

0.1.4 (2021-11-08)
------------------

* Add more network options to Snotel API usage
* Fix dataframe joining bug

0.1.5 (2021-12-23)
------------------

* Adjust filtering to snow course logic in CDEC client to handle perviously unhandled scenario
* Only include measurementDate column with snow courses

0.2.0 (2021-12-29)
------------------

* Added mesowest
* Added in a token json arg to the get_*_data functions
* Pinned docutils for an update that happened
* Added in a resample_df function for the highway stations where the returned data is 5min for air temp.

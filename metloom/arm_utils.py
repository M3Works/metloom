"""
Utility functions for U.S. Department of Energy’s Atmospheric Radiation Measurement (ARM) User Facility.
https://www.arm.gov/

The download tools here will modelled after information from the ARM Live Data Webservice
https://adc.arm.gov/armlive/register#web_services
"""

import os
import concurrent.futures
import logging
import pathlib
import urllib
import requests
import pandas
import xarray
import typing
from datetime import date, datetime

LOG = logging.getLogger(__name__)


def get_station_data(
    *,
    site: str,
    measurement: str,
    facility_code: str,
    data_level: str,
    start: typing.Union[date, datetime] = None,
    end: typing.Union[date, datetime] = None,
    access_token=None,
    user_id=None,
    url: str = "https://adc.arm.gov/armlive/data",
    destination: os.PathLike = ".armdata",
) -> pandas.DataFrame:
    """
    Get data from the ARM Live Data Webservice as a pandas DataFrame.

    This function queries the ARM Live Data Webservice with the provided parameters,
    downloads the resulting files, and returns their contents as a pandas DataFrame.

    Args:
        site: Site identifier code (e.g., 'sgp', 'nsa', etc.)
        measurement: Measurement identifier code.
        facility_code: Facility identifier code.
        data_level: Data processing level (e.g., 'c1', 'b1', etc.)
        access_token: ARM access token. Defaults to M3W_ARM_ACCESS_TOKEN environment variable.
        user_id: ARM user ID. Defaults to M3W_ARM_USER_ID environment variable.
        start: Start date in format 'YYYY-MM-DD'. Defaults to None, which downloads all data.
        end: End date in format 'YYYY-MM-DD' or 'YYYY-MM-DD'. Defaults to None, which downloads all data.
        url: Base URL for the ARM Live Data Webservice. Defaults to 'https://adc.arm.gov/armlive/data'.

    Returns:
        pandas.DataFrame: DataFrame containing the requested ARM data.

    Raises:
        AssertionError: If user_id or access_token is not provided.
        HTTPError: If the API request fails.
    """
    # set the default values for access_token and user_id
    access_token = access_token or os.getenv("M3W_ARM_ACCESS_TOKEN", None)
    user_id = user_id or os.getenv("M3W_ARM_USER_ID", None)

    # check if user_id and access_token are provided
    assert user_id is not None, "user_id must be provided, set M3W_ARM_USER_ID environment variable"
    assert access_token is not None, "access_token must be provided, set M3W_ARM_ACCESS_TOKEN environment variable"

    #  define the parameters for the query
    params = dict(
        user=f"{user_id}:{access_token}",
        ds=(f"{site.lower()}{measurement.lower()}{facility_code.upper()}.{data_level.lower()}"),
        wt="json",
    )
    if start is not None:
        start = datetime.fromisoformat(start) if isinstance(start, str) else start
        params["start"] = start.date().isoformat() if hasattr(start, "date") else start.isoformat()
    if end is not None:
        end = datetime.fromisoformat(end) if isinstance(end, str) else end
        params["end"] = end.date().isoformat() if hasattr(end, "date") else end.isoformat()

    query_url = urllib.parse.urljoin(url, "query")
    LOG.debug(f"Query: {query_url}")

    # make the request to the ARM Live Data Webservice
    response = requests.get(query_url, params=params)
    response.raise_for_status()

    # extract the files from the response
    files = response.json().get("files", [])
    if not files:
        LOG.warning("No files found for the given parameters, check that data exists for the given dates.")
        return

    # download the files in parallel
    LOG.info(f"Downloading {len(files)} files...this may take several minutes.")
    local_files = list()
    kwargs = dict(user_id=user_id, access_token=access_token, url=url, destination=destination)
    os.makedirs(destination, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        jobs = {executor.submit(_download_arm_file, file=file, **kwargs): file for file in files}
        for future in concurrent.futures.as_completed(jobs):
            try:
                data = future.result()
                local_files.append(data)
            except Exception as e:
                LOG.error(f"Failed to download {jobs[future]}:\n{e}")

    # create DataFrame from the downloaded files
    LOG.info(f"Reading {len(local_files)} files into DataFrame...")
    dfs = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        jobs = {executor.submit(_read_arm_file, file=f): f for f in local_files}
        for future in concurrent.futures.as_completed(jobs):
            try:
                data = future.result()
                dfs.append(data)
            except Exception as e:
                LOG.error(f"Failed to read {jobs[future]}:\n{e}")

    df = pandas.concat(dfs)
    df.sort_index(inplace=True)
    df.index.name = "datetime"
    return df


def _download_arm_file(
    user_id: str,
    access_token: str,
    url: str,
    destination: os.PathLike,
    file: str,
) -> os.PathLike:
    """
    Download a file from the ARM Live Data Webservice, using /saveData
    """

    output = pathlib.Path(destination, file)
    if output.exists():
        LOG.debug(f"File {output} already exists, skipping download.")
        return output

    else:
        save_data_url = urllib.parse.urljoin(url, "saveData")
        params = dict(
            user=f"{user_id}:{access_token}",
            file=file,
        )

        LOG.debug(f"Downloading {file} to {output}")
        response = requests.get(save_data_url, params=params)
        response.raise_for_status()
        with open(output, "wb") as f:
            f.write(response.content)

        return output


def _read_arm_file(file: os.PathLike) -> pandas.DataFrame:
    """
    Read NetCDF file as a pandas DataFrame
    """
    try:
        data = xarray.open_dataset(file, engine="netcdf4")
    except Exception as e:
        LOG.error(f"Failed to read {file}: {e}")
        return None

    df = data.to_dataframe()
    print(df)
    return df[~df.index.duplicated(keep="first")]

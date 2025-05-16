"""
Utility functions for U.S. Department of Energy’s Atmospheric Radiation Measurement (ARM) User Facility.
https://www.arm.gov/

The download tools here will modelled after information from the ARM Live Data Webservice
https://adc.arm.gov/armlive/register#web_services
"""

import os
import re
import concurrent.futures
import pydash
import logging
import pathlib
import json
import requests
import pandas as pd
import typing
from datetime import date, datetime

LOG = logging.getLogger(__name__)
ARM_FILE_DATE_REGEX = re.compile(r".*?\.(?P<date>\d{8})\.\.*")


def get_station_data(
    *,
    site: str,
    measurement: str,
    facility_code: str,
    data_level: str,
    variables: list[str],
    start: typing.Union[date, datetime] = None,
    end: typing.Union[date, datetime] = None,
    token_json: pathlib.Path = pathlib.Path("~/.arm_token.json").expanduser(),
    access_token=None,
    user_id=None,
    destination: pathlib.Path = pathlib.Path(".armdata"),
    download_chunk_size: int = 20,
    url: str = "https://adc.arm.gov/armlive/data",
) -> pd.DataFrame:
    """
    Get data from the ARM Live Data Webservice as a pandas DataFrame.

    This function queries the ARM Live Data Webservice with the provided parameters,
    downloads the resulting files, and returns their contents as a pandas DataFrame.

    Args:
        site: Site identifier code (e.g., 'sgp', 'nsa', etc.)
        measurement: Measurement identifier code.
        facility_code: Facility identifier code.
        data_level: Data processing level (e.g., 'c1', 'b1', etc.)
        variables: List of variable names to download.
        start: Start date/datetime. Defaults to None, which downloads all available data.
        end: End date/datetime. Defaults to None, which downloads all available data.
        token_json: Path to a JSON file containing the access token and user ID. Defaults to ~/.arm_token.json.
                    If provided, it will take precedence over the environment variables for access_token and user_id.
        access_token: ARM access token. Defaults to M3W_ARM_ACCESS_TOKEN environment variable.
        user_id: ARM user ID. Defaults to M3W_ARM_USER_ID environment variable.
        destination: Directory to save downloaded files. Defaults to ".armdata".
        download_chunk_size: Number of files to download in each batch. Defaults to 20.
        url: Base URL for the ARM Live Data Webservice. Defaults to "https://adc.arm.gov/armlive/data".

    Returns:
        pd.DataFrame: DataFrame containing the requested variables with datetime index.

    Raises:
        AssertionError: If user_id or access_token is not provided.
        HTTPError: If the API request fails.
    """
    if token_json.is_file():
        LOG.info(f"Loading token from {token_json}")
        with open(token_json, "r") as f:
            token = json.load(f)
        if access_token is None:
            access_token = token.get("access_token", None)
        if user_id is None:
            user_id = token.get("user_id", None)

    # set the default values for access_token and user_id
    access_token = access_token or os.getenv("M3W_ARM_ACCESS_TOKEN", None)
    user_id = user_id or os.getenv("M3W_ARM_USER_ID", None)

    # check if user_id and access_token are provided
    assert (
        user_id is not None
    ), "user_id must be provided, set M3W_ARM_USER_ID environment variable"
    assert (
        access_token is not None
    ), "access_token must be provided, set M3W_ARM_ACCESS_TOKEN environment variable"

    #  define the parameters for the query
    q_params = dict(
        user=f"{user_id}:{access_token}",
        ds=(
            f"{site.lower()}{measurement.lower()}{facility_code.upper()}.{data_level.lower()}"
        ),
        wt="json",
    )
    if start is not None:
        start = datetime.fromisoformat(start) if isinstance(start, str) else start
        q_params["start"] = (
            start.date().isoformat() if hasattr(start, "date") else start.isoformat()
        )
    if end is not None:
        end = datetime.fromisoformat(end) if isinstance(end, str) else end
        q_params["end"] = (
            end.date().isoformat() if hasattr(end, "date") else end.isoformat()
        )

    # make the request to the ARM Live Data Webservice
    LOG.info(f"Querying {url}/query for data.")
    response = requests.get(f"{url}/query", params=q_params)
    response.raise_for_status()

    # extract the files from the response
    files = sorted(response.json().get("files", []))
    if not files:
        LOG.warning(
            "No files found for the given parameters, check the values and case is correct for site, measurement, "
            "facility_code, and data_level."
        )
        return

    # download the data, with the desired variables as a single csv file
    # the ARM API allows for multiple files to be download, but when I attemped to do all of them at once it failed
    LOG.info(f"Downloading {len(files)} files, this may take a several minutes.")
    destination.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        jobs = set()
        for i, chunk in enumerate(pydash.chunk(files, download_chunk_size)):
            jobs.add(
                executor.submit(
                    _download_arm_files,
                    user_id=user_id,
                    access_token=access_token,
                    ds=q_params["ds"],
                    url=url,
                    variables=variables,
                    destination=destination,
                    files=chunk,
                )
            )
        csv_files = [
            future.result() for future in concurrent.futures.as_completed(jobs)
        ]

    # read the csv files into a single DataFrame
    LOG.info("Reading files into a single DataFrame.")
    df = pd.concat(
        [pd.read_csv(f, index_col="time", parse_dates=True) for f in csv_files],
        axis="index",
    )
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep="first")]
    df.index.name = "datetime"
    return df


def get_station_location(
    site: str,
    measurement: str,
    facility_code: str,
    data_level: str,
    start: typing.Union[date, datetime] = None,
    end: typing.Union[date, datetime] = None,
) -> typing.Tuple[float, float, float]:
    """
    Get the location of a station from the ARM Live Data Webservice.
    """

    # download the data for the given site, measurement, facility_code, and data_level
    df = get_station_data(
        site=site,
        measurement=measurement,
        facility_code=facility_code,
        data_level=data_level,
        start=start,
        end=end,
        variables=["lat", "lon", "alt"],
    )

    # it is possible that the location is not unique, so we need to check for that
    # for example if the data is from a site with a balloon sensor
    lat = _get_location_helper(df["lat"], "latitude")
    lon = _get_location_helper(df["lon"], "longitude")
    alt = _get_location_helper(df["alt"], "altitude")
    return lat, lon, alt * 3.28084  # convert meters to feet


def _get_location_helper(data: pd.Series, text: str) -> float:
    """
    Check if the data has non-unique values and log a warning if so.
    """
    unique_data = data.drop_duplicates()
    if len(unique_data) > 1:
        LOG.warning(
            f"Multiple {text} found, the mean is being provided: mean={data.mean()}, min={data.min()}, max={data.max()}"
        )
        return float(data.mean())
    return float(unique_data.iloc[0])


def _download_arm_files(
    user_id: str,
    access_token: str,
    ds: str,
    url: str,
    variables: list[str],
    destination: os.PathLike,
    files: list[str],
) -> list[os.PathLike]:
    """
    Download a list of files from the ARM Live Data Webservice, using /saveData
    """
    d0 = ARM_FILE_DATE_REGEX.search(files[0]).group("date")
    dn = ARM_FILE_DATE_REGEX.search(files[-1]).group("date")
    dv = "_".join(variables)
    output = pathlib.Path(destination, f"{ds}_{dv}_{d0}_{dn}.csv")

    if output.exists():
        LOG.debug(f"File {output} already exists, skipping download.")
        return output

    params = dict(
        user=f"{user_id}:{access_token}", variables=",".join(variables), wt="csv"
    )
    response = requests.get(f"{url}/mod", params=params, data=json.dumps(files))
    response.raise_for_status()
    with open(output, "wb") as f:
        f.write(response.content)

    return output

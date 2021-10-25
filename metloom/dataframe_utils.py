from logging import getLogger
from typing import Optional

import pandas as pd

LOG = getLogger("metloom.dataframe_utils")


def join_df(
    df: Optional[pd.DataFrame], new_df: Optional[pd.DataFrame], how="left",
    on=None, filter_unused=False
):
    """
    join two dataframes handling None
    Args:
        df: optional dataframe
        new_df: optional dataframe
        how: method for merging
        on: optional kwarg for DataFrame.join
        filter_unused: boolean, whether to filter out columns with _unused in
            then name

    Returns:
        The joined dataframes. This method prefers values from the first if
        columns are overlapping and renames the overlapping values from
        the `new_df` to <column>_unused
    """
    if df is None:
        result_df = new_df
    elif new_df is None:
        result_df = df
    else:
        try:
            result_df = df.join(new_df, how=how, on=on, rsuffix="_unused")
            if filter_unused:
                columns = result_df.columns
                final_columns = [c for c in columns if "_unused" not in c]
                result_df = result_df.filter(final_columns)
        except Exception as e:
            LOG.error("failed joining dataframes.")
            raise e

    return result_df


def append_df(df: Optional[pd.DataFrame], new_df: Optional[pd.DataFrame]):
    """
    append 2 dfs handling Nones
    Args:
        df: optional dataframe
        new_df: optional dataframe
    Returns:
        dataframe or None
    """
    if df is None:
        result_df = new_df
    elif new_df is None:
        result_df = df
    else:
        result_df = df.append(new_df)
    return result_df

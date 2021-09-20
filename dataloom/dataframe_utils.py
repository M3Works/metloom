from logging import getLogger

LOG = getLogger("dataloom.dataframe_utils")


def join_df(df, new_df, how="left", on=None):
    """

    Args:
        df:
        new_df:
        how: method for merging

    Returns:
        The joined dataframes. This method prefers values from the first if columns are overlapping
        and renames the overlapping values from the `new_df` to <column>_unused
    """
    if df is None:
        result_df = new_df
    elif new_df is None:
        result_df = df
    else:
        try:
            result_df = df.join(new_df, how=how, on=on, rsuffix="_unused")
        except Exception as e:
            LOG.error("failed joining dataframes.")
            raise e

    return result_df


def append_df(df, new_df):
    if df is None:
        result_df = new_df
    elif new_df is None:
        result_df = df
    else:
        result_df = df.append(new_df)
    return result_df

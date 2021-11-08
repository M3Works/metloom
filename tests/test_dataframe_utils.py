import numpy as np
import pandas as pd
import pytest
from datetime import datetime

from metloom.dataframe_utils import join_df, append_df, merge_df

df1 = pd.DataFrame.from_records([{"foo": 12.0}, {"foo": 11.0}])
df2 = pd.DataFrame.from_records([{"bar": 1.0}, {"bar": 1.0}])
df3 = pd.DataFrame.from_records([
    {"foo": 12.0, "bar": 1.0}, {"foo": 11.0, "bar": 1.0}
])
df4 = pd.DataFrame.from_records(
    [
        {"foo": 12.0},
        {"foo": 11.0},
        {"foo": 12.0},
        {"foo": 11.0},
    ],
    index=[0, 1, 0, 1],
)


@pytest.mark.parametrize(
    "set1, set2, expected", [(df1, df2, df3), (df1, None, df1), (None, df2, df2)]
)
def test_join_df(set1, set2, expected):
    pd.testing.assert_frame_equal(join_df(set1, set2), expected)


def test_join_df_failure():
    with pytest.raises(AttributeError):
        join_df(df1, "bad value")


@pytest.mark.parametrize(
    "set1, set2, expected", [(df1, None, df1), (None, df2, df2), (df1, df1, df4)]
)
def test_append_df(set1, set2, expected):
    pd.testing.assert_frame_equal(append_df(set1, set2), expected)


def test_merge_df():
    first = pd.DataFrame(
        {
            "datetime": [
                datetime(2020, 1, 3), datetime(2020, 1, 6),
                datetime(2020, 1, 7)
            ],
            "a": [3.0, 6.0, 7.0],
            "c": [3.0, 6.0, 7.0]
        }
    ).set_index("datetime")
    second = pd.DataFrame(
        {
            "datetime": [
                datetime(2020, 1, 2), datetime(2020, 1, 5),
                datetime(2020, 1, 6), datetime(2020, 1, 8)],
            "b": [2.0, 5.0, 6.0, 8.0],
            "c": [2.0, 5.0, 6.0, 8.0]
        },
    ).set_index("datetime")

    expected = pd.DataFrame(
        {
            "datetime": [
                datetime(2020, 1, 2), datetime(2020, 1, 3), datetime(2020, 1, 5),
                datetime(2020, 1, 6), datetime(2020, 1, 7), datetime(2020, 1, 8)],
            "a": [np.nan, 3.0, np.nan, 6.0, 7.0, np.nan],
            "c": [2.0, 3.0, 5.0, 6.0, 7.0, 8.0],
            "b": [2.0, np.nan, 5.0, 6.0, np.nan, 8.0],
        }
    ).set_index("datetime")
    result = merge_df(first, second)
    pd.testing.assert_frame_equal(expected, result)

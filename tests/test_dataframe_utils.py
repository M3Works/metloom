import pandas as pd
import pytest

from metloom.dataframe_utils import join_df, append_df

df1 = pd.DataFrame.from_records([{"foo": 12.0}, {"foo": 11.0}])
df2 = pd.DataFrame.from_records([{"bar": 1.0}, {"bar": 1.0}])
df3 = pd.DataFrame.from_records([{"foo": 12.0, "bar": 1.0}, {"foo": 11.0, "bar": 1.0}])
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

import pytest
import geopandas as gpd
import pandas as pd
from os import path


def side_effect_error(*args):
    raise ValueError("Testing error")


class TestPointData(object):
    @pytest.fixture(scope="class")
    def data_dir(self):
        this_dir = path.dirname(__file__)
        return path.join(this_dir, "data")

    @pytest.fixture(scope="class")
    def shape_obj(self, data_dir):
        fp = path.join(data_dir, "testing.shp")
        return gpd.read_file(fp)

    @staticmethod
    def expected_response(dates, vals, var, unit, station, points):
        obj = []
        for dt, v in zip(dates, vals):
            obj.append(
                {
                    "datetime": pd.Timestamp(dt, tz="UTC"),
                    "measurementDate": pd.Timestamp(dt, tz="UTC"),
                    var: v,
                    f"{var}_units": unit,
                    "site": station.id,
                    "datasource": "NRCS"
                }
            )
        df = gpd.GeoDataFrame.from_dict(
            obj,
            geometry=[points] * len(dates),
        )
        # needed to reorder the columns for the pd testing compare
        df = df.filter(
            [
                "datetime", "geometry", "site", "measurementDate",
                var, f"{var}_units", "datasource"]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df

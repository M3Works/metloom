import pytest
import geopandas as gpd
import pandas as pd
from os import path

from metloom.pointdata.base import PointData, DataValidationError


def side_effect_error(*args):
    raise ValueError("Testing error")


class TestPointData:
    def test_class_attributes(self):
        # Base implementation should fail
        with pytest.raises(AttributeError):
            PointData("foo", "bar").tzinfo


class BasePointDataTest(object):
    @pytest.fixture(scope="class")
    def data_dir(self):
        this_dir = path.dirname(__file__)
        return path.join(this_dir, "data")

    @pytest.fixture(scope="class")
    def shape_obj(self, data_dir):
        fp = path.join(data_dir, "testing.shp")
        return gpd.read_file(fp)

    @staticmethod
    def expected_response(dates, variables_map, station, points,
                          include_measurement_date=False):
        obj = []
        for idt, dt in enumerate(dates):
            # get the value and unit corresponding to the date
            row_obj = {k: v[idt] for k, v in variables_map.items()}
            entry = {
                "datetime": pd.Timestamp(dt, tz="UTC"),
                "site": station.id,
                "datasource": "NRCS",
                **row_obj
            }
            if include_measurement_date:
                entry["measurementDate"] = pd.Timestamp(dt, tz="UTC")
            obj.append(
                entry
            )
        df = gpd.GeoDataFrame.from_dict(
            obj,
            geometry=[points] * len(dates),
        )
        # needed to reorder the columns for the pd testing compare
        var_keys = list(variables_map.keys())
        var_keys.sort()
        df = df.filter(
            [
                "datetime", "geometry", "site", "measurementDate",
                *var_keys, "datasource"]
        )
        df.set_index(keys=["datetime", "site"], inplace=True)
        return df.sort_index(axis=1)


class TestPointDataValidations:
    def test_validate_gdf(self):
        with pytest.raises(DataValidationError):
            PointData._validate_geodataframe(pd.DataFrame())

    def test_validate_indicies(self):
        with pytest.raises(DataValidationError):
            PointData._validate_df_indicies(gpd.GeoDataFrame(index=["datetime"]))

    @pytest.mark.parametrize("incoming_columns", [
        # Missing datasource
        ["geometry", "measurementDate", "quality_code"],
        ["datasource"],
    ])
    def test_validate_columns(self, incoming_columns):
        gdf = gpd.GeoDataFrame(columns=incoming_columns)
        with pytest.raises(DataValidationError):
            columns = PointData.EXPECTED_COLUMNS.copy()
            PointData._validate_df_columns(gdf, columns)

    def test_validate_units(self):
        # Missing units for this variables
        incoming = ["Depth", 'SWE']
        gdf = gpd.GeoDataFrame(columns=incoming)
        with pytest.raises(DataValidationError):
            PointData._validate_df_units(gdf, PointData.EXPECTED_COLUMNS)

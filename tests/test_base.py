from geopandas import GeoDataFrame
from rasterio.rio.main import gdal_version_cb

from metloom.pointdata import PointData
from metloom.variables import VariableBase, DerivedDataDescription, SensorDescription
import pytest
import geopandas as gpd
import numpy as np


class DensityDescription(DerivedDataDescription):
    def compute(self, gdf:gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """ Compute density and infill in valid with nans"""
        result = gpd.GeoSeries(name=self.name, index=gdf.index)
        result = np.ones_like(gdf[self.required_sensors[0].name]) * np.nan

        # Assume position?
        depth = gdf[self.required_sensors[1].name]
        swe = gdf[self.required_sensors[0].name]
        valid = (depth > 0) & (swe > 0)
        result[valid] = gdf[self.required_sensors[0].name][valid] / gdf[self.required_sensors[1].name][valid]
        return result


class FakeVariables(VariableBase):
    """A fake Variable class for testing"""
    SWE = SensorDescription(name='swe')
    SNOWDEPTH = SensorDescription(name='snowdepth')
    DENSITY = DensityDescription(name='density', required_sensors=[SWE, SNOWDEPTH])


class FakePointData(PointData):
    """A fake PointData class for testing purposes."""
    ALLOWED_VARIABLES = FakeVariables


class TestPointData:
    @pytest.mark.parametrize("variables, expected", [
        # Testa a typical derived usage
        ([FakePointData.ALLOWED_VARIABLES.DENSITY], [FakePointData.ALLOWED_VARIABLES.SWE, FakePointData.ALLOWED_VARIABLES.SNOWDEPTH]),
        # Test a normal usage
        ([FakePointData.ALLOWED_VARIABLES.SWE], [FakePointData.ALLOWED_VARIABLES.SWE]),
        # Test for duplicates
        ([FakePointData.ALLOWED_VARIABLES.DENSITY, FakePointData.ALLOWED_VARIABLES.SWE], [FakePointData.ALLOWED_VARIABLES.SWE, FakePointData.ALLOWED_VARIABLES.SNOWDEPTH])
    ])
    def test_get_required_sensors(self, variables, expected):
        """Test that the required variables are returned correctly."""
        required_vars = PointData.get_required_sensors(variables)
        # Check all the variables in expected are in required_vars
        assert all([v in required_vars for v in expected])

    @pytest.mark.parametrize("variables, expected", [
        # Testa a typical derived usage
        ([FakePointData.ALLOWED_VARIABLES.DENSITY, FakePointData.ALLOWED_VARIABLES.SWE], [FakePointData.ALLOWED_VARIABLES.DENSITY]),
        # No derived variables
        ([PointData.ALLOWED_VARIABLES.SWE], []),

    ])
    def test_get_derived_variables(self, variables, expected):
        """Test that the required variables are returned correctly."""
        required_vars = PointData.get_derived_descriptions(variables)
        # Check all the variables in expected are in required_vars
        assert all([v in required_vars for v in expected])

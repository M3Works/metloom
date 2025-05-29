from metloom.pointdata import PointData
import pytest


class TestPointData:
    @pytest.mark.parametrize("variables, expected", [
        # Testa a typical derived usage
        ([PointData.ALLOWED_VARIABLES.DENSITY], [PointData.ALLOWED_VARIABLES.SWE, PointData.ALLOWED_VARIABLES.SNOWDEPTH]),
        # Test a normal usage
        ([PointData.ALLOWED_VARIABLES.SWE], [PointData.ALLOWED_VARIABLES.SWE]),
        # Test for duplicates
        ([PointData.ALLOWED_VARIABLES.DENSITY, PointData.ALLOWED_VARIABLES.SWE], [PointData.ALLOWED_VARIABLES.SWE, PointData.ALLOWED_VARIABLES.SNOWDEPTH])
    ])
    def test_get_required_variables(self, variables, expected):
        """Test that the required variables are returned correctly."""
        required_vars = PointData.get_required_variables(variables)
        # Check all the variables in expected are in required_vars
        assert all([v in required_vars for v in expected])

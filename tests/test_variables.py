import pytest

from dataloom.variables import VariableBase, CdecStationVariables


class TestBaseVariables:
    def test_validate_variable_fails(self):
        with pytest.raises(ValueError):
            VariableBase.from_code(-1)


class TestCDECStationVariables:
    @pytest.mark.parametrize(
        "code, expected",
        [
            (2, CdecStationVariables.PRECIPITATION),
            ("2", CdecStationVariables.PRECIPITATION),
            ("3", CdecStationVariables.SWE),
            (30, CdecStationVariables.AVGTEMP),
        ],
    )
    def test_from_code(self, code, expected):
        assert CdecStationVariables.from_code(code) == expected

    def test_from_code_failure(self):
        with pytest.raises(ValueError):
            CdecStationVariables.from_code(-9999)

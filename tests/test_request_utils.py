import pytest

from metloom.request_utils import no_ssl_verification


def test_no_ssl_verification_is_deprecated():
    with pytest.warns(DeprecationWarning, match="not thread-safe") as record:
        with no_ssl_verification():
            pass
    # the warning points at the caller's with statement
    assert record[0].filename == __file__

"""
Taken from
stackoverflow.com/questions/15445981/how-do-i-disable-the-security-certificate-check-in-python-requests
This was necessary to make requests to Snotel without exceptions
as we were not able to do the SSL verification.

Deprecated: the Snotel endpoint now verifies, and metloom no longer uses this.
"""
import warnings
import contextlib
import requests
from urllib3.exceptions import InsecureRequestWarning

old_merge_environment_settings = requests.Session.merge_environment_settings


@contextlib.contextmanager
def no_ssl_verification():
    warnings.warn(
        "no_ssl_verification is deprecated and will be removed in a future "
        "release: metloom no longer needs it for Snotel, and while it is "
        "active it turns off SSL verification for every requests call in "
        "the process, so it is not thread-safe.",
        DeprecationWarning,
        # past contextlib's __enter__ to the caller's with statement
        stacklevel=3,
    )
    opened_adapters = set()

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        # Verification happens only once per connection so we need to close
        # all the opened adapters once we're done. Otherwise, the effects of
        # verify=False persist beyond the end of this context manager.
        opened_adapters.add(self.get_adapter(url))

        settings = old_merge_environment_settings(
            self, url, proxies, stream, verify, cert
        )
        settings['verify'] = False

        return settings

    requests.Session.merge_environment_settings = merge_environment_settings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = old_merge_environment_settings

        for adapter in opened_adapters:
            try:
                adapter.close()
            except Exception:
                pass

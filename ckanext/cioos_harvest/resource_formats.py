"""
Resource URL classification shared by the ISO 19115-3 parser and plugin.py.

Kept free of CKAN/harvester imports so the parser module can use it without
creating an import cycle with ``ckanext.cioos_harvest.harvesters``.
"""

from urllib.parse import urlparse

_OBIS_HOSTS = ("obis.org", "iobis.org")


def is_obis_url(url):
    """Return True if *url* is hosted on OBIS (obis.org, iobis.org or a subdomain).

    Matches on the host so that unrelated URLs merely containing "obis"
    are not labelled as OBIS resources.
    """
    host = (urlparse(url.strip()).hostname or "").lower()
    return any(host == h or host.endswith("." + h) for h in _OBIS_HOSTS)

"""Tests for plugin.py."""

import pytest

from ckanext.cioos_harvest.model.iso19115_3 import _guess_format_from_url
from ckanext.cioos_harvest.plugin import Cioos_HarvestPlugin


def test_plugin():
    pass


_OBIS_CASES = [
    ("https://obis.org/dataset/2a3a5a0f-1a7c-4b8c-9d5c-0b5e3c1e7f11", True),
    ("https://OBIS.org/dataset/abc", True),
    ("https://ipt.iobis.org/obiscanada/resource?r=dfo_maritimes", True),
    ("https://mapper.obis.org/?datasetid=abc", True),
    ("https://example.com/notobis.org/data", False),
    ("https://www.myobis-portal.ca/data", False),
]


@pytest.mark.parametrize("url,is_obis", _OBIS_CASES)
def test_cioos_guess_resource_format_obis(url, is_obis):
    fmt = Cioos_HarvestPlugin().cioos_guess_resource_format(url)
    assert (fmt == "OBIS") is is_obis


@pytest.mark.parametrize("url,is_obis", _OBIS_CASES)
def test_iso19115_3_guess_format_obis(url, is_obis):
    assert (_guess_format_from_url(url) == "OBIS") is is_obis

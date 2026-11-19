"""Tests for harvesters/ckan_cioos.py."""

import pytest
import responses
from urllib3.util.retry import Retry

from ckanext.cioos_harvest.harvesters import ckan_cioos
from ckanext.cioos_harvest.harvesters.ckan_cioos import CIOOSCKANHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

URL = "https://remote.example/api/3/action/package_search"


@pytest.fixture
def harvester(monkeypatch):
    monkeypatch.setattr(
        ckan_cioos,
        "REMOTE_RETRY",
        Retry(total=2, backoff_factor=0, status_forcelist=(502, 503, 504)),
    )
    h = CIOOSCKANHarvester()
    h.config = {}
    return h


@responses.activate
def test_get_content_retries_transient_502(harvester):
    responses.get(URL, status=502, body="<html>Bad gateway</html>")
    responses.get(URL, json={"result": {"results": []}})

    assert '"results"' in harvester._get_content(URL)


@responses.activate
def test_get_content_raises_upstream_error_when_retries_run_out(harvester):
    for _ in range(3):
        responses.get(URL, status=502, body="<html>Bad gateway</html>")

    with pytest.raises(ContentFetchError):
        harvester._get_content(URL)


@responses.activate
def test_get_content_raises_on_client_error(harvester):
    responses.get(URL, status=404)

    with pytest.raises(ContentFetchError, match="404"):
        harvester._get_content(URL)


def test_search_error_is_the_one_upstream_raises():
    # gather_stage must catch the class _search_for_datasets raises, or a
    # remote failure crashes the gather consumer instead of logging an error.
    assert ckan_cioos.SearchError is SearchError


def test_owner_org_by_name_uses_the_remote_org_name():
    pkg = {"owner_org": "8f1ade88-id", "organization": {"id": "8f1ade88-id", "name": "dfo-mpo"}}

    assert ckan_cioos.owner_org_by_name(pkg)["owner_org"] == "dfo-mpo"


def test_owner_org_by_name_keeps_owner_org_without_remote_org():
    assert ckan_cioos.owner_org_by_name({"owner_org": "abc"})["owner_org"] == "abc"
    assert ckan_cioos.owner_org_by_name({"organization": None}) == {"organization": None}

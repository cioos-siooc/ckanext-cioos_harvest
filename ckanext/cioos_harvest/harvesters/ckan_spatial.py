"""CKAN Spatial Harvester.

Harvests remote CKAN instances, filtering datasets by spatial query
against the remote instance's spatial search API.
"""

import json
import logging

import requests
from ckan.lib.search import SearchError
from requests.exceptions import HTTPError, RequestException
from urllib3.contrib import pyopenssl

from ckanext.cioos_harvest.harvesters.base import (
    ContentFetchError,
    deduplicate_catalogue_entries,
    make_site_catalogue_entry,
    singleton_new,
)
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

log = logging.getLogger(__name__)


class CKANSpatialHarvester(CKANHarvester):
    __new__ = singleton_new

    def _post_content(self, url, params={}):

        headers = {}
        api_key = self.config.get("api_key")
        if api_key:
            headers["Authorization"] = api_key

        pyopenssl.inject_into_urllib3()

        try:
            http_request = requests.post(url, headers=headers, json=params)
        except HTTPError as e:
            raise ContentFetchError(
                "HTTP error: %s %s" % (e.response.status_code, e.request.url)
            )
        except RequestException as e:
            raise ContentFetchError("Request error: %s" % e)
        except Exception as e:
            raise ContentFetchError("HTTP general exception: %s" % e)
        return http_request.text

    def info(self):
        return {
            "name": "ckan_spatial",
            "title": "CKAN Spatial",
            "description": "Harvests remote CKAN instances filtering by spatial query",
            "form_config_interface": "Text",
        }

    def modify_package_dict(self, package_dict, harvest_object):
        # Strip remote harvest extras so they don't shadow the local
        # values that ckanext-harvest's before_dataset_index adds to
        # the Solr index.
        _remote_harvest_keys = {
            "harvest_source_id",
            "harvest_source_url",
            "harvest_source_title",
            "harvest_object_id",
        }
        package_dict["extras"] = [
            e for e in package_dict.get("extras", [])
            if e["key"] not in _remote_harvest_keys
        ]

        # provide default values if harvesting from a ckan catalogue that does not have these in their schema
        if not package_dict.get("projects"):
            package_dict["projects"] = []
        if not package_dict.get("datacentre"):
            package_dict["datacentre"] = []

        # populate publishing data catalogue list
        dc = make_site_catalogue_entry()
        if not package_dict.get("included_in_data_catalogue"):
            package_dict["included_in_data_catalogue"] = [dc]
        else:
            package_dict["included_in_data_catalogue"].append(dc)
            package_dict["included_in_data_catalogue"] = deduplicate_catalogue_entries(
                package_dict["included_in_data_catalogue"]
            )

        return package_dict

    def modify_search(self, pkg_dicts, remote_ckan_base_url, fq_terms):
        ss_params = {}
        spatial_filter_file = self.config.get("spatial_filter_file", None)
        if spatial_filter_file:
            f = open(spatial_filter_file)
            spatial_filter_wkt = f.read()
        else:
            spatial_filter_wkt = self.config.get("spatial_filter", None)
        if spatial_filter_wkt.startswith(("POLYGON", "MULTIPOLYGON")):
            ss_params["poly"] = spatial_filter_wkt
        if spatial_filter_wkt.startswith("BOX"):
            ss_params["bbox"] = spatial_filter_wkt[4:-1]
        ss_params["crs"] = self.config.get("spatial_crs", 4326)
        spatial_id_list = []
        if spatial_filter_wkt:
            spatial_search_url = remote_ckan_base_url + "/api/2/search/dataset/geo"
            try:
                ss_content = self._post_content(spatial_search_url, ss_params)
            except ContentFetchError as e:
                raise SearchError(
                    "Error sending request to spatial search remote "
                    "CKAN instance %s using URL %r. Error: %s"
                    % (remote_ckan_base_url, spatial_search_url, e)
                )
            try:
                ss_response_dict = json.loads(ss_content)
            except ValueError:
                raise SearchError(
                    "Spatial Search response from remote CKAN was not JSON: %r"
                    % ss_content
                )
            try:
                spatial_id_list = ss_response_dict.get("results", [])
            except ValueError:
                raise SearchError(
                    "Response JSON did not contain results list: %r" % ss_response_dict
                )

        # Filter out packages not found by spatial search
        pkg_dicts = [p for p in pkg_dicts if p["id"] in spatial_id_list]

        log.debug("Found the follow packages during spatial search:\n %r", pkg_dicts)

        return pkg_dicts

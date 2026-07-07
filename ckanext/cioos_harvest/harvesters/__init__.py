"""
CIOOS Harvest extension harvesters.

This module contains custom harvesters for CIOOS that extend the base
ckanext-harvest harvesters with CIOOS-specific functionality.
"""

from ckanext.cioos_harvest.harvesters.base import (
    ContentFetchError,
    ContentNotFoundError,
    RemoteResourceError,
    SearchError,
    all_packages_for_source,
)
from ckanext.cioos_harvest.harvesters.ckan_cioos import CIOOSCKANHarvester
from ckanext.cioos_harvest.harvesters.ckan_schema import CIOOSCKANSchemaHarvester
from ckanext.cioos_harvest.harvesters.ckan_spatial import CKANSpatialHarvester
from ckanext.cioos_harvest.harvesters.geonetwork import GeoNetworkHarvester
from ckanext.cioos_harvest.harvesters.obis import OBISHarvester
from ckanext.cioos_harvest.harvesters.waf import WAFHarvesterISO19115_3
from ckanext.cioos_harvest.harvesters.waf_datastream import DatastreamSitemapHarvester

__all__ = [
    "CIOOSCKANHarvester",
    "CIOOSCKANSchemaHarvester",
    "CKANSpatialHarvester",
    "WAFHarvesterISO19115_3",
    "DatastreamSitemapHarvester",
    "GeoNetworkHarvester",
    "OBISHarvester",
    "all_packages_for_source",
    "ContentFetchError",
    "ContentNotFoundError",
    "RemoteResourceError",
    "SearchError",
]

"""
CIOOS Harvest extension harvesters.

This module contains custom harvesters for CIOOS that extend the base
ckanext-harvest harvesters with CIOOS-specific functionality.
"""

from ckanext.cioos_harvest.harvesters.ckan_schema import CIOOSCKANSchemaHarvester
from ckanext.cioos_harvest.harvesters.waf_cioos import WAFHarvesterCIOOS
from ckanext.cioos_harvest.harvesters.waf_datastream import DatastreamSitemapHarvester
from ckanext.cioos_harvest.harvesters.geonetwork import GeoNetworkHarvester
from ckanext.cioos_harvest.harvesters.base import (
    all_packages_for_source,
    ContentFetchError,
    ContentNotFoundError,
    RemoteResourceError,
    SearchError,
)

__all__ = [
    'CIOOSCKANSchemaHarvester',
    'WAFHarvesterCIOOS',
    'DatastreamSitemapHarvester',
    'GeoNetworkHarvester',
    'all_packages_for_source',
    'ContentFetchError',
    'ContentNotFoundError',
    'RemoteResourceError',
    'SearchError',
]

# encoding: utf-8
"""
CIOOS Spatial Logic/Actions

Custom CKAN actions for spatial queries with polygon support.
Uses Solr-based spatial search via ckanext-spatial's solr-spatial-field backend.
"""
import logging
import urllib
import json

from shapely import wkt as shapely_wkt
from ckantoolkit import side_effect_free, get_action
from ckanext.spatial.lib import normalize_bbox, fit_bbox

log = logging.getLogger(__name__)


def load_json(j):
    try:
        new_val = json.loads(j)
    except Exception:
        new_val = j
    return new_val


def _build_spatial_params(data_dict):
    """Parse bbox/poly from data_dict and return spatial search parameters.

    Returns a dict with either 'ext_bbox' (for bbox queries) or 'fq' (for
    polygon queries) ready to be merged into package_search parameters.
    Returns None if no valid spatial input is found.
    """
    if 'bbox' in data_dict:
        bbox = normalize_bbox(data_dict['bbox'])
        if not bbox:
            return None
        bbox = fit_bbox(bbox)
        ext_bbox = '{minx},{miny},{maxx},{maxy}'.format(**bbox)
        return {'extras': {'ext_bbox': ext_bbox}}

    if 'poly' in data_dict:
        poly_str = urllib.parse.unquote_plus(data_dict['poly'])
        if poly_str.startswith('BOX'):
            bbox = normalize_bbox(poly_str[4:-1])
            if not bbox:
                return None
            bbox = fit_bbox(bbox)
            ext_bbox = '{minx},{miny},{maxx},{maxy}'.format(**bbox)
            return {'extras': {'ext_bbox': ext_bbox}}
        else:
            try:
                geom = shapely_wkt.loads(poly_str)
                if not geom.is_valid:
                    log.warning('Invalid polygon WKT provided: %s', poly_str)
                    return None
                fq = '{!field f=spatial_geom}Intersects(%s)' % geom.wkt
                return {'fq': fq}
            except Exception:
                log.warning('Failed to parse polygon WKT: %s', poly_str)
                return None

    return None


@side_effect_free
def spatial_query_geo(context, data_dict):
    """
    Perform a spatial query using either bbox or polygon geometry.

    :param bbox: Bounding box string (minx,miny,maxx,maxy)
    :param poly: Polygon WKT or BOX string (URL encoded)
    :returns: dict with count and list of package IDs
    """
    spatial_params = _build_spatial_params(data_dict)
    if not spatial_params:
        return []

    search_params = {'rows': 1000}
    search_params.update(spatial_params)

    result = get_action('package_search')(context, search_params)
    ids = [pkg['id'] for pkg in result.get('results', [])]

    return dict(count=len(ids), results=ids)


@side_effect_free
def spatial_query_geo_package_search(context, data_dict):
    """
    Perform a package search with spatial filtering.

    Combines spatial filtering with package_search to return full
    search results filtered by geographic extent.

    :param bbox: Bounding box string (minx,miny,maxx,maxy)
    :param poly: Polygon WKT or BOX string (URL encoded)
    :returns: package_search results filtered by spatial extent
    """
    search_params = {k: v for k, v in data_dict.items()
                     if k not in ('bbox', 'poly')}

    spatial_params = _build_spatial_params(data_dict)
    if spatial_params:
        # Merge extras dicts if both exist
        if 'extras' in spatial_params and 'extras' in search_params:
            search_params['extras'].update(spatial_params.pop('extras'))
        search_params.update(spatial_params)

    return get_action('package_search')(context, search_params)

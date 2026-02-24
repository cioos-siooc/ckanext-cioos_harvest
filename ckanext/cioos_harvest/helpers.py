# encoding: utf-8
"""
CIOOS Spatial Helpers

These helpers extend ckanext-spatial with CIOOS-specific functionality.
"""
import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
import ckan.lib.helpers as h

config = toolkit.config


def spatial_widget_expands():
    '''Return the value of the spatial_widget_expands config setting.

    To disable expanding of the spatial widget when drawing search box, add this line to the
    [app:main] section of your CKAN config file::

      ckan.spatial.spatial_widget_expands = False

    Returns ``True`` by default, if the setting is not in the config file.

    :rtype: bool

    '''
    value = config.get('ckan.spatial.spatial_widget_expands', True)
    value = toolkit.asbool(value)
    return value


def spatial_default_extent():
    '''Return the value of the ckanext.spatial.default_extent config setting.

    :rtype: text

    '''
    value = config.get('ckanext.spatial.default_extent')
    return value


def spatial_get_map_initial_max_zoom(pkg):
    '''Return the initial max zoom level for a package's spatial map.

    Uses the package's spatial_initial_max_zoom extra, or falls back to
    the ckanext.spatial.initial_max_zoom config setting, or defaults to 9.

    :param pkg: The package dict
    :rtype: int
    '''
    max_zoom = h.get_pkg_dict_extra(pkg, 'spatial_initial_max_zoom') or \
        pkg.get('spatial_initial_max_zoom') or \
        config.get('ckanext.spatial.initial_max_zoom', 9)
    return max_zoom

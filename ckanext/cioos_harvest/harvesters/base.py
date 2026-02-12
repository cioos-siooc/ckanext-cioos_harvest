"""
Base utilities and exceptions for CIOOS harvesters.

This module provides shared functionality used by multiple CIOOS harvesters.
"""

from ckan import model, logic
import ckan.plugins.toolkit as toolkit

import logging
log = logging.getLogger(__name__)


# Custom exceptions
class ContentFetchError(Exception):
    """Raised when content cannot be fetched from a remote URL."""
    pass


class ContentNotFoundError(ContentFetchError):
    """Raised when content is not found (404) at a remote URL."""
    pass


class RemoteResourceError(Exception):
    """Raised when a remote resource (group, organization) cannot be fetched."""
    pass


class SearchError(Exception):
    """Raised when a search operation fails."""
    pass


def all_packages_for_source(source_id):
    """
    Fetches all datasets belonging to a particular harvest source.

    This function handles pagination to retrieve all packages, not just
    the first page. It's used for tracking packages that need to be
    deleted when they're no longer present on the remote source.

    Args:
        source_id: The ID of the harvest source

    Returns:
        list: Array of package dictionaries
    """
    limit = 1000
    page = 1
    fq = '+harvest_source_id:"{0}"'.format(source_id)
    search_dict = {
        'fq': fq,
        'rows': limit,
        'sort': 'metadata_modified desc',
        'start': (page - 1) * limit,
    }

    context = {'model': model, 'session': model.Session}
    out = []
    query = logic.get_action('package_search')(context, search_dict)

    while query['results']:
        out = out + query['results']
        search_dict['start'] = search_dict['start'] + limit
        query = logic.get_action('package_search')(context, search_dict)

    return out


def get_object_extra(harvest_object, key):
    """
    Helper function for retrieving a value from a harvest object extra.

    Args:
        harvest_object: The HarvestObject instance
        key: The key of the extra to retrieve

    Returns:
        The value of the extra, or None if not found
    """
    for extra in harvest_object.extras:
        if extra.key == key:
            return extra.value
    return None

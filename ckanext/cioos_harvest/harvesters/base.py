"""
Base utilities and exceptions for CIOOS harvesters.

This module provides shared functionality used by multiple CIOOS harvesters:
- Custom exception classes
- JSON parsing helpers (load_json, from_json)
- Harvest object helpers (get_object_extra, get_extra)
- Tag sanitization
- Composite key flattening
- XML content extraction
- Responsible-organization group handling
- Singleton pattern for harvester classes
"""

import json
import logging
import re
import xml.etree.ElementTree as ET

import ckan.lib.munge as munge
import ckan.plugins.toolkit as toolkit
import requests
from sqlalchemy.orm.exc import StaleDataError

from ckan import logic, model
from ckanext.harvest.model import HarvestObjectError
from ckanext.spatial.harvesters.base import SpatialHarvester

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Singleton pattern
# ---------------------------------------------------------------------------


def singleton_new(cls, *args, **kwargs):
    """Reusable __new__ for singleton harvester classes.

    Usage: set ``__new__ = singleton_new`` in the class body.
    """
    if "_instance" not in cls.__dict__:
        cls._instance = object.__new__(cls)
    return cls._instance


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------


def load_json(val):
    """Parse *val* as JSON, returning the original value on failure.

    This eagerly attempts to decode **any** string.  Use ``from_json``
    instead when bare scalars (e.g. ``'49.4'``) must pass through
    unchanged.
    """
    try:
        return json.loads(val)
    except Exception:
        return val


def from_json(val):
    """Parse *val* as JSON only if it looks like a JSON object or array.

    Strings that do not start with ``{`` or ``[`` are returned unchanged.
    This prevents bare scalars (bbox floats, booleans) from being
    accidentally decoded.
    """
    if isinstance(val, str):
        stripped = val.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(val)
            except Exception:
                pass
    return val


# ---------------------------------------------------------------------------
# Harvest object / extras helpers
# ---------------------------------------------------------------------------


def get_object_extra(harvest_object, key):
    """Retrieve a single extra value from a HarvestObject by key."""
    for extra in harvest_object.extras:
        if extra.key == key:
            return extra.value
    return None


def get_extra(key, package_dict):
    """Find an extra dict ``{"key": ..., "value": ...}`` inside *package_dict*."""
    for extra in package_dict.get("extras", []):
        if extra["key"] == key:
            return extra
    return None


# ---------------------------------------------------------------------------
# Composite key flattening
# ---------------------------------------------------------------------------


def flatten_composite_keys(obj, _new_obj=None, _keys=None):
    """Recursively flatten nested dicts by joining keys with ``_``.

    Example::

        >>> flatten_composite_keys({"a": {"b": 1, "c": 2}})
        {"a_b": 1, "a_c": 2}
    """
    if _new_obj is None:
        _new_obj = {}
    if _keys is None:
        _keys = []
    if not isinstance(obj, dict):
        return _new_obj
    for key, value in obj.items():
        if isinstance(value, dict):
            flatten_composite_keys(value, _new_obj, _keys + [key])
        else:
            _new_obj["_".join(_keys + [key])] = value
    return _new_obj


# ---------------------------------------------------------------------------
# Tag sanitization
# ---------------------------------------------------------------------------

# CKAN tag validator allows: alphanumeric, space, and -_.,;'()
# Characters outside that set are replaced with safe equivalents first,
# then any remaining disallowed characters are stripped.
_TAG_REPLACEMENTS = [
    ("\u2013", "-"),  # en dash  →  hyphen
    ("\u2014", "-"),  # em dash  →  hyphen
    ("&", "and"),
    (":", " -"),
]
_TAG_INVALID_RE = re.compile(r"[^\w \-_.,;'()]", re.UNICODE)


def sanitize_tag(text):
    """Replace/strip characters that CKAN's tag validator rejects."""
    for char, replacement in _TAG_REPLACEMENTS:
        text = text.replace(char, replacement)
    text = _TAG_INVALID_RE.sub("", text)
    return " ".join(text.split())  # collapse any double-spaces left behind


# ---------------------------------------------------------------------------
# Paginated package search
# ---------------------------------------------------------------------------


def all_packages_for_source(source_id):
    """Fetch all datasets belonging to a harvest source (handles pagination)."""
    limit = 1000
    fq = f'+harvest_source_id:"{source_id}"'
    search_dict = {
        "fq": fq,
        "rows": limit,
        "sort": "metadata_modified desc",
        "start": 0,
    }

    context = {"model": model, "session": model.Session}
    out = []
    query = logic.get_action("package_search")(context, search_dict)

    while query["results"]:
        out = out + query["results"]
        search_dict["start"] = search_dict["start"] + limit
        query = logic.get_action("package_search")(context, search_dict)

    return out


# ---------------------------------------------------------------------------
# XML content extraction
# ---------------------------------------------------------------------------


def _get_xml_url_content(xml_url, urlopen_timeout, harvest_object):
    """Fetch XML from *xml_url* and return the response text, or ``""`` on error."""
    try:
        r = requests.get(xml_url, timeout=urlopen_timeout)
        ET.XML(r.content)  # test for valid xml
        return r

    except ET.ParseError as e:
        msg = "%s: %s. From external XML content at %s" % (
            type(e).__name__,
            str(e),
            xml_url,
        )
        log.warning(msg)
        err = HarvestObjectError(message=msg, object=harvest_object, stage="Import")
        err.save()
    except requests.exceptions.Timeout as e:
        msg = "%s: %s. From external XML content at %s" % (
            type(e).__name__,
            str(e),
            xml_url,
        )
        log.warning(msg)
        err = HarvestObjectError(message=msg, object=harvest_object, stage="Import")
        err.save()
    except requests.exceptions.TooManyRedirects as e:
        msg = "HTTP too many redirects: %s" % e.code
        log.warning(msg)
        err = HarvestObjectError(message=msg, object=harvest_object, stage="Import")
        err.save()
    except requests.exceptions.RequestException as e:
        msg = "HTTP request exception: %s" % e.code
        log.warning(msg)
        err = HarvestObjectError(message=msg, object=harvest_object, stage="Import")
        err.save()
    except Exception as e:
        msg = "%s: %s. From external XML content at %s" % (
            type(e).__name__,
            str(e),
            xml_url,
        )
        log.warning(msg)
        err = HarvestObjectError(message=msg, object=harvest_object, stage="Import")
        err.save()

    except StaleDataError as e:
        log.warning(
            "Harvest object %s is stail. Error object not created. %s"
            % (harvest_object.id, str(e))
        )

    return ""


def extract_xml_from_harvest_object(package_dict, harvest_object):
    """Extract XML content from harvest object and store in package_dict.

    Tries (in order):
    1. ``package_dict['harvest_document_content']`` if it starts with ``<``
    2. ``harvest_object.content`` if it starts with ``<``
    3. Fetching from ``xml_location_url``
    """
    content = harvest_object.content
    source_config = json.loads(harvest_object.source.config or "{}")
    key = "harvest_document_content"
    value = ""
    package_content = package_dict.get(key, "")

    if package_content.startswith("<"):
        value = package_content
    elif content.startswith("<"):
        value = harvest_object.content
    else:
        log.warning(
            'Unable to find harvest object "%s" '
            'referenced by dataset "%s". Trying xml url',
            harvest_object.id,
            package_dict["id"],
        )

        # try reading from xml url
        xml_url = load_json(package_dict.get("xml_location_url"))
        if not xml_url:
            log.warning(
                "Empty or Missing URL in xml_location_url field. "
                "External xml metadata will not be retreaved."
            )
        else:
            urlopen_timeout = (
                float(
                    source_config.get("url_read_timeout")
                    or toolkit.config.get("ckan.index_xml_url_read_timeout")
                    or "500"
                )
                / 1000.0
            )  # get value in milliseconds but urllib assumes it is in seconds

            # single file
            if xml_url and isinstance(xml_url, str):
                value = _get_xml_url_content(xml_url, urlopen_timeout, harvest_object)

            # list of files
            if xml_url and isinstance(xml_url, list):
                for xml_file in xml_url:
                    value = (
                        value
                        + "<doc>"
                        + _get_xml_url_content(xml_url, urlopen_timeout, harvest_object)
                        + "</doc>"
                    )

                if value:
                    value = (
                        '<?xml version="1.0" encoding="utf-8"?><docs>'
                        + value
                        + "</docs>"
                    )

            value = re.sub(r"\s+", " ", value)  # remove extra white space
            value = re.sub("> <", "><", value)
            value = re.sub("> ", ">", value)
            value = re.sub(" <", "<", value)
    if value:
        log.info("Success. External xml retrieved.")
        package_dict[key] = value
    return package_dict


# ---------------------------------------------------------------------------
# Responsible-organization group handling
# ---------------------------------------------------------------------------


def handle_groups(
    context,
    harvest_object,
    group_mapping,
    group_type,
    cats=None,
    additional_contacts=None,
):
    """Create or find CKAN groups for responsible organisations.

    Examines cited-responsible-party contacts (*cats*) and optional
    *additional_contacts* (e.g. metadata-point-of-contact), filters by
    configured roles, then creates/finds matching CKAN groups.

    Returns a list of ``{"id": ..., "name": ...}`` dicts.
    """
    if cats is None:
        cats = []
    if additional_contacts is None:
        additional_contacts = []

    source_config = json.loads(harvest_object.source.config or "{}")
    validated_groups = []

    harvest_responsible_organizations = (
        source_config.get("harvest_responsible_organizations")
        or toolkit.config.get("ckan.harvest_responsible_organizations")
        or "true"
    ).lower()
    if harvest_responsible_organizations != "true":
        log.debug("Skipping Handle Groups: %r ", cats)
        return validated_groups

    resp_org_roles = load_json(
        source_config.get("responsible_organization_roles")
        or toolkit.config.get("ckan.responsible_organization_roles")
        or '["owner", "originator", "custodian", "author", "principalInvestigator"]'
    )

    # Additional roles that can be associated with responsible_organization
    # even if not in citation
    additional_resp_org_roles = load_json(
        source_config.get("additional_responsible_organization_roles")
        or toolkit.config.get("ckan.additional_responsible_organization_roles")
        or "[]"
    )

    # Process both citation contacts and additional contacts
    all_contacts = []

    # Add citation contacts with their roles
    for cat in cats:
        role = load_json(cat.get("role"))
        if not isinstance(role, list):
            role = [role]
        if not set(role).isdisjoint(set(resp_org_roles)):
            all_contacts.append(cat)

    # Add additional contacts (from metadata-point-of-contact, etc.)
    for contact in additional_contacts:
        role = load_json(contact.get("role"))
        if not isinstance(role, list):
            role = [role]
        if not set(role).isdisjoint(set(additional_resp_org_roles)):
            all_contacts.append(contact)
            log.debug(
                "Adding additional contact with role %s: %s"
                % (role, contact.get("organisation-name"))
            )

    # Process all collected contacts
    for cat in all_contacts:
        if not cat.get("organisation-name"):
            continue

        organisation_name = cat["organisation-name"].strip()
        orgname = group_mapping.get(
            organisation_name, munge.munge_name(organisation_name).lower()
        )
        groupname = "_".join([group_type, orgname])

        printname = orgname if not None else "NONE"
        log.debug("Group %s mapped into %s" % (organisation_name, printname))

        if groupname:
            try:
                data_dict = {"id": groupname}
                group = toolkit.get_action("group_show")(
                    context.copy(), data_dict=data_dict
                )
                log.info("Found Existing Group %s" % (groupname))
                validated_groups.append({"id": group["id"], "name": group["name"]})
            except toolkit.ObjectNotFound:
                log.debug("Group %s is not available" % (groupname))
                # check if group exists as an organization
                try:
                    org = toolkit.get_action("organization_show")(
                        context.copy(),
                        data_dict={
                            "id": orgname,
                            "include_datasets": False,
                            "include_dataset_count": False,
                            "include_extras": True,
                            "include_users": False,
                            "include_groups": False,
                            "include_tags": False,
                            "include_followers": False,
                        },
                    )
                    org["name"] = groupname
                    for key in [
                        "id",
                        "packages",
                        "created",
                        "users",
                        "groups",
                        "tags",
                        "is_organization",
                        "num_followers",
                        "package_count",
                        "approval_status",
                    ]:
                        org.pop(key, None)
                    org["type"] = group_type or "group"
                    if org.get("organization-uri"):
                        org["group-uri"] = org["organization-uri"].copy()
                    created_group = toolkit.get_action("group_create")(
                        context.copy(), data_dict=org
                    )
                    log.info("Group %s created from org %s", groupname, orgname)
                    validated_groups.append(
                        {
                            "id": created_group["id"],
                            "name": created_group["name"],
                        }
                    )
                except toolkit.ValidationError as e:
                    SpatialHarvester._save_object_error(
                        "Validation Error while creating group %s: %s"
                        % (org["name"], e.error_dict),
                        harvest_object,
                        "Import",
                    )
                    continue
                except toolkit.ObjectNotFound:
                    # no organization match so generate new group
                    log.debug(
                        "Organization %s not found, can not generate "
                        "group %s from organization" % (orgname, groupname)
                    )
                    group = {
                        "name": groupname,
                        "display_name": organisation_name,
                        "title": organisation_name,
                        "type": group_type or "group",
                        "title_translated": {
                            "en": organisation_name,
                            "fr": organisation_name,
                        },
                        "organisation-uri": {
                            "authority": cat.get("organisation-uri_authority", ""),
                            "code": cat.get("organisation-uri_code", ""),
                            "code-space": cat.get("organisation-uri_code-space", ""),
                            "version": cat.get("organisation-uri_version", ""),
                        },
                    }
                    try:
                        created_group = toolkit.get_action("group_create")(
                            context.copy(), data_dict=group
                        )
                    except toolkit.ValidationError as e:
                        SpatialHarvester._save_object_error(
                            "Validation Error while creating group %s: %s"
                            % (group["name"], e.error_dict),
                            harvest_object,
                            "Import",
                        )
                        continue

                    log.info("Group %s created", groupname)
                    validated_groups.append(
                        {
                            "id": created_group["id"],
                            "name": created_group["name"],
                        }
                    )
    return validated_groups


# ---------------------------------------------------------------------------
# Data catalogue helpers
# ---------------------------------------------------------------------------


def make_site_catalogue_entry():
    """Build a data catalogue dict from the current CKAN site config."""
    return {
        "name": load_json(toolkit.config.get("ckan.site_title")),
        "description": load_json(toolkit.config.get("ckan.site_description")),
        "url": toolkit.config.get("ckan.site_url"),
    }


def deduplicate_catalogue_entries(entries):
    """Deduplicate a list of catalogue dicts by URL, preserving order."""
    return list({item.get("url", ""): item for item in entries[::-1]}.values())


# ---------------------------------------------------------------------------
# Resource field translation
# ---------------------------------------------------------------------------


def translate_resource_fields(resources, primary_lang, json_decoder=None):
    """Decode multilingual name/description on resources into ``_translated`` dicts.

    For each resource, if ``name`` or ``description`` is a JSON-encoded
    lang-dict, decode it and set:
    - ``resource[field + "_translated"]`` = the lang dict
    - ``resource[field]`` = the plain ``primary_lang`` string

    If the value is a plain string, wrap it in ``{primary_lang: value}``.

    Parameters
    ----------
    resources : list[dict]
        The resources list from ``package_dict["resources"]``.
        Modified in place.
    primary_lang : str
        Two-letter language code for plain-string extraction.
    json_decoder : callable or None
        Function to decode JSON strings.  Defaults to ``from_json``.
    """
    if json_decoder is None:
        json_decoder = from_json

    for resource in resources:
        for field in ("name", "description"):
            raw = resource.get(field, "")
            if not raw:
                continue
            # Skip if already translated
            if resource.get(field + "_translated"):
                continue
            val = json_decoder(raw)
            if isinstance(val, dict):
                resource[field + "_translated"] = val
                resource[field] = val.get(primary_lang) or next(iter(val.values()), "")
            else:
                resource[field + "_translated"] = {primary_lang: val}

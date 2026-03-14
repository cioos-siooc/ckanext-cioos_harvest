"""
Unified field handlers for CIOOS harvesters.

These functions handle the mapping of ISO/harvest values to CKAN package
fields for scheming, fluent, and composite extensions.  They are used by
both the ISpatialHarvester plugin callback (plugin.py) and the WAF
harvester (waf.py), parameterised to cover the differences between the
two code paths.
"""

import json
import logging
from numbers import Number

import ckan.plugins.toolkit as toolkit

from ckanext.cioos_harvest.harvesters.base import (
    flatten_composite_keys,
    from_json,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fluent field handler
# ---------------------------------------------------------------------------


def handle_fluent_field(
    field,
    iso_values,
    package_dict,
    schema,
    default_language,
    handled_fields,
    harvest_config=None,
    tag_source=None,
    tag_accessor=None,
    tag_sanitizer=None,
    merge_existing_tags=False,
    json_decoder=None,
):
    """Populate a fluent (multilingual) field on *package_dict*.

    Handles both ``fluent_text`` and ``fluent_tags`` presets.

    Parameters
    ----------
    field : dict
        Schema field definition (must contain ``field_name``, ``preset``).
    iso_values : dict
        Parsed ISO values dictionary.
    package_dict : dict
        The CKAN package dict being constructed.
    schema : dict
        The full scheming dataset schema.
    default_language : str
        Two-letter language code for the record's primary language.
    handled_fields : list
        Accumulator of already-handled field names (mutated in place).
    harvest_config : dict or None
        Harvest source configuration (used for ``clean_tags`` option).
    tag_source : str or None
        Key in *iso_values* to read tags from.  Defaults to ``"tags"``
        for the WAF path and ``field["field_name"]`` for the plugin path.
    tag_accessor : callable or None
        ``fn(tag_item) -> value`` to extract the keyword from each tag
        entry.  Defaults to ``json_decoder(item)`` (WAF path).
        For the plugin path, pass ``lambda t: json_decoder(t.get("keyword", t))``.
    tag_sanitizer : callable or None
        ``fn(text) -> text`` applied to each tag value.  Defaults to
        ``sanitize_tag``.  Pass ``munge.munge_tag`` for the plugin path
        (only applied when ``harvest_config["clean_tags"]`` is truthy).
    merge_existing_tags : bool
        If True, also add ``package_dict["tags"]`` entries to the fluent
        field (plugin path behaviour).
    json_decoder : callable or None
        Function to decode JSON strings.  Defaults to ``from_json``.
    """
    if json_decoder is None:
        json_decoder = from_json

    field_name = field["field_name"]
    if field_name in handled_fields:
        return

    if not field.get("preset", "").startswith("fluent"):
        return

    schema_languages = toolkit.h.fluent_form_languages(schema=schema)

    # ---- fluent_tags ----
    if field.get("preset", "") == "fluent_tags":
        source_key = tag_source if tag_source is not None else "tags"
        raw_tags = iso_values.get(source_key, [])

        if harvest_config is None:
            harvest_config = {}
        do_clean = toolkit.asbool(harvest_config.get("clean_tags", False))

        field_value = {lang: [] for lang in schema_languages}

        for t in raw_tags:
            # Extract the keyword value from the tag entry
            if tag_accessor is not None:
                tobj = tag_accessor(t)
            else:
                tobj = json_decoder(t)

            if isinstance(tobj, Number):
                tobj = str(tobj)

            if isinstance(tobj, dict):
                for key, value in tobj.items():
                    if key in schema_languages:
                        if do_clean and tag_sanitizer:
                            if isinstance(value, list):
                                value = [tag_sanitizer(kw) for kw in value]
                            else:
                                value = tag_sanitizer(value)
                        elif tag_sanitizer and not do_clean:
                            # WAF path: always sanitize
                            value = tag_sanitizer(value)
                        if isinstance(value, list):
                            field_value[key].extend(value)
                        else:
                            field_value[key].append(value)
            elif isinstance(tobj, list):
                # ISO 19139 groups multiple keywords per MD_Keywords element
                # as a list — flatten them into individual tag entries.
                for item in tobj:
                    item = str(item) if not isinstance(item, str) else item
                    if tag_sanitizer:
                        item = tag_sanitizer(item)
                    field_value[default_language].append(item)
            else:
                if do_clean and tag_sanitizer:
                    tobj = tag_sanitizer(str(tobj))
                elif tag_sanitizer and not do_clean:
                    tobj = tag_sanitizer(str(tobj))
                field_value[default_language].append(tobj)

        # Optionally merge plain tags from package_dict (plugin path)
        if merge_existing_tags:
            for item in package_dict.get("tags", []):
                tag_name = item.get("name") if isinstance(item, dict) else item
                if tag_name and tag_name not in field_value.get(default_language, []):
                    field_value.setdefault(default_language, []).append(tag_name)

        package_dict[field_name] = field_value
        package_dict["tags"] = []

    # ---- fluent_text ----
    else:
        # Strip trailing _translated part of field name
        if field_name.endswith("_translated"):
            package_fn = field_name[:-11]
        else:
            package_fn = field_name

        # If a previous handler already set this field as a multilingual dict,
        # reuse it rather than re-reading the plain package_fn value.
        existing = package_dict.get(field_name)
        if isinstance(existing, dict) and existing:
            field_value = existing
        else:
            field_value = json_decoder(package_dict.get(package_fn, ""))

        if isinstance(field_value, dict):
            result = dict(field_value)
        else:
            result = {default_language: field_value}

        # Fall back to any available non-empty value for missing languages
        fallback = next((v for v in result.values() if v), "")
        for lang in schema_languages:
            if not result.get(lang):
                result[lang] = fallback
        package_dict[field_name] = result

    handled_fields.append(field_name)


# ---------------------------------------------------------------------------
# Composite field handler
# ---------------------------------------------------------------------------


def handle_composite_field(
    field,
    iso_values,
    package_dict,
    handled_fields,
    separator="|",
    extras=None,
):
    """Flatten composite/repeating_subfields into package_dict.

    Parameters
    ----------
    field : dict
        Schema field definition.
    iso_values : dict
        Parsed ISO values dictionary.
    package_dict : dict
        The CKAN package dict being constructed.
    handled_fields : list
        Accumulator of already-handled field names.
    separator : str
        Separator between field name, index, and subfield key.
        WAF path uses ``"|"``, plugin path uses
        ``toolkit.h.scheming_composite_separator()``.
    extras : dict or None
        If provided, remove the field from this extras dict after handling
        (plugin path).  If None, write to ``package_dict["__extras"]``
        instead of ``package_dict`` directly (WAF path).
    """
    field_name = field["field_name"]
    if field_name in handled_fields:
        return

    field_value = iso_values.get(field_name, {})
    if not field_value:
        return

    # Determine if this is a composite field
    is_composite = field.get("preset", "") == "composite"
    is_repeating = field.get("preset", "") == "composite_repeating" or field.get(
        "repeating_subfields"
    )

    if not (is_composite or is_repeating):
        return

    if is_composite:
        if isinstance(field_value, list):
            field_value = field_value[0]
        field_value = flatten_composite_keys(field_value)

        if extras is None:
            # WAF path: write to __extras
            if "__extras" not in package_dict:
                package_dict["__extras"] = {}
            for key, value in field_value.items():
                package_dict["__extras"][field_name + separator + key] = value
        else:
            # Plugin path: write to package_dict directly
            for key, value in field_value.items():
                package_dict[field_name + separator + key] = value
            if extras.get(field_name):
                del extras[field_name]

        handled_fields.append(field_name)

    elif is_repeating:
        if isinstance(field_value, dict):
            field_value = [field_value]

        # Determine index offset: WAF path uses 1-indexed,
        # plugin path uses 0-indexed
        index_offset = 0 if extras is not None else 1

        for idx, subitem in enumerate(field_value):
            # Try to decode bytes/strings before flattening
            if isinstance(subitem, (bytes, str)) and not isinstance(subitem, bool):
                try:
                    subitem_str = (
                        subitem.decode("utf-8")
                        if isinstance(subitem, bytes)
                        else subitem
                    )
                    subitem = json.loads(subitem_str)
                except (ValueError, TypeError):
                    pass
            if not isinstance(subitem, dict):
                log.warning(
                    "Skipping non-dict subitem for composite field %s (got %s)",
                    field_name,
                    type(subitem).__name__,
                )
                continue

            subitem = flatten_composite_keys(subitem)

            if extras is None:
                # WAF path: write to __extras, 1-indexed
                if "__extras" not in package_dict:
                    package_dict["__extras"] = {}
                for key, value in subitem.items():
                    package_dict["__extras"][
                        field_name
                        + separator
                        + str(idx + index_offset)
                        + separator
                        + key
                    ] = value
            else:
                # Plugin path: write to package_dict directly, 0-indexed
                for key, value in subitem.items():
                    package_dict[
                        field_name
                        + separator
                        + str(idx + index_offset)
                        + separator
                        + key
                    ] = value

        if extras is not None and extras.get(field_name):
            del extras[field_name]

        handled_fields.append(field_name)


# ---------------------------------------------------------------------------
# Scheming field handler
# ---------------------------------------------------------------------------


def handle_scheming_field(
    field,
    iso_values,
    extras,
    package_dict,
    handled_fields,
    json_decoder=None,
):
    """Move schema fields from extras or iso_values into package_dict.

    Parameters
    ----------
    field : dict
        Schema field definition.
    iso_values : dict
        Parsed ISO values dictionary.
    extras : dict
        Extras dictionary (key→value, not the list-of-dicts format).
    package_dict : dict
        The CKAN package dict being constructed.
    handled_fields : list
        Accumulator of already-handled field names.
    json_decoder : callable or None
        Function to decode JSON strings from extras.  Defaults to
        ``from_json`` (conservative decoder).
    """
    if json_decoder is None:
        json_decoder = from_json

    field_name = field["field_name"]
    if field_name in handled_fields:
        return

    iso_field_value = iso_values.get(field_name, {})
    extra_field_value = extras.get(field_name, "")

    # Move schema fields from extras to package dictionary
    if field_name in extras and not package_dict.get(field_name, ""):
        package_dict[field_name] = json_decoder(extra_field_value)
        del extras[field_name]
        handled_fields.append(field_name)
    # Move schema fields from iso_values to package dictionary
    elif iso_field_value and not package_dict.get(field_name, ""):
        # Convert list to single value for select fields (not multi-select)
        if field.get("preset", "") == "select" and isinstance(iso_field_value, list):
            iso_field_value = iso_field_value[0]
        package_dict[field_name] = iso_field_value
        if field_name in extras:
            del extras[field_name]
        handled_fields.append(field_name)

"""Consolidated ISO 19115-3 WAF harvester.

Merges WAFHarvesterCIOOS (field handlers) and WAFHarvesterISO19115_3
(monkey-patch + import_stage) into a single class that inherits directly
from WAFHarvester.
"""

import logging

from ckan.lib.helpers import json
from ckan.plugins.core import SingletonPlugin, implements

import ckanext.spatial.harvesters.base as spatial_base
from ckan import model as ckan_model
from ckan import plugins as p
from ckanext.cioos_harvest.harvesters.base import (
    from_json,
    sanitize_tag,
    singleton_new,
    translate_resource_fields,
)
from ckanext.cioos_harvest.harvesters.field_handlers import (
    handle_composite_field,
    handle_fluent_field,
    handle_scheming_field,
)
from ckanext.cioos_harvest.model.iso19115_3 import ISODocument
from ckanext.harvest.interfaces import IHarvester
from ckanext.spatial.harvesters.waf import WAFHarvester

log = logging.getLogger(__name__)


class WAFHarvesterISO19115_3(WAFHarvester, SingletonPlugin):
    """WAF harvester for ISO 19115-3 XML metadata (mdb:MD_Metadata root element).

    Always routes documents to the functional ISO 19115-3 parser (ISODocument)
    and skips ISO 19139 XSD validation.
    """

    implements(IHarvester)

    __new__ = singleton_new

    def info(self):
        return {
            "name": "waf_iso19115_3_harvester",
            "title": "CIOOS Web Accessible Folder - ISO 19115-3",
            "description": (
                "A Web Accessible Folder (WAF) harvester for ISO 19115-3 XML metadata "
                "(mdb:MD_Metadata root element). Uses a dedicated ISO 19115-3 XML parser "
                "with no backward-compatibility paths for ISO 19139."
            ),
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_config(self, source_config):
        """Strip validator_profiles before base validation — this harvester skips XSD validation."""
        if source_config:
            try:
                config_obj = json.loads(source_config)
                if "validator_profiles" in config_obj:
                    log.info(
                        "%s: ignoring validator_profiles %s "
                        "(XSD validation is skipped for this harvester)",
                        self.__class__.__name__,
                        config_obj["validator_profiles"],
                    )
                    config_obj.pop("validator_profiles")
                    source_config = json.dumps(config_obj)
            except ValueError:
                pass
        return super().validate_config(source_config)

    def _validate_document(self, document_string, harvest_object, validator=None):
        """Skip ISO 19139 XSD validation — this harvester is ISO 19115-3 only."""
        log.debug(
            "%s: skipping XSD validation (GUID: %s)",
            self.__class__.__name__,
            harvest_object.guid,
        )
        return True, None, []

    # ------------------------------------------------------------------
    # Import stage
    # ------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """Monkey-patch spatial_base.ISODocument, run the base import, then restore."""
        original_document = self._get_object_extra(harvest_object, "original_document")
        content = original_document or harvest_object.content or ""

        # Move content to harvest_object.content and clear the extras that
        # would otherwise trigger the XSLT transform path in the base class.
        harvest_object.content = content
        harvest_object.save()
        for extra in harvest_object.extras:
            if extra.key in ("original_document", "original_format"):
                extra.value = ""
        ckan_model.Session.flush()

        old_cls = spatial_base.ISODocument
        spatial_base.ISODocument = ISODocument
        try:
            return super().import_stage(harvest_object)
        finally:
            spatial_base.ISODocument = old_cls

    # Delegate to shared base functions (kept as methods for backward compat)
    from_json = staticmethod(from_json)

    # ------------------------------------------------------------------
    # Package dict construction
    # ------------------------------------------------------------------

    def _expand_point_bboxes(self, iso_values, guid=""):
        """Expand degenerate point bounding boxes to tiny polygons in-place.

        ISO 19115-3 allows a bounding box where west==east and south==north,
        which legitimately describes a single-coordinate location (e.g. a
        mooring, a station, or a single water-column sample).

        ckanext-spatial's SpatialHarvester.get_package_dict() treats this as
        an error: when xmin==xmax or ymin==ymax it calls _save_object_error()
        with "Point extent defined instead of polygon", which marks the harvest
        object as ERRORED in the harvester UI — even though the dataset is
        actually imported correctly.

        This method pre-processes iso_values['bbox'] before the parent
        get_package_dict() is called, expanding each degenerate bbox outward
        by _EPSILON degrees (~1 m at the equator, ~0.7 m at 74°N).  The
        resulting tiny polygon is geographically indistinguishable from a point
        at any practical resolution and passes cleanly through ckanext-spatial's
        polygon check.

        Args:
            iso_values: The iso_values dict produced by the ISO 19115-3 parser.
                        Modified in-place; safe because each harvest object gets
                        a fresh dict that is never shared between records.
            guid:       The harvest object GUID, used only for the debug log.
        """
        _EPSILON = 0.00001
        for bbox in iso_values.get("bbox", []):
            try:
                west, east = float(bbox["west"]), float(bbox["east"])
                south, north = float(bbox["south"]), float(bbox["north"])
            except (TypeError, ValueError):
                continue
            if west == east or south == north:
                log.debug(
                    "Point bbox detected for %s — expanding by epsilon to avoid "
                    "harvest error",
                    guid,
                )
                bbox["west"] = str(west - _EPSILON)
                bbox["east"] = str(east + _EPSILON)
                bbox["south"] = str(south - _EPSILON)
                bbox["north"] = str(north + _EPSILON)

    def get_package_dict(self, iso_values, harvest_object):
        self._expand_point_bboxes(iso_values, guid=harvest_object.guid)

        package_dict = super().get_package_dict(
            iso_values, harvest_object
        )

        package = harvest_object.package

        # Use the record's primary language (mdb:defaultLocale) for plain-text
        # extraction — not the CKAN site default locale.
        primary_lang = (iso_values.get("metadata-language") or "en")[:2]

        # Resolve the plain primary-language title from the JSON lang-dict
        # (used for package_dict['title'] below after the scheming-fields loop).
        iso_title = self.from_json(iso_values["title"])
        if isinstance(iso_title, dict):
            iso_title = iso_title.get(primary_lang) or next(
                iter(iso_title.values()), ""
            )

        # Use the metadata GUID (authority_code with dots replaced by dashes) as
        # both the CKAN package id and a stable, deterministic URL slug.
        # Idempotent: the same XML record always maps to the same id/name.
        guid = iso_values.get("guid", "")
        if guid:
            package_dict["id"] = guid.split("_")[
                -1
            ]  # Use the code portion of the GUID as the CKAN package id
            package_dict["name"] = guid.replace(".", "-")
        elif package is not None:
            package_dict["name"] = package.name
        else:
            raise Exception(
                "Could not generate a package name: metadata GUID is missing."
            )

        # Handle Scheming, Composite, and Fluent extensions
        loaded_plugins = p.toolkit.config.get("ckan.plugins")
        if "scheming_datasets" in loaded_plugins:
            composite = "composite" in loaded_plugins
            fluent = "fluent" in loaded_plugins

            log.debug("Scheming/Composite/Fluent found — processing dictionary")
            schema = p.toolkit.h.scheming_get_dataset_schema("dataset")

            # Convert extras key:value list to dict
            extras = {x["key"]: x["value"] for x in package_dict.get("extras", [])}

            for field in schema["dataset_fields"]:
                fn = field["field_name"]
                iso = iso_values.get(fn, {})
                if isinstance(iso, list):
                    iso = list(filter(len, iso))

                handled_fields = []
                if composite:
                    self.handle_composite_harvest_dictionary(
                        field, iso_values, package_dict, handled_fields
                    )
                if fluent:
                    self.handle_fluent_harvest_dictionary(
                        field,
                        iso_values,
                        package_dict,
                        schema,
                        handled_fields,
                        self.source_config,
                    )
                self.handle_scheming_harvest_dictionary(
                    field, iso_values, extras, package_dict, handled_fields
                )

            extras_as_dict = []
            for key, value in extras.items():
                if package_dict.get(key, ""):
                    log.error(
                        "extras %s found in package dict: key:%s value:%s",
                        key,
                        key,
                        value,
                    )
                if isinstance(value, (list, dict)):
                    extras_as_dict.append({"key": key, "value": json.dumps(value)})
                else:
                    extras_as_dict.append({"key": key, "value": value})
            package_dict["extras"] = extras_as_dict

            # Filter eov values against the schema's allowed choices so that
            # unknown codes (e.g. 'airWaterExchange') produce a warning instead
            # of a hard validation error that would block the whole record.
            eov_field = next(
                (f for f in schema["dataset_fields"] if f["field_name"] == "eov"), None
            )
            if eov_field:
                valid_eov = {c["value"] for c in eov_field.get("choices", [])}
                raw_eov = package_dict.get("eov") or []
                if isinstance(raw_eov, str):
                    raw_eov = [raw_eov]
                filtered, skipped = [], []
                for v in raw_eov:
                    if v in valid_eov:
                        filtered.append(v)
                    else:
                        skipped.append(v)
                if skipped:
                    log.warning(
                        "Record %s: unknown EOV value(s) %s — skipping (not in schema choices)",
                        harvest_object.guid,
                        skipped,
                    )
                package_dict["eov"] = filtered or ["other"]

        # Ensure ecv is always present (schema field, defaults to empty list).
        if "ecv" not in package_dict:
            package_dict["ecv"] = []

        # Overwrite title/notes with plain locale strings (not JSON blobs)
        package_dict["title"] = iso_title

        iso_abstract = self.from_json(iso_values.get("abstract", ""))
        if isinstance(iso_abstract, dict):
            package_dict["notes"] = iso_abstract.get(
                primary_lang, next(iter(iso_abstract.values()), "")
            )

        # translation_method fields: title and notes always carry both portal
        # languages; keywords uses only the record's primary language (the
        # original) so that translated-language entries are not falsely marked
        # as having an empty translation method.
        _default_tm = {"en": "", "fr": ""}
        package_dict["title_translation_method"] = dict(
            _default_tm, **(iso_values.get("title_translation_method") or {})
        )
        package_dict["notes_translation_method"] = dict(
            _default_tm, **(iso_values.get("abstract_translation_method") or {})
        )
        package_dict["keywords_translation_method"] = iso_values.get(
            "keywords_translation_method"
        ) or {primary_lang: ""}

        # Build a URL → format map from the annotated resource-locator entries.
        # _infer_resource_types() in iso19115_3.py sets locator['format'] for
        # every resource-locator it recognises; we apply those labels here.
        locator_formats = {
            loc["url"]: loc["format"]
            for loc in iso_values.get("resource-locator", [])
            if loc.get("url") and loc.get("format")
        }

        # Decode multilingual name/description on resources into _translated dicts.
        translate_resource_fields(
            package_dict.get("resources", []), primary_lang, json_decoder=from_json
        )

        # Apply CIOOS-specific format labels derived from URL patterns.
        for resource in package_dict.get("resources", []):
            url = resource.get("url", "")
            if url in locator_formats:
                resource["format"] = locator_formats[url]
            elif resource.get("format") in (
                None,
                "text/html",
                "text/html; charset=utf-8",
            ):
                resource["format"] = "HTML"

        # Set license_id from CIOOS-specific legal constraints fields when the
        # parent SpatialHarvester left it unset (use-constraints was empty).
        if not package_dict.get("license_id"):
            package_dict["license_id"] = (
                iso_values.get("legal-constraints-reference-code")
                or iso_values.get("use-constraints")
                or ""
            )
            if not package_dict["license_id"]:
                log.warning("No license_id found.")
        # Resolve license_title and license_url from CKAN's license register.
        license_id = package_dict.get("license_id")
        if license_id:
            try:
                register = ckan_model.Package.get_license_register()
                if license_id in register:
                    lic = register[license_id]
                    package_dict["license_title"] = lic.title
                    package_dict["license_url"] = getattr(lic, "url", "")
                else:
                    log.warning(
                        "license_id %r not found in CKAN license register "
                        "(check licenses_group_url configuration)",
                        license_id,
                    )
            except Exception as exc:
                log.warning(
                    "Could not resolve license_title for %r: %s", license_id, exc
                )

        # Derive metadata_created / metadata_modified from ISO 19115-3 metadata-level
        # dates (mdb:dateInfo).  These record when the METADATA RECORD itself was
        # first published / last revised — semantically equivalent to CKAN's own
        # package-level created/modified timestamps.
        #
        # metadata-reference-date is sorted oldest-first and truncated to YYYY-MM-DD
        # by _infer_clean_metadata_reference_date; metadata-date holds the full
        # datetime string of the newest date.
        _ref_dates = iso_values.get("metadata-reference-date", [])
        _meta_date = iso_values.get("metadata-date", "")

        # metadata_created: prefer explicit 'creation' type; fall back to oldest date.
        _meta_created = next(
            (
                d["value"]
                for d in _ref_dates
                if (d.get("type") or "").lower() == "creation"
            ),
            _ref_dates[0]["value"] if _ref_dates else "",
        )
        # metadata_modified: prefer explicit 'revision' type; fall back to the full
        # metadata-date datetime (highest precision), then newest ref-date.
        _meta_modified = next(
            (
                d["value"]
                for d in _ref_dates
                if (d.get("type") or "").lower() == "revision"
            ),
            _meta_date or (_ref_dates[-1]["value"] if _ref_dates else ""),
        )

        if _meta_created:
            package_dict["metadata_created"] = _meta_created
        if _meta_modified:
            package_dict["metadata_modified"] = _meta_modified

        return package_dict

    # ------------------------------------------------------------------
    # Field handlers — delegate to shared field_handlers module
    # ------------------------------------------------------------------

    def handle_fluent_harvest_dictionary(
        self, field, iso_values, package_dict, schema, handled_fields, harvest_config
    ):
        default_language = iso_values.get("metadata-language", "en") or "en"
        handle_fluent_field(
            field,
            iso_values,
            package_dict,
            schema,
            default_language,
            handled_fields,
            tag_sanitizer=sanitize_tag,
            json_decoder=from_json,
        )

    def handle_composite_harvest_dictionary(
        self, field, iso_values, package_dict, handled_fields
    ):
        handle_composite_field(
            field,
            iso_values,
            package_dict,
            handled_fields,
            separator="|",
            extras=None,  # WAF path: writes to __extras
        )

    def handle_scheming_harvest_dictionary(
        self, field, iso_values, extras, package_dict, handled_fields
    ):
        handle_scheming_field(
            field,
            iso_values,
            extras,
            package_dict,
            handled_fields,
            json_decoder=from_json,
        )

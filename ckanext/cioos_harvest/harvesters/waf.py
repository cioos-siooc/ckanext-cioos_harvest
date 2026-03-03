"""Consolidated ISO 19115-3 WAF harvester.

Merges WAFHarvesterCIOOS (field handlers) and WAFHarvesterISO19115_3
(monkey-patch + import_stage) into a single class that inherits directly
from WAFHarvester.
"""

import logging

from ckan import model as ckan_model
from ckan import plugins as p
from ckan.lib.helpers import json
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.spatial.harvesters.waf import WAFHarvester
import ckanext.spatial.harvesters.base as spatial_base

from ckanext.cioos_harvest.model.iso19115_3 import ISODocument

log = logging.getLogger(__name__)


class WAFHarvesterISO19115_3(WAFHarvester, SingletonPlugin):
    """WAF harvester for ISO 19115-3 XML metadata (mdb:MD_Metadata root element).

    Always routes documents to the functional ISO 19115-3 parser (ISODocument)
    and skips ISO 19139 XSD validation.
    """

    implements(IHarvester)

    def __new__(cls, *args, **kwargs):
        if '_instance' not in cls.__dict__:
            cls._instance = object.__new__(cls)
        return cls._instance

    def info(self):
        return {
            'name': 'waf_iso19115_3_harvester',
            'title': 'CIOOS Web Accessible Folder - ISO 19115-3',
            'description': (
                'A Web Accessible Folder (WAF) harvester for ISO 19115-3 XML metadata '
                '(mdb:MD_Metadata root element). Uses a dedicated ISO 19115-3 XML parser '
                'with no backward-compatibility paths for ISO 19139.'
            ),
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_document(self, document_string, harvest_object, validator=None):
        """Skip ISO 19139 XSD validation — this harvester is ISO 19115-3 only."""
        log.debug('Skipping XSD validation for ISO 19115-3 harvester (GUID: %s)',
                  harvest_object.guid)
        return True, None, []

    # ------------------------------------------------------------------
    # Import stage
    # ------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """Monkey-patch spatial_base.ISODocument, run the base import, then restore."""
        original_document = self._get_object_extra(harvest_object, 'original_document')
        content = original_document or harvest_object.content or ''

        # Move content to harvest_object.content and clear the extras that
        # would otherwise trigger the XSLT transform path in the base class.
        harvest_object.content = content
        harvest_object.save()
        for extra in harvest_object.extras:
            if extra.key in ('original_document', 'original_format'):
                extra.value = ''
        ckan_model.Session.flush()

        old_cls = spatial_base.ISODocument
        spatial_base.ISODocument = ISODocument
        try:
            return super(WAFHarvesterISO19115_3, self).import_stage(harvest_object)
        finally:
            spatial_base.ISODocument = old_cls

    # ------------------------------------------------------------------
    # JSON helper
    # ------------------------------------------------------------------

    def from_json(self, val):
        if isinstance(val, str):
            stripped = val.strip()
            if stripped.startswith('{') or stripped.startswith('['):
                try:
                    return json.loads(val)
                except Exception:
                    pass
        return val

    # ------------------------------------------------------------------
    # Package dict construction
    # ------------------------------------------------------------------

    def _expand_point_bboxes(self, iso_values, guid=''):
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
        for bbox in iso_values.get('bbox', []):
            try:
                west, east = float(bbox['west']), float(bbox['east'])
                south, north = float(bbox['south']), float(bbox['north'])
            except (TypeError, ValueError):
                continue
            if west == east or south == north:
                log.debug('Point bbox detected for %s — expanding by epsilon to avoid '
                          'harvest error', guid)
                bbox['west'] = str(west - _EPSILON)
                bbox['east'] = str(east + _EPSILON)
                bbox['south'] = str(south - _EPSILON)
                bbox['north'] = str(north + _EPSILON)

    def get_package_dict(self, iso_values, harvest_object):
        self._expand_point_bboxes(iso_values, guid=harvest_object.guid)

        package_dict = super(WAFHarvesterISO19115_3, self).get_package_dict(
            iso_values, harvest_object)

        package = harvest_object.package

        # Use the record's primary language (mdb:defaultLocale) for plain-text
        # extraction — not the CKAN site default locale.
        primary_lang = (iso_values.get('metadata-language') or 'en')[:2]

        # Resolve the plain primary-language title from the JSON lang-dict
        # (used for package_dict['title'] below after the scheming-fields loop).
        iso_title = self.from_json(iso_values['title'])
        if isinstance(iso_title, dict):
            iso_title = iso_title.get(primary_lang) or next(iter(iso_title.values()), '')

        # Use the metadata GUID (authority_code with dots replaced by dashes) as
        # both the CKAN package id and a stable, deterministic URL slug.
        # Idempotent: the same XML record always maps to the same id/name.
        guid = iso_values.get('guid', '')
        if guid:
            package_dict['id'] = guid.split('_')[-1]  # Use the code portion of the GUID as the CKAN package id
            package_dict['name'] = guid.replace('.', '-')
        elif package is not None:
            package_dict['name'] = package.name
        else:
            raise Exception(
                'Could not generate a package name: metadata GUID is missing.')

        # Handle Scheming, Composite, and Fluent extensions
        loaded_plugins = p.toolkit.config.get("ckan.plugins")
        if 'scheming_datasets' in loaded_plugins:
            composite = 'composite' in loaded_plugins
            fluent = 'fluent' in loaded_plugins

            log.debug('Scheming/Composite/Fluent found — processing dictionary')
            schema = p.toolkit.h.scheming_get_dataset_schema('dataset')

            # Convert extras key:value list to dict
            extras = {x['key']: x['value'] for x in package_dict.get('extras', [])}

            for field in schema['dataset_fields']:
                fn = field['field_name']
                iso = iso_values.get(fn, {})
                if isinstance(iso, list):
                    iso = list(filter(len, iso))

                handled_fields = []
                if composite:
                    self.handle_composite_harvest_dictionary(
                        field, iso_values, package_dict, handled_fields)
                if fluent:
                    self.handle_fluent_harvest_dictionary(
                        field, iso_values, package_dict, schema, handled_fields,
                        self.source_config)
                self.handle_scheming_harvest_dictionary(
                    field, iso_values, extras, package_dict, handled_fields)

            extras_as_dict = []
            for key, value in extras.items():
                if package_dict.get(key, ''):
                    log.error('extras %s found in package dict: key:%s value:%s', key, key, value)
                if isinstance(value, (list, dict)):
                    extras_as_dict.append({'key': key, 'value': json.dumps(value)})
                else:
                    extras_as_dict.append({'key': key, 'value': value})
            package_dict['extras'] = extras_as_dict

        # Ensure ecv is always present (schema field, defaults to empty list).
        if 'ecv' not in package_dict:
            package_dict['ecv'] = []

        # Overwrite title/notes with plain locale strings (not JSON blobs)
        package_dict['title'] = iso_title

        iso_abstract = self.from_json(iso_values.get('abstract', ''))
        if isinstance(iso_abstract, dict):
            package_dict['notes'] = iso_abstract.get(
                primary_lang, next(iter(iso_abstract.values()), ''))

        # translation_method fields: title and notes always carry both portal
        # languages; keywords uses only the record's primary language (the
        # original) so that translated-language entries are not falsely marked
        # as having an empty translation method.
        _default_tm = {'en': '', 'fr': ''}
        package_dict['title_translation_method'] = dict(
            _default_tm, **(iso_values.get('title_translation_method') or {}))
        package_dict['notes_translation_method'] = dict(
            _default_tm, **(iso_values.get('abstract_translation_method') or {}))
        package_dict['keywords_translation_method'] = (
            iso_values.get('keywords_translation_method') or {primary_lang: ''}
        )

        # Build a URL → format map from the annotated resource-locator entries.
        # _infer_resource_types() in iso19115_3.py sets locator['format'] for
        # every resource-locator it recognises; we apply those labels here.
        locator_formats = {
            loc['url']: loc['format']
            for loc in iso_values.get('resource-locator', [])
            if loc.get('url') and loc.get('format')
        }

        # Post-process resources: the parser stores name/description as
        # JSON-encoded lang-dicts.  Decode them into a plain primary-language
        # string (for backward-compat) plus a _translated sibling dict.
        # Also apply CIOOS-specific format labels derived from the URL.
        for resource in package_dict.get('resources', []):
            for field in ('name', 'description'):
                val = self.from_json(resource.get(field, ''))
                if isinstance(val, dict):
                    resource[field + '_translated'] = val
                    resource[field] = (
                        val.get(primary_lang) or next(iter(val.values()), '')
                    )
            url = resource.get('url', '')
            if url in locator_formats:
                resource['format'] = locator_formats[url]
            elif resource.get('format') in (None, 'text/html', 'text/html; charset=utf-8'):
                resource['format'] = 'HTML'

        # Set license_id from CIOOS-specific legal constraints fields when the
        # parent SpatialHarvester left it unset (use-constraints was empty).
        if not package_dict.get('license_id'):
            package_dict['license_id'] = (
                iso_values.get('legal-constraints-reference-code')
                or iso_values.get('use-constraints')
                or 'CC-BY-4.0'
            )

        # Resolve license_title and license_url from CKAN's license register.
        license_id = package_dict.get('license_id')
        if license_id:
            try:
                register = ckan_model.Package.get_license_register()
                if license_id in register:
                    lic = register[license_id]
                    package_dict['license_title'] = lic.title
                    package_dict['license_url'] = getattr(lic, 'url', '')
                else:
                    log.warning('license_id %r not found in CKAN license register '
                                '(check licenses_group_url configuration)', license_id)
            except Exception as exc:
                log.warning('Could not resolve license_title for %r: %s', license_id, exc)

        # Derive metadata_created / metadata_modified from ISO 19115-3 metadata-level
        # dates (mdb:dateInfo).  These record when the METADATA RECORD itself was
        # first published / last revised — semantically equivalent to CKAN's own
        # package-level created/modified timestamps.
        #
        # metadata-reference-date is sorted oldest-first and truncated to YYYY-MM-DD
        # by _infer_clean_metadata_reference_date; metadata-date holds the full
        # datetime string of the newest date.
        _ref_dates = iso_values.get('metadata-reference-date', [])
        _meta_date = iso_values.get('metadata-date', '')

        # metadata_created: prefer explicit 'creation' type; fall back to oldest date.
        _meta_created = next(
            (d['value'] for d in _ref_dates if (d.get('type') or '').lower() == 'creation'),
            _ref_dates[0]['value'] if _ref_dates else '',
        )
        # metadata_modified: prefer explicit 'revision' type; fall back to the full
        # metadata-date datetime (highest precision), then newest ref-date.
        _meta_modified = next(
            (d['value'] for d in _ref_dates if (d.get('type') or '').lower() == 'revision'),
            _meta_date or (_ref_dates[-1]['value'] if _ref_dates else ''),
        )

        if _meta_created:
            package_dict['metadata_created'] = _meta_created
        if _meta_modified:
            package_dict['metadata_modified'] = _meta_modified

        return package_dict

    # ------------------------------------------------------------------
    # Field handlers (fluent / composite / scheming)
    # ------------------------------------------------------------------

    def handle_fluent_harvest_dictionary(self, field, iso_values, package_dict,
                                        schema, handled_fields, harvest_config):
        field_name = field['field_name']
        if field_name in handled_fields:
            return
        if not field.get('preset', '').startswith('fluent'):
            return

        default_language = iso_values.get('metadata-language', 'en') or 'en'

        if field.get('preset', '') == 'fluent_tags':
            tags = iso_values.get('tags', [])
            schema_languages = p.toolkit.h.fluent_form_languages(schema=schema)
            field_value = {lang: [] for lang in schema_languages}
            for t in tags:
                tobj = self.from_json(t)
                if isinstance(tobj, dict):
                    for key, value in tobj.items():
                        if key in schema_languages:
                            field_value[key].append(value)
                else:
                    field_value[default_language].append(tobj)
            package_dict[field_name] = field_value
            # With fluent_tags active, keywords are stored in the fluent
            # field (e.g. 'keywords') — the plain 'tags' list must be empty.
            package_dict['tags'] = []
        else:
            if field_name.endswith('_translated'):
                package_fn = field_name[:-11]
            else:
                package_fn = field_name
            package_val = package_dict.get(package_fn, '')
            field_value = self.from_json(package_val)
            if isinstance(field_value, dict):
                package_dict[field_name] = field_value
            else:
                package_dict[field_name] = {default_language: field_value}

        handled_fields.append(field_name)

    def flatten_composite_keys(self, obj, new_obj={}, keys=[]):
        for key, value in obj.items():
            if isinstance(value, dict):
                self.flatten_composite_keys(obj[key], new_obj, keys + [key])
            else:
                new_obj['_'.join(keys + [key])] = value
        return new_obj

    def handle_composite_harvest_dictionary(self, field, iso_values, package_dict,
                                           handled_fields):
        field_name = field['field_name']
        if field_name in handled_fields:
            return
        field_value = iso_values.get(field_name, {})
        if '__extras' not in package_dict:
            package_dict['__extras'] = {}

        if field_value and field.get('preset', '') == 'composite':
            if isinstance(field_value, list):
                field_value = field_value[0]
            field_value = self.flatten_composite_keys(field_value)
            for key, value in field_value.items():
                package_dict['__extras'][field_name + '|' + key] = value
            handled_fields.append(field_name)
        elif field_value and field.get('preset', '') == 'composite_repeating':
            if isinstance(field_value, dict):
                field_value[0] = field_value
            for idx, subitem in enumerate(field_value):
                subitem = self.flatten_composite_keys(subitem)
                for key, value in subitem.items():
                    package_dict['__extras'][field_name + '|' + str(idx + 1) + '|' + key] = value
            handled_fields.append(field_name)

    def handle_scheming_harvest_dictionary(self, field, iso_values, extras,
                                          package_dict, handled_fields):
        field_name = field['field_name']
        if field_name in handled_fields:
            return
        iso_field_value = iso_values.get(field_name, {})
        extra_field_value = extras.get(field_name, '')

        if field_name in extras and not package_dict.get(field_name, ''):
            package_dict[field_name] = self.from_json(extra_field_value)
            del extras[field_name]
            handled_fields.append(field_name)
        elif iso_field_value and not package_dict.get(field_name, ''):
            if field.get('preset', '') == 'select' and isinstance(iso_field_value, list):
                iso_field_value = iso_field_value[0]
            package_dict[field_name] = iso_field_value
            if field_name in extras:
                del extras[field_name]
            handled_fields.append(field_name)

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

    def get_package_dict(self, iso_values, harvest_object):
        package_dict = super(WAFHarvesterISO19115_3, self).get_package_dict(
            iso_values, harvest_object)

        package = harvest_object.package

        # Resolve the plain locale title from the JSON lang-dict
        iso_title = self.from_json(iso_values['title'])
        iso_title = iso_title.get(
            p.toolkit.config.get('ckan.locale_default', 'en'), iso_title)

        if package is None or package.title != iso_title:
            name = self._gen_new_name(iso_title)
            if not name:
                name = self._gen_new_name(str(iso_values['guid']))
            if not name:
                raise Exception(
                    'Could not generate a unique name from the title or the GUID. '
                    'Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

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
                    self.handle_composite_harvest_dictinary(
                        field, iso_values, package_dict, handled_fields)
                if fluent:
                    self.handle_fluent_harvest_dictinary(
                        field, iso_values, package_dict, schema, handled_fields,
                        self.source_config)
                self.handle_scheming_harvest_dictinary(
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

        # Overwrite title/notes with plain locale strings (not JSON blobs)
        package_dict['title'] = iso_title

        locale = p.toolkit.config.get('ckan.locale_default', 'en')
        iso_abstract = self.from_json(iso_values.get('abstract', ''))
        if isinstance(iso_abstract, dict):
            package_dict['notes'] = iso_abstract.get(
                locale, next(iter(iso_abstract.values()), ''))

        # Post-process resources: the parser stores name/description as
        # JSON-encoded lang-dicts.  Decode them into a plain default-locale
        # string (for backward-compat) plus a _translated sibling dict.
        for resource in package_dict.get('resources', []):
            for field in ('name', 'description'):
                val = self.from_json(resource.get(field, ''))
                if isinstance(val, dict):
                    resource[field + '_translated'] = val
                    resource[field] = (
                        val.get(locale) or next(iter(val.values()), '')
                    )

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

        return package_dict

    # ------------------------------------------------------------------
    # Field handlers (fluent / composite / scheming)
    # ------------------------------------------------------------------

    def handle_fluent_harvest_dictinary(self, field, iso_values, package_dict,
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

            pkg_dict_tags = package_dict.get('tags', [])
            if pkg_dict_tags and not harvest_config.get('clean_tags'):
                tag_list = []
                for x in pkg_dict_tags:
                    x['name'] = self.from_json(x['name'])
                    if isinstance(x['name'], dict):
                        for item in list(x['name'].values()):
                            if item not in tag_list:
                                tag_list.append(item)
                    else:
                        if x['name'] not in tag_list:
                            tag_list.append(x['name'])
                package_dict['tags'] = [{'name': t} for t in tag_list]
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

    def handle_composite_harvest_dictinary(self, field, iso_values, package_dict,
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

    def handle_scheming_harvest_dictinary(self, field, iso_values, extras,
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

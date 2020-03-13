import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.validation.validation import BaseValidator
import ckan.lib.munge as munge
import json
import logging
import subprocess
import os
from numbers import Number

log = logging.getLogger(__name__)


# place holder, spatial extension expects a validator to be present
class MyValidator(BaseValidator):

    name = 'my-validator'

    title = 'My very own validator'

    @classmethod
    def is_valid(cls, xml):

        return True, []


class Cioos_HarvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(ISpatialHarvester, inherit=True)
    plugins.implements(plugins.IOrganizationController, inherit=True)

    # IOrganizationController
    def read(self, entity):
        pass

    def create(self, entity):
        log.debug('create:%r', entity.__dict__)
        if entity.title_translated == '{}' or not entity.title_translated:
            toolkit.get_action('organization_patch')(
                data_dict={
                    'id': entity.id,
                    'title': entity.title,
                    'title_translated': '{"en":"%s", "fr":"%s"}' % (entity.title, entity.title)
                }
            )
        return entity

    def edit(self, entity):
        pass

    def delete(self, entity):
        pass

    def before_view(self, pkg_dict):
        return pkg_dict

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'cioos_harvest')

    # ISpatialHarvester
    def get_validators(self):
        return [MyValidator]

    def from_json(self, val):
        try:
            new_val = json.loads(val)
        except Exception:
            new_val = val
        return new_val

    # def transform_to_iso(self, original_document, original_format, harvest_object):
    #     log.debug('original_format:%r',original_format)
    #     return original_document
    #
    #     lowered = original_document.lower()
    #     if '</mdb:MD_Metadata>'.lower() in lowered:
    #         log.debug('Found ISO19115-3 format, transforming to ISO19139')
    #
    #         xsl_filename = os.path.abspath("./ckanext-spatial/ckanext/spatial/transformers/ISO19115-3/toISO19139.xsl")
    #         process = subprocess.Popen(["saxonb-xslt", "-s:-", xsl_filename], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #         process.stdin.write(original_document.encode('utf-8'))
    #         newDoc, errors = process.communicate()
    #         process.stdin.close()
    #         if errors:
    #             log.error(errors)
    #             return None
    #         return newDoc
    #
    #     return None

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key, copied from ckanext-spatial/ckanext/spatial/harvesters/base.py
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def get_package_dict(self, context, data_dict):
        package_dict = data_dict['package_dict']
        iso_values = data_dict['iso_values']
        source_config = json.loads(data_dict['harvest_object'].source.config)
        xml_location_url = self._get_object_extra(data_dict['harvest_object'], 'waf_location')
        xml_modified_date = self._get_object_extra(data_dict['harvest_object'], 'waf_modified_date')

        # Handle Scheming, Composit, and Fluent extensions
        loaded_plugins = plugins.toolkit.config.get("ckan.plugins")
        if 'scheming_datasets' in loaded_plugins:
            composite = 'composite' in loaded_plugins
            fluent = 'fluent' in loaded_plugins

            log.debug('#### Scheming, Composite, or Fluent extensions found, processing dictinary ####')
            schema = plugins.toolkit.h.scheming_get_dataset_schema('dataset')

            # convert extras key:value list to dictinary
            extras = {x['key']: x['value'] for x in package_dict.get('extras', [])}

            extras['xml_location_url'] = xml_location_url
            extras['xml_modified_date'] = xml_modified_date

            # Package name, default harvester uses title or guid in that order.
            # we want to reverse that order, so guid or title. Also use english
            # title only for name
            title_as_name = self.from_json(package_dict.get('title', '{}')).get('en', package_dict['name'])
            # log.debug('title_as_name:%r',title_as_name)
            name = munge.munge_name(extras.get('guid', title_as_name)).lower()
            # log.debug('name:%r',name)
            package_dict['name'] = name

            # populate license_id
            package_dict['license_id'] = iso_values.get('legal-constraints-reference-code') or iso_values.get('use-constraints') or 'CC-BY-4.0'

            # populate trlanslation method for bilingual field
            notes_translation_method = iso_values.get('abstract_translation_method')
            title_translation_method = iso_values.get('title_translation_method')
            if notes_translation_method:
                extras['notes_translation_method'] = notes_translation_method
            if title_translation_method:
                extras['title_translation_method'] = title_translation_method
            # interate over schema fields and update package dictinary as needed
            for field in schema['dataset_fields']:
                # fn = field['field_name']
                # iso = iso_values.get(fn, {})
                # # remove empty strings from list
                # if isinstance(iso, list):
                #     iso = list(filter(len, iso))

                handled_fields = []
                if composite:
                    self.handle_composite_harvest_dictinary(field, iso_values, extras, package_dict, handled_fields)

                if fluent:
                    self.handle_fluent_harvest_dictinary(field, iso_values, package_dict, schema, handled_fields, source_config)

                self.handle_scheming_harvest_dictinary(field, iso_values, extras, package_dict, handled_fields)

            # set default values
            package_dict['progress'] = extras.get('progress', 'onGoing')
            package_dict['frequency-of-update'] = extras.get('frequency-of-update', 'asNeeded')

            extras_as_dict = []
            for key, value in extras.iteritems():
                if package_dict.get(key, ''):
                    log.error('extras %s found in package dict: key:%s value:%s', key, key, value)
                if isinstance(value, (list, dict)):
                    extras_as_dict.append({'key': key, 'value': json.dumps(value)})
                else:
                    extras_as_dict.append({'key': key, 'value': value})

            package_dict['extras'] = extras_as_dict
            #log.debug('PACKAGE_DICT Keywords:%r', package_dict['keywords'])
        return package_dict

    def handle_fluent_harvest_dictinary(self, field, iso_values, package_dict, schema, handled_fields, harvest_config):
        field_name = field['field_name']
        if field_name in handled_fields:
            return

        field_value = {}

        if not field.get('preset', '').startswith(u'fluent'):
            return

        # set default language, default to english
        default_language = iso_values.get('metadata-language', 'en')[0:2]
        if not default_language:
            default_language = 'en'

        # handle tag fields
        if field.get('preset', '') == u'fluent_tags':
            fluent_tags = iso_values.get(field_name, [])
            schema_languages = plugins.toolkit.h.fluent_form_languages(schema=schema)

            # init language key
            field_value = {l: [] for l in schema_languages}

            # process fluent_tags by convert list of language dictinarys into
            # a dictinary of language lists
            for t in fluent_tags:
                tobj = self.from_json(t.get('keyword', t))
                if isinstance(tobj, Number):
                    tobj = str(tobj)
                if isinstance(tobj, dict):
                    for key, value in tobj.iteritems():
                        if key in schema_languages:
                            field_value[key].append(value)
                else:
                    field_value[default_language].append(tobj)

            package_dict[field_name] = field_value

            # update tags with all values from fluent_tags
            tag_list = [t['name'] for t in package_dict['tags']]
            for item in field_value.get('en', []) + field_value.get('fr', []):
                if item not in tag_list:
                    tag_list.append(item)
            package_dict['tags'] = [{'name': t} for t in tag_list]

        else:
            # strip trailing _translated part of field name
            if field_name.endswith(u'_translated'):
                package_fn = field_name[:-11]
            else:
                package_fn = field_name

            package_val = package_dict.get(package_fn, '')
            field_value = self.from_json(package_val)

            if isinstance(field_value, dict):  # assume biligual values already in data
                package_dict[field_name] = field_value
            else:
                # create bilingual dictinary. This will likely fail validation as it does not contain all the languages
                package_dict[field_name] = {}
                package_dict[field_name][default_language] = field_value

        handled_fields.append(field_name)

    def flatten_composite_keys(self, obj, new_obj={}, keys=[]):
        for key, value in obj.iteritems():
            if isinstance(value, dict):
                self.flatten_composite_keys(obj[key], new_obj, keys + [key])
            else:
                new_obj['_'.join(keys + [key])] = value
        return new_obj

    def handle_composite_harvest_dictinary(self, field, iso_values, extras, package_dict, handled_fields):
        sep = plugins.toolkit.h.composite_separator()
        field_name = field['field_name']
        if field_name in handled_fields:
            return

        field_value = iso_values.get(field_name, {})
        # add __extras field to package dict as composit expects fields to be located there
        if '__extras' not in package_dict:
            package_dict['__extras'] = {}

        # Calculate possable field names and try to populate value
        # this is helpfull when a composite field combination maps directly
        # to a iso_value. creates a dictinary from the values so the next block
        # is dealing with a consistant dictiniary structure
        if not field_value and field.get('preset', '') == 'composite':
            for sf in field['subfields']:
                computed_field_name = '-'.join([field_name, sf['field_name']])
                sf_value = iso_values.get(computed_field_name)
                if sf_value:
                    field_value[sf['field_name']] = sf_value

        # populate composite fields from multi-level dictinary
        if field_value and field.get('preset', '') == 'composite':
            if isinstance(field_value, list):
                field_value = field_value[0]
            field_value = self.flatten_composite_keys(field_value, {}, [])

            for key, value in field_value.iteritems():
                newKey = field_name + sep + key
                package_dict['__extras'][newKey] = value

            # remove from extras so as not to duplicate fields
            if extras.get(field_name):
                del extras[field_name]
            handled_fields.append(field_name)

        # populate composite repeating fields
        elif field_value and field.get('preset', '') == 'composite_repeating':
            if isinstance(field_value, dict):
                field_value[0] = field_value

            for idx, subitem in enumerate(field_value):
                # collaps subfields into one key value pair
                subitem = self.flatten_composite_keys(subitem, {}, [])
                for key, value in subitem.iteritems():
                    newKey = field_name + sep + str(idx + 1) + sep + key
                    package_dict['__extras'][newKey] = value

            # remove from extras so as not to duplicate fields
            if extras.get(field_name):
                del extras[field_name]
            handled_fields.append(field_name)

    def handle_scheming_harvest_dictinary(self, field, iso_values, extras, package_dict, handled_fields):
        field_name = field['field_name']
        if field_name in handled_fields:
            return
        iso_field_value = iso_values.get(field_name, {})
        extra_field_value = extras.get(field_name, "")

        # move schema fields, in extras, to package dictionary
        if field_name in extras and not package_dict.get(field_name, ''):
            package_dict[field_name] = extra_field_value
            del extras[field_name]
            handled_fields.append(field_name)
        # move schema fields, in iso_values, to package dictionary
        elif iso_field_value and not package_dict.get(field_name, ''):
            # convert list to single value for select fields (not multi-select)
            if field.get('preset', '') == 'select' and isinstance(iso_field_value, list):
                iso_field_value = iso_field_value[0]
            package_dict[field_name] = iso_field_value
            # remove from extras so as not to duplicate fields
            if extras.get(field_name):
                del extras[field_name]
            handled_fields.append(field_name)

import ckan.plugins as plugins
from ckan import model
import ckan.plugins.toolkit as toolkit
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.validation.validation import BaseValidator
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObjectError
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from sqlalchemy.orm.exc import StaleDataError
import ckan.lib.munge as munge
import json
import requests
from numbers import Number
import socket
import xml.etree.ElementTree as ET
import re
from six import string_types

import logging
log = logging.getLogger(__name__)


def load_json(j):
    try:
        new_val = json.loads(j)
    except Exception:
        new_val = j
    return new_val


def _get_xml_url_content(xml_url, urlopen_timeout, harvest_object):
    try:
        try:
            r = requests(xml_url, timeout=urlopen_timeout)
            ET.XML(r.content)  # test for valid xml
            return r
        except ET.ParseError as e:
            msg = '%s: %s. From external XML content at %s' % (type(e).__name__, str(e), xml_url)
            log.warn(msg)
            err = HarvestObjectError(message=msg, object=harvest_object, stage='Import')
            err.save()
        except requests.exceptions.Timeout as e:
            msg = '%s: %s. From external XML content at %s' % (type(e).__name__, str(e), xml_url)
            log.warn(msg)
            err = HarvestObjectError(message=msg, object=harvest_object, stage='Import')
            err.save()
        except requests.exceptions.TooManyRedirects as e:
            msg = 'HTTP too many redirects: %s' % e.code
            log.warn(msg)
            err = HarvestObjectError(message=msg, object=harvest_object, stage='Import')
            err.save()
        except requests.exceptions.RequestException as e:
            msg = 'HTTP request exception: %s' % e.code
            log.warn(msg)
            err = HarvestObjectError(message=msg, object=harvest_object, stage='Import')
            err.save()
        except Exception as e:
            msg = '%s: %s. From external XML content at %s' % (type(e).__name__, str(e), xml_url)
            log.warn(msg)
            err = HarvestObjectError(message=msg, object=harvest_object, stage='Import')
            err.save()
        finally:
            return ''

    except StaleDataError as e:
        log.warn('Harvest object %s is stail. Error object not created. %s' % (harvest_object.id, str(e)))


def _get_extra(key, package_dict):
    for extra in package_dict.get('extras', []):
        if extra['key'] == key:
            return extra


def _extract_xml_from_harvest_object(context, package_dict, harvest_object):
    content = harvest_object.content
    key = 'harvest_document_content'
    value = ''

    if content.startswith('<'):
        value = harvest_object.content
    else:
        log.warn('Unable to find harvest object "%s" '
                 'referenced by dataset "%s". Trying xml url',
                 harvest_object.id, package_dict['id'])

        # try reading from xml url
        xml_url = load_json(package_dict.get('xml_location_url'))
        if not xml_url:
            log.warn('Empty or Missing URL in xml_location_url field. External xml metadata will not be retreaved.')
        else:
            urlopen_timeout = float(toolkit.config.get('ckan.index_xml_url_read_timeout', '500')) / 1000.0  # get value in millieseconds but urllib assumes it is in seconds

            # single file
            if xml_url and isinstance(xml_url, string_types):
                value = _get_xml_url_content(xml_url, urlopen_timeout, harvest_object)

            # list of files
            if xml_url and isinstance(xml_url, list):
                for xml_file in xml_url:
                    value = value + '<doc>' + _get_xml_url_content(xml_url, urlopen_timeout, harvest_object) + '</doc>'

                if value:
                    value = '<?xml version="1.0" encoding="utf-8"?><docs>' + value + '</docs>'

            value = re.sub('\s+',' ', value) # remove extra white space
            value = re.sub('> <','><', value)
            value = re.sub('> ','>', value)
            value = re.sub(' <','<', value)
    if value:
        log.info('Success. External xml retrieved.')
        package_dict[key] = value
    return package_dict


class CIOOSCKANHarvester(CKANHarvester):

    def info(self):
        return {
            'name': 'ckan_cioos',
            'title': 'CKAN CIOOS',
            'description': 'Harvests remote CKAN instances with improved handling/indexing of external xml files',
            'form_config_interface': 'Text'
        }

    def modify_package_dict(self, package_dict, harvest_object):

        context = {
            'model': model,
            'session': model.Session,
            'user': toolkit.c.user
        }

        package_dict = _extract_xml_from_harvest_object(context, package_dict, harvest_object)

        existing_extra = _get_extra('metadata_created_source', package_dict)
        if not existing_extra:
            package_dict['extras'].append({'key': 'metadata_created_source', 'value': package_dict.get('metadata_created')})

        existing_extra = _get_extra('metadata_modified_source', package_dict)
        if not existing_extra:
            package_dict['extras'].append({'key': 'metadata_modified_source', 'value': package_dict.get('metadata_modified')})

        return package_dict


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
        if hasattr(entity, 'title_translated'):
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

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key, copied from ckanext-spatial/ckanext/spatial/harvesters/base.py
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def trim_values(self, values):
        if(isinstance(values, Number)):
            return values
        elif(isinstance(values, list)):
            return [self.trim_values(x) for x in values]
        elif(isinstance(values, dict)):
            return {k.strip(): self.trim_values(v) for k, v in values.items()}
        elif(isinstance(values, str)):
            try:
                json_object = json.loads(values)
            except ValueError:
                return values.strip()
            else:
                return json.dumps(self.trim_values(json_object))
        return values

    def cioos_guess_resource_format(self, url, use_mimetypes=True):
        '''
        Given a URL try to guess the best format to assign to the resource

        This function does not replace the guess_resource_format() in the base
        spatial harvester. In stead it adds some resource and file types that
        are missing from that function.

        Returns None if no format could be guessed.

        '''
        url = url.lower().strip()
        resource_types = {
            # ERDDAP
            'ERDDAP': ('/erddap/',),
        }

        for resource_type, parts in resource_types.items():
            if any(part in url for part in parts):
                return resource_type

        file_types = {
            'CSV': ('csv',),
            'PDF': ('pdf',),
            'TXT': ('txt',),
            'XML': ('xml',),
            'HTML': ('html',),
            'JSON': ('json',),
        }

        for file_type, extensions in file_types.items():
            if any(url.endswith(extension) for extension in extensions):
                return file_type

        return None

    def get_package_dict(self, context, data_dict):
        package_dict = data_dict['package_dict']
        iso_values = data_dict['iso_values']
        harvest_object = data_dict['harvest_object']
        source_config = json.loads(data_dict['harvest_object'].source.config)
        xml_location_url = self._get_object_extra(data_dict['harvest_object'], 'waf_location')
        xml_modified_date = self._get_object_extra(data_dict['harvest_object'], 'waf_modified_date')

        # convert extras key:value list to dictinary
        extras = {x['key']: x['value'] for x in package_dict.get('extras', [])}

        extras['xml_location_url'] = xml_location_url
        extras['xml_modified_date'] = xml_modified_date

        # copy some fields over from iso_values if they exist
        if(iso_values.get('limitations-on-public-access')):
            extras['limitations-on-public-access'] = iso_values.get('limitations-on-public-access')
        if(iso_values.get('access-constraints')):
            extras['access-constraints'] = iso_values.get('access-constraints')
        if(iso_values.get('use-constraints')):
            extras['use-constraints'] = iso_values.get('use-constraints')
        if(iso_values.get('use-constraints-code')):
            extras['use-constraints-code'] = iso_values.get('use-constraints-code')
        if(iso_values.get('legal-constraints-reference-code')):
            extras['legal-constraints-reference-code'] = iso_values.get('legal-constraints-reference-code')
        if(iso_values.get('distributor')):
            extras['distributor'] = iso_values.get('distributor')

        # load remote xml content
        package_dict = _extract_xml_from_harvest_object(context, package_dict, harvest_object)

        # Handle Scheming, Composit, and Fluent extensions
        loaded_plugins = plugins.toolkit.config.get("ckan.plugins")
        if 'scheming_datasets' in loaded_plugins:
            # composite = 'composite' in loaded_plugins
            fluent = 'fluent' in loaded_plugins

            log.debug('#### Scheming, Composite, or Fluent extensions found, processing dictinary ####')
            schema = plugins.toolkit.h.scheming_get_dataset_schema('dataset')

            # Package name, default harvester uses title or guid in that order.
            # we want to reverse that order, so guid or title. Also use english
            # title only for name
            title_as_name = self.from_json(package_dict.get('title', '{}')).get('en', package_dict['name'])
            name = munge.munge_name(extras.get('guid', title_as_name)).lower()
            package_dict['name'] = name

            # populate license_id
            package_dict['license_id'] = iso_values.get('legal-constraints-reference-code') or iso_values.get('use-constraints') or 'CC-BY-4.0'

            # populate citation
            package_dict['citation'] = iso_values.get('citation')

            # populate trlanslation method for bilingual field
            notes_translation_method = iso_values.get('abstract_translation_method')
            title_translation_method = iso_values.get('title_translation_method')
            if notes_translation_method:
                extras['notes_translation_method'] = notes_translation_method
            if title_translation_method:
                extras['title_translation_method'] = title_translation_method

            # iterate over schema fields and update package dictionary as needed
            for field in schema['dataset_fields']:
                handled_fields = []
                self.handle_composite_harvest_dictinary(field, iso_values, extras, package_dict, handled_fields)

                if fluent:
                    self.handle_fluent_harvest_dictinary(field, iso_values, package_dict, schema, handled_fields, source_config)

                self.handle_scheming_harvest_dictinary(field, iso_values, extras, package_dict, handled_fields)

            # populate resource format if missing
            for resource in package_dict.get('resources', []):
                if not resource.get('format'):
                    if (resource.get('resource_locator_protocol').startswith('http') or
                            resource.get('url').startswith('http')):
                        resource['format'] = 'text/html'

            # set default values
            package_dict['progress'] = extras.get('progress', 'onGoing')
            package_dict['frequency-of-update'] = extras.get('frequency-of-update', 'asNeeded')

        extras_as_list = []
        for key, value in extras.items():
            if package_dict.get(key, ''):
                log.error('extras %s found in package dict: key:%s value:%s', key, key, value)
            if isinstance(value, (list, dict)):
                extras_as_list.append({'key': key, 'value': json.dumps(value)})
            else:
                extras_as_list.append({'key': key, 'value': value})

        package_dict['extras'] = extras_as_list

        # update resource format
        resources = package_dict.get('resources', [])
        if len(resources):
            for resource in resources:
                url = resource.get('url', '').strip()
                format = resource.get('format') or ''
                if url:
                    format = self.cioos_guess_resource_format(url) or format
                resource['format'] = format
        package_dict['resources'] = resources
        return self.trim_values(package_dict)

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
            do_clean = toolkit.asbool(harvest_config.get('clean_tags', False))

            # init language key
            field_value = {sl: [] for sl in schema_languages}

            # process fluent_tags by convert list of language dictionaries into
            # a dictionary of language lists
            for t in fluent_tags:
                tobj = self.from_json(t.get('keyword', t))
                if isinstance(tobj, Number):
                    tobj = str(tobj)
                if isinstance(tobj, dict):
                    for key, value in tobj.items():
                        if key in schema_languages:
                            if do_clean:
                                if isinstance(value, list):
                                    value = [munge.munge_tag(kw) for kw in value]
                                else:
                                    value = munge.munge_tag(value)
                            field_value[key].append(value)
                else:
                    if do_clean:
                        tobj = munge.munge_tag(tobj)
                    field_value[default_language].append(tobj)

            package_dict[field_name] = field_value

            # update tags with all values from fluent_tags
            tag_list = [t['name'] for t in package_dict['tags']]
            for item in field_value.get('en', []) + field_value.get('fr', []):
                if item not in tag_list:
                    tag_list.append(item)
            package_dict['tags'] = [{'name': t} for t in tag_list]

        else:
            # Populate translated fields from core. this could have been done in
            # the spatial extensions. example 'title' -> 'title_translated'

            # strip trailing _translated part of field name
            if field_name.endswith(u'_translated'):
                package_fn = field_name[:-11]
            else:
                package_fn = field_name

            package_val = package_dict.get(package_fn, '')
            field_value = self.from_json(package_val)

            if isinstance(field_value, dict):  # assume bilingual values already in data
                package_dict[field_name] = field_value
            else:
                # create bilingual dictionary. This will likely fail validation as it does not contain all the languages
                package_dict[field_name] = {}
                package_dict[field_name][default_language] = field_value

        handled_fields.append(field_name)

    def flatten_composite_keys(self, obj, new_obj={}, keys=[]):
        for key, value in obj.items():
            if isinstance(value, dict):
                self.flatten_composite_keys(obj[key], new_obj, keys + [key])
            else:
                new_obj['_'.join(keys + [key])] = value
        return new_obj

    def handle_composite_harvest_dictinary(self, field, iso_values, extras, package_dict, handled_fields):
        sep = plugins.toolkit.h.scheming_composite_separator()
        field_name = field['field_name']
        if field_name in handled_fields:
            return

        field_value = iso_values.get(field_name, {})

        # populate composite fields from multi-level dictionary
        if field_value and field.get('simple_subfields'):
            if isinstance(field_value, list):
                field_value = field_value[0]
            field_value = self.flatten_composite_keys(field_value, {}, [])

            for key, value in field_value.items():
                newKey = field_name + sep + key
                package_dict[newKey] = value

            # remove from extras so as not to duplicate fields
            if extras.get(field_name):
                del extras[field_name]
            handled_fields.append(field_name)

        # populate composite repeating fields
        elif field_value and field.get('repeating_subfields'):
            if isinstance(field_value, dict):
                field_value[0] = field_value

            for idx, subitem in enumerate(field_value):
                # collapse subfields into one key value pair
                subitem = self.flatten_composite_keys(subitem, {}, [])
                for key, value in subitem.items():
                    newKey = field_name + sep + str(idx + 1) + sep + key
                    package_dict[newKey] = value

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

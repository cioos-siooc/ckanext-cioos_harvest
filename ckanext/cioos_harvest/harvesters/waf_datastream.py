from __future__ import print_function

import logging
import hashlib
import unicodedata

import requests
from sqlalchemy.orm import aliased
from sqlalchemy.exc import DataError

from ckan import model
from ckan.lib.helpers import json
from ckan.logic import ValidationError, NotFound, get_action

from ckan.plugins.core import SingletonPlugin, implements
from ckantoolkit import config

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
import ckanext.harvest.queue as queue

from ckanext.spatial.harvesters.waf import WAFHarvester
from ckanext.harvest.queue import get_connection_redis

from ckanext.cioos_harvest.harvesters.waf import WAFHarvesterISO19115_3

from lxml import etree

import boto3
from copy import deepcopy

log = logging.getLogger(__name__)


class DatastreamSitemapHarvester(WAFHarvesterISO19115_3):
    '''
    A Harvester for DataStream's ISO 19115-2 sitemap.

    Inherits CIOOS field handling (scheming/fluent/composite) from
    WAFHarvesterISO19115_3 but uses ckanext-spatial's standard ISODocument
    parser (not the ISO 19115-3 parser) and adds DataStream-specific
    processing: DOI normalization, AWS Translate auto-translation, and a
    single "Access DataStream" resource.
    '''

    implements(IHarvester)

    def __new__(cls, *args, **kwargs):
        if '_instance' not in cls.__dict__:
            cls._instance = object.__new__(cls)
        return cls._instance

    redis_translation_store = 'awsTranslations'
    translation_method_text = "text translated using the Amazon translate service / texte traduit à l'aide du service Amazon translate"

    def info(self):
        return {
            'name': 'datastream_sitemap',
            'title': 'Sitemap Harvester for datastream ISO19115-2',
            'description': 'site map listing datasets urls with avilable iso19115-2 xml'
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_document(self, document_string, harvest_object, validator=None):
        """Skip XSD validation — DataStream ISO 19115-2 documents use gmi:MI_Metadata
        which is not covered by the ISO 19139 schemas bundled with ckanext-spatial."""
        log.debug('Skipping XSD validation for DataStream harvester (GUID: %s)',
                  harvest_object.guid)
        return True, None, []

    def validate_config(self, source_config):
        """Strip validator_profiles before base validation — this harvester skips XSD validation."""
        if source_config:
            try:
                config_obj = json.loads(source_config)
                if 'validator_profiles' in config_obj:
                    log.info(
                        'DatastreamSitemapHarvester: ignoring validator_profiles %s '
                        '(XSD validation is skipped for this harvester)',
                        config_obj['validator_profiles'])
                    config_obj.pop('validator_profiles')
                    source_config = json.dumps(config_obj)
            except ValueError:
                pass
        return super(DatastreamSitemapHarvester, self).validate_config(source_config)

    # ------------------------------------------------------------------
    # Import stage — skip ISO 19115-3 monkey-patch
    # ------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """Use standard ckanext-spatial ISODocument (not the ISO 19115-3 parser).

        WAFHarvesterISO19115_3.import_stage() monkey-patches spatial_base.ISODocument
        with the custom ISO 19115-3 parser.  DataStream XML is ISO 19115-2
        (gmi:MI_Metadata) and must be parsed by the standard ISODocument, so we
        bypass that monkey-patch by calling WAFHarvester.import_stage directly.
        """
        return WAFHarvester.import_stage(self, harvest_object)

    # ------------------------------------------------------------------
    # Translation
    # ------------------------------------------------------------------

    def translate_string(self, redis_conn, string_to_translate, source_lang='en', target_lang='fr'):
        store_name = '%s_%s_to_%s' % (self.redis_translation_store, source_lang, target_lang)
        # check for string in redis
        redis_trans = redis_conn.hget(store_name, string_to_translate)
        if redis_trans:
            # replace non-breaking white space
            redis_trans = redis_trans.replace(u'\u00A0', ' ')
            log.debug('"%s" found in cache', string_to_translate)
            return redis_trans

        # if not exists, call aws translate
        try:
            translate = boto3.client(service_name='translate', use_ssl=True)
            aws_trans_obj = translate.translate_text(Text=string_to_translate, SourceLanguageCode=source_lang, TargetLanguageCode=target_lang)
            aws_trans = aws_trans_obj.get('TranslatedText')
            # replace non-breaking white space
            aws_trans = aws_trans.replace(u'\u00A0', ' ')

            # save translation to redis
            if aws_trans:
                log.debug('"%s" saved to cache', string_to_translate)
                redis_conn.hset(store_name, mapping={string_to_translate: aws_trans})
                return aws_trans
        except Exception as e:
            log.error('Could not translate text "%s": %s', string_to_translate[:80], e)

        return None

    # ------------------------------------------------------------------
    # License helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_use_limitation_url(harvest_object):
        """Parse the first gmd:useLimitation URL from harvest_object.content.

        DataStream XML stores the license as
        ``gmd:useLimitation/gco:CharacterString`` (free-text), not as
        ``gmd:useConstraints/gmd:MD_RestrictionCode`` (controlled vocabulary).
        ckanext-spatial's ISODocument only reads the latter, so ``extras['licence']``
        ends up as an empty list and ``license_id`` is never set.

        Returns the URL string, or ``None`` if it cannot be extracted.
        """
        try:
            content = getattr(harvest_object, 'content', None)
            if not isinstance(content, (str, bytes)):
                return None
            if isinstance(content, str):
                content = content.encode('utf-8')
            tree = etree.fromstring(content)
            ns = {
                'gmd': 'http://www.isotc211.org/2005/gmd',
                'gco': 'http://www.isotc211.org/2005/gco',
            }
            urls = tree.xpath(
                './/gmd:MD_LegalConstraints/gmd:useLimitation'
                '/gco:CharacterString/text()',
                namespaces=ns)
            return next((u.strip() for u in urls if u.strip()), None)
        except Exception as exc:
            log.debug('DataStream: could not parse useLimitation: %s', exc)
            return None

    @staticmethod
    def _resolve_license_id_from_url(url):
        """Reverse-lookup a license ID by its URL.

        Checks the CKAN license register first (populated when
        ``licenses_group_url`` is configured to the CIOOS license file), then
        falls back to loading the local ``ckan_license.json`` shipped with
        ``cioos-siooc-schema``.  Trailing slashes are normalised before
        comparison so ``https://…/by/1-0`` and ``https://…/by/1-0/`` both
        match.
        """
        url_stripped = url.rstrip('/')

        # 1. Try CKAN license register
        try:
            from ckan import model as _ckan_model
            register = _ckan_model.Package.get_license_register()
            for lid, lic in register.items():
                if getattr(lic, 'url', '').rstrip('/') == url_stripped:
                    return lid
        except Exception:
            pass

        # 2. Fall back to the CIOOS ckan_license.json
        try:
            from pathlib import Path as _Path
            import json as _json
            # waf_datastream.py: harvesters/ → cioos_harvest/ → ckanext/
            #   → ckanext-cioos_harvest/ → src_extensions/ (or src/)
            lic_path = (
                _Path(__file__).parent.parent.parent.parent.parent
                / 'cioos-siooc-schema' / 'ckan_license.json'
            )
            if lic_path.exists():
                for lic_data in _json.loads(lic_path.read_text(encoding='utf-8')):
                    if lic_data.get('url', '').rstrip('/') == url_stripped:
                        return lic_data['id']
        except Exception as exc:
            log.debug('DataStream: CIOOS license file lookup failed: %s', exc)

        return None

    # ------------------------------------------------------------------
    # Responsible party helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_cited_responsible_party(harvest_object):
        """Parse gmd:citedResponsibleParty elements from DataStream XML.

        Extracts individual/organisation names and roles from
        ``gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty``.
        Parties with identical ``(individual-name, organisation-name)`` keys are
        merged into a single dict; if merged, ``role`` becomes a list, otherwise
        it stays a plain string.

        Returns a list of dicts with keys:
            ``individual-name``, ``organisation-name``, ``role``
        """
        try:
            content = getattr(harvest_object, 'content', None)
            if not isinstance(content, (str, bytes)):
                return []
            if isinstance(content, str):
                content = content.encode('utf-8')
            tree = etree.fromstring(content)
            ns = {
                'gmd': 'http://www.isotc211.org/2005/gmd',
                'gco': 'http://www.isotc211.org/2005/gco',
            }
            parties = tree.xpath(
                './/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty',
                namespaces=ns)

            from collections import OrderedDict
            merged = OrderedDict()
            for party in parties:
                ind_names = party.xpath(
                    'gmd:individualName/gco:CharacterString/text()', namespaces=ns)
                org_names = party.xpath(
                    'gmd:organisationName/gco:CharacterString/text()', namespaces=ns)
                roles = party.xpath(
                    'gmd:role/gmd:CI_RoleCode/@codeListValue', namespaces=ns)

                ind_name = ind_names[0].strip() if ind_names else ''
                org_name = org_names[0].strip() if org_names else ''
                role = roles[0].strip() if roles else ''

                key = (ind_name, org_name)
                if key in merged:
                    existing = merged[key]['role']
                    if isinstance(existing, list):
                        existing.append(role)
                    else:
                        merged[key]['role'] = [existing, role]
                else:
                    merged[key] = {
                        'individual-name': ind_name,
                        'organisation-name': org_name,
                        'role': role,
                    }

            return list(merged.values())
        except Exception as exc:
            log.debug('DataStream: could not parse citedResponsibleParty: %s', exc)
            return []

    @staticmethod
    def _map_responsible_org_to_contact(responsible_organisation):
        """Convert ckanext-spatial responsible-organisation to CIOOS flat format.

        ckanext-spatial nested format::

            {'individual-name': ..., 'organisation-name': ...,
             'contact-info': {'email': ..., 'online-resource': ...}, 'role': ...}

        CIOOS schema expected flat format::

            {'individual-name': ..., 'organisation-name': ...,
             'contact-info_email': ..., 'contact-info_online-resource': ...,
             'role': ...}
        """
        result = []
        for org in (responsible_organisation or []):
            contact_info = org.get('contact-info') or {}
            result.append({
                'individual-name': org.get('individual-name', ''),
                'organisation-name': org.get('organisation-name', ''),
                'contact-info_email': contact_info.get('email', ''),
                'contact-info_online-resource': contact_info.get('online-resource', ''),
                'role': org.get('role', ''),
            })
        return result

    # ------------------------------------------------------------------
    # DOI normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_doi_url(identifier):
        """Return *identifier* as a fully-qualified HTTPS URL.

        DataStream XML sometimes stores the identifier as a bare DOI path
        (e.g. ``10.25976/rbmo-8i70``) rather than a full URL.  Everything
        downstream (resource URL, citation, unique-resource-identifier-full)
        needs a proper URL, so we normalise here.

        Rules (applied in order):
        1. Already a URL (starts with ``http://`` or ``https://``) — return
           as-is (but upgrade http → https for consistency).
        2. Starts with ``doi:`` — strip the prefix and prepend
           ``https://doi.org/``.
        3. Anything else — assume it is a bare DOI path and prepend
           ``https://doi.org/``.
        """
        if not identifier:
            return identifier
        identifier = identifier.strip()
        if identifier.lower().startswith('https://'):
            return identifier
        if identifier.lower().startswith('http://'):
            return 'https://' + identifier[7:]
        if identifier.lower().startswith('doi:'):
            return 'https://doi.org/' + identifier[4:]
        # bare DOI path or anything else
        log.debug('DataStream: normalising bare identifier to DOI URL: %s', identifier)
        return 'https://doi.org/' + identifier

    # ------------------------------------------------------------------
    # Package dict construction
    # ------------------------------------------------------------------

    def get_package_dict(self, iso_values, harvest_object):
        log.debug(" *** in waf_Datastream get_package_dict")

        # ----------------------------------------------------------------
        # Step 1 — Normalize language code (3-letter → 2-letter)
        # ----------------------------------------------------------------
        # ckanext-spatial returns 'eng' from gmd:LanguageCode; the CIOOS
        # select field only accepts 'en' or 'fr'.
        if iso_values.get('metadata-language'):
            iso_values['metadata-language'] = iso_values['metadata-language'][:2]

        primary_lang = (iso_values.get('metadata-language') or 'en')[:2]

        # ----------------------------------------------------------------
        # Step 2 — Normalise the DOI identifier to a full HTTPS URL
        # ----------------------------------------------------------------
        if iso_values.get('unique-resource-identifier'):
            iso_values['unique-resource-identifier'] = self._normalise_doi_url(
                iso_values['unique-resource-identifier'])
        doi_url = iso_values.get('unique-resource-identifier', '')

        redis_conn = get_connection_redis()

        # ----------------------------------------------------------------
        # Step 3 — Translate keywords and build iso_values['tags']
        # ----------------------------------------------------------------
        # The fluent_tags handler in WAFHarvesterISO19115_3 reads from
        # iso_values['tags'] to produce package_dict['keywords'] as a flat
        # dict {"en": [...], "fr": [...]}.  We build translated tag strings
        # here so that handler receives bilingual entries.
        #
        # Save the original keyword items before clearing 'tags' — we need
        # them after super() resets things.
        original_keywords = iso_values.get('keywords', [])

        translated_tags = []
        has_translation = False
        for item in original_keywords:
            keyword_raw = item.get('keyword', [])
            if not isinstance(keyword_raw, list):
                keyword_raw = [keyword_raw]

            for kw_raw in keyword_raw:
                if isinstance(kw_raw, bytes):
                    kw_raw = kw_raw.decode('utf-8')
                try:
                    kw_obj = json.loads(kw_raw)
                    if not isinstance(kw_obj, dict):
                        kw_obj = str(kw_obj)
                except (ValueError, TypeError):
                    kw_obj = kw_raw

                if isinstance(kw_obj, dict):
                    en_str = kw_obj.get('en', '') or ''
                    fr_str = kw_obj.get('fr', '') or ''
                    if isinstance(en_str, list):
                        en_str = en_str[0] if en_str else ''
                    if isinstance(fr_str, list):
                        fr_str = fr_str[0] if fr_str else ''
                else:
                    en_str = str(kw_obj) if kw_obj else ''
                    fr_str = ''

                if en_str and not fr_str:
                    en_str = unicodedata.normalize("NFKD", en_str.replace('"', ''))
                    fr_str = self.translate_string(redis_conn, en_str, 'en', 'fr') or en_str
                    has_translation = True
                elif fr_str and not en_str:
                    fr_str = unicodedata.normalize("NFKD", fr_str.replace('"', ''))
                    en_str = self.translate_string(redis_conn, fr_str, 'fr', 'en') or fr_str
                    has_translation = True

                if en_str or fr_str:
                    translated_tags.append(json.dumps({'en': en_str, 'fr': fr_str}))

        if not translated_tags:
            translated_tags = [json.dumps({'en': 'other', 'fr': 'autre'})]

        # Replace iso_values['tags'] with our translated bilingual entries.
        # The spatial base's tag processing expects dicts with a 'name' key;
        # it would fail on our JSON strings.  WAFHarvesterISO19115_3 calls
        # super().get_package_dict() first (which processes tags as dicts),
        # then runs the fluent handler which reads iso_values['tags'] directly.
        # We store the bilingual tags in a separate key and inject them into
        # iso_values['tags'] AFTER super() to avoid breaking the spatial base.
        iso_values['_datastream_tags'] = translated_tags
        iso_values['tags'] = []  # suppress spatial base tag processing

        if has_translation:
            iso_values['keywords_translation_method'] = {
                'en': '', 'fr': 'Keyword ' + self.translation_method_text}

        # ----------------------------------------------------------------
        # Step 4 — Translate title
        # ----------------------------------------------------------------
        title_raw = iso_values.get('title', '')
        if isinstance(title_raw, str) and not title_raw.strip().startswith('{'):
            en_title = title_raw
            fr_title = self.translate_string(redis_conn, en_title, 'en', 'fr') or en_title
            # Store as JSON dict — WAFHarvesterISO19115_3 decodes it for
            # both the plain title and the title_translated fluent field.
            iso_values['title'] = json.dumps({'en': en_title, 'fr': fr_title})
            iso_values['title_translation_method'] = {
                'en': '', 'fr': 'Title ' + self.translation_method_text}

        # ----------------------------------------------------------------
        # Step 5 — Translate abstract/notes
        # ----------------------------------------------------------------
        abstract_raw = iso_values.get('abstract', '')
        if isinstance(abstract_raw, str) and not abstract_raw.strip().startswith('{'):
            en_abstract = abstract_raw
            fr_abstract = self.translate_string(redis_conn, en_abstract, 'en', 'fr') or en_abstract
            iso_values['abstract'] = json.dumps({'en': en_abstract, 'fr': fr_abstract})
            iso_values['abstract_translation_method'] = {
                'en': '', 'fr': 'Description ' + self.translation_method_text}

        # ----------------------------------------------------------------
        # Step 6 — CIOOS field handling via WAFHarvesterISO19115_3
        # ----------------------------------------------------------------
        # This runs: _expand_point_bboxes, spatial base get_package_dict,
        # scheming/fluent/composite field handlers, license resolution,
        # ecv, metadata_created/modified, title/notes plain-string override.
        package_dict = super(DatastreamSitemapHarvester, self).get_package_dict(
            iso_values, harvest_object)

        # ----------------------------------------------------------------
        # Step 6.3 — Extras cleanup
        # ----------------------------------------------------------------
        # ckanext-spatial serializes gmd:EX_VerticalExtent elements as raw XML
        # strings when all sub-elements carry gco:nilReason="missing".
        # Those blobs carry no useful data and are not expected by the CIOOS portal.
        if 'vertical-extent' in package_dict:
            ve = package_dict['vertical-extent']
            # ckanext-spatial returns a list of raw XML str/bytes when all
            # sub-elements carry nilReason; real extent would be a list of dicts.
            if isinstance(ve, list) and (not ve or not isinstance(ve[0], dict)):
                del package_dict['vertical-extent']

        # ckanext-spatial serializes an empty access-constraints list as the
        # JSON string '[]'.  The CIOOS portal stores it as an empty string.
        for _e in package_dict.get('extras', []):
            if _e['key'] == 'access_constraints' and _e['value'] == '[]':
                _e['value'] = ''

        # ----------------------------------------------------------------
        # Step 6.4 — Truncate dataset-reference-date values to date-only
        # ----------------------------------------------------------------
        # ckanext-spatial's ISODocument returns full ISO datetime strings
        # (e.g. '2023-01-19T20:54:15.592Z'); the CIOOS portal expects
        # date-only strings (e.g. '2023-01-19').
        for _entry in package_dict.get('dataset-reference-date', []):
            if isinstance(_entry, dict) and _entry.get('value'):
                _entry['value'] = str(_entry['value'])[:10]

        # ----------------------------------------------------------------
        # Step 6.5 — License: useLimitation → licence extra / use-constraints / license_id
        # ----------------------------------------------------------------
        # DataStream XML stores the license as gmd:useLimitation/gco:CharacterString
        # (a free-text URL), not as gmd:useConstraints/gmd:MD_RestrictionCode.
        # ckanext-spatial's ISODocument only reads useConstraints, so extras['licence']
        # ends up as '[]' and license_id is never resolved by the base class.
        # We parse the raw XML directly, fix the extras, and do a URL-based
        # license ID lookup against the CIOOS ckan_license.json.
        _license_url = self._extract_use_limitation_url(harvest_object)
        if _license_url:
            # Fix extras.licence (set by ckanext-spatial as empty list → '[]')
            for _e in package_dict.get('extras', []):
                if _e['key'] == 'licence':
                    _e['value'] = _license_url
                    break
            else:
                package_dict.setdefault('extras', []).append(
                    {'key': 'licence', 'value': _license_url})

            # Add extras.use-constraints (mirrors plugin.py ISpatialHarvester logic)
            if not any(_e['key'] == 'use-constraints'
                       for _e in package_dict.get('extras', [])):
                package_dict['extras'].append(
                    {'key': 'use-constraints', 'value': _license_url})

            # Resolve license_id by URL when the base class could not set it
            if not package_dict.get('license_id'):
                package_dict['license_id'] = (
                    self._resolve_license_id_from_url(_license_url)
                    or _license_url
                )
                if package_dict['license_id'] == _license_url:
                    log.warning(
                        'license_id not resolved for URL %r — using URL as fallback',
                        _license_url)

        # ----------------------------------------------------------------
        # Step 6.6 — cited-responsible-party and metadata-point-of-contact
        # ----------------------------------------------------------------
        # ckanext-spatial's ISODocument does not populate these for
        # gmi:MI_Metadata documents.  We parse cited-responsible-party
        # directly from the raw XML (gmd:CI_Citation) and build
        # metadata-point-of-contact by flattening iso_values['responsible-organisation']
        # from the nested ckanext-spatial format into the CIOOS flat format.
        if not package_dict.get('cited-responsible-party'):
            _crp = self._parse_cited_responsible_party(harvest_object)
            if _crp:
                package_dict['cited-responsible-party'] = _crp

        if not package_dict.get('metadata-point-of-contact'):
            _resp_org = iso_values.get('responsible-organisation', [])
            _mpoc = self._map_responsible_org_to_contact(_resp_org)
            if _mpoc:
                package_dict['metadata-point-of-contact'] = _mpoc

        # ----------------------------------------------------------------
        # Step 6.7 — metadata-reference-date from gmd:dateStamp
        # ----------------------------------------------------------------
        # ISO 19115-2 has a single gmd:dateStamp with no type information.
        # The CIOOS portal expects a list of typed date entries; derive two
        # entries (Creation and Revision) from the single stamp, matching
        # the format produced by the ISO 19115-3 parser's infer chain.
        if not package_dict.get('metadata-reference-date'):
            _metadata_date = iso_values.get('metadata-date', '')
            if _metadata_date:
                _date_only = str(_metadata_date)[:10]
                package_dict['metadata-reference-date'] = [
                    {'type': 'Creation', 'value': _date_only},
                    {'type': 'Revision', 'value': _date_only},
                ]

        # ----------------------------------------------------------------
        # Step 7 — Inject translated keywords
        # ----------------------------------------------------------------
        # Now that super() has run, inject our pre-translated tags into
        # iso_values['tags'] and re-run the fluent_tags handler manually
        # to produce the correct {"en": [...], "fr": [...]} format.
        iso_values['tags'] = iso_values.pop('_datastream_tags', [])
        from ckan import plugins as p
        loaded_plugins = p.toolkit.config.get("ckan.plugins", "")
        if 'scheming_datasets' in loaded_plugins and 'fluent' in loaded_plugins:
            schema = p.toolkit.h.scheming_get_dataset_schema('dataset')
            from ckanext.cioos_harvest.harvesters.waf import _sanitize_tag
            schema_languages = p.toolkit.h.fluent_form_languages(schema=schema)
            kw_field_value = {lang: [] for lang in schema_languages}
            for t in iso_values['tags']:
                tobj = self.from_json(t)
                if isinstance(tobj, dict):
                    for key, value in tobj.items():
                        if key in schema_languages:
                            kw_field_value[key].append(_sanitize_tag(value))
                else:
                    kw_field_value[primary_lang].append(_sanitize_tag(str(tobj)))
            package_dict['keywords'] = kw_field_value
        package_dict['tags'] = []

        # ----------------------------------------------------------------
        # Step 8 — Fix name / id from DOI path
        # ----------------------------------------------------------------
        # WAFHarvesterISO19115_3 derives name from guid.replace('.', '-')
        # which leaves '/' un-replaced.  DataStream GUIDs are bare DOI
        # paths (e.g. '10.25976/30dz-8f05') so we replace both '.' and '/'.
        guid = iso_values.get('guid', '')
        if guid:
            name = guid.replace('.', '-').replace('/', '-')
            package_dict['name'] = name
            package_dict['id'] = name

        # ----------------------------------------------------------------
        # Step 9 — Citation as a bilingual dict
        # ----------------------------------------------------------------
        package_dict['citation'] = {'en': doi_url, 'fr': doi_url}

        # ----------------------------------------------------------------
        # Step 10 — unique-resource-identifier-full
        # ----------------------------------------------------------------
        package_dict['unique-resource-identifier-full'] = [{'code': doi_url}]

        # ----------------------------------------------------------------
        # Step 11 — Single "Access DataStream" resource
        # ----------------------------------------------------------------
        package_dict['resources'] = [
            {
                'url': doi_url,
                'name': 'Access DataStream',
                'name_translated': {'en': 'Access DataStream'},
                'description': '',
                'description_translated': {},
                'format': 'HTML',
                'resource_locator_protocol': '',
                'resource_locator_function': '',
            }]

        # ----------------------------------------------------------------
        # Step 12 — projects (mirrors plugin.py logic for standalone use)
        # ----------------------------------------------------------------
        if 'projects' not in package_dict:
            package_dict['projects'] = iso_values.get('keyword-project', [])

        # ----------------------------------------------------------------
        # Step 13 — temporal-extent as a top-level list with date-only values
        # ----------------------------------------------------------------
        # ckanext-spatial's SpatialHarvester stores temporal extent as two
        # separate extras ('temporal-extent-begin', 'temporal-extent-end')
        # with full ISO datetime strings.  The CIOOS portal expects a
        # top-level list of {'begin': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}.
        # Build this from iso_values (preferred) or fall back to extras.
        if not package_dict.get('temporal-extent'):
            te_raw = iso_values.get('temporal-extent', [])
            if te_raw:
                entries = []
                for ex in te_raw:
                    entry = {}
                    if ex.get('begin'):
                        entry['begin'] = ex['begin'][:10]
                    if ex.get('end'):
                        entry['end'] = ex['end'][:10]
                    if entry:
                        entries.append(entry)
                if entries:
                    package_dict['temporal-extent'] = entries
            else:
                # Fall back to the extras that ckanext-spatial added
                begin = end = ''
                kept_extras = []
                for e in package_dict.get('extras', []):
                    if e['key'] == 'temporal-extent-begin':
                        begin = (e['value'] or '')[:10]
                    elif e['key'] == 'temporal-extent-end':
                        end = (e['value'] or '')[:10]
                    else:
                        kept_extras.append(e)
                if begin or end:
                    package_dict['temporal-extent'] = [{'begin': begin, 'end': end}]
                    package_dict['extras'] = kept_extras

        # ----------------------------------------------------------------
        # Step 14 — EOV default
        # ----------------------------------------------------------------
        if not package_dict.get('eov'):
            package_dict['eov'] = ['other']

        return package_dict

    # ------------------------------------------------------------------
    # Gather stage
    # ------------------------------------------------------------------

    def gather_stage(self, harvest_job, collection_package_id=None):
        log = logging.getLogger(__name__ + '.WAF.gather')
        log.debug('WafHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        source_url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        ######  Get current harvest object out of db ######

        url_to_modified_db = {}  ## mapping of url to last_modified in db
        url_to_ids = {}  ## mapping of url to guid in db


        HOExtraAlias1 = aliased(HOExtra)
        HOExtraAlias2 = aliased(HOExtra)
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id, HOExtraAlias1.value, HOExtraAlias2.value).\
                                    join(HOExtraAlias1, HarvestObject.extras).\
                                    join(HOExtraAlias2, HarvestObject.extras).\
                                    filter(HOExtraAlias1.key=='waf_modified_date').\
                                    filter(HOExtraAlias2.key=='waf_location').\
                                    filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)


        for guid, package_id, modified_date, url in query:
            url_to_modified_db[url] = modified_date
            url_to_ids[url] = (guid, package_id)

        ######  Get current list of records from source ######

        # Get contents of sitemap.xml
        # https://datastream.org/dataset/sitemap.xml
        try:
            sitemap_response = requests.get(source_url, timeout=60)
            sitemap_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (source_url, e), harvest_job)
            return None

        sitemape_content = sitemap_response.text

        # convert xml content to lxml etree
        sitemap_tree = etree.fromstring(str.encode(sitemape_content))

        # using dataset urls, generate url to xml metadata files. aka add /iso19115.xml to end
        url_to_modified_harvest = {}  ## mapping of url to last_modified in harvest
        try:
            for url_node in sitemap_tree.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
                loc_node = url_node.find(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                last_modified_node = url_node.find(".//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")
                url = loc_node.text + '/iso19115.xml'
                modified_date = last_modified_node.text
                url_to_modified_harvest[url] = modified_date
        except Exception as e:
            msg = 'Error extracting URLs from %s, error was %r' % (source_url, e)
            self._save_gather_error(msg, harvest_job)
            return None

        ######  Compare source and db ######

        harvest_locations = set(url_to_modified_harvest.keys())
        old_locations = set(url_to_modified_db.keys())

        new = harvest_locations - old_locations
        delete = old_locations - harvest_locations
        possible_changes = old_locations & harvest_locations
        change = []

        for item in possible_changes:
            if (not url_to_modified_harvest[item] or not url_to_modified_db[item]  # if there is no date assume change
                    or url_to_modified_harvest[item] > url_to_modified_db[item]):
                change.append(item)

        def create_extras(url, date, status):
            extras = [HOExtra(key='waf_modified_date', value=date),
                      HOExtra(key='waf_location', value=url),
                      HOExtra(key='status', value=status)]
            if collection_package_id:
                extras.append(
                    HOExtra(key='collection_package_id',
                            value=collection_package_id)
                )
            return extras


        ids = []
        for location in new:
            guid = hashlib.md5(location.encode('utf8', 'ignore')).hexdigest()
            obj = HarvestObject(job=harvest_job,
                                extras=create_extras(location,
                                                     url_to_modified_harvest[location],
                                                     'new'),
                                guid=guid
                               )
            obj.save()
            ids.append(obj.id)

        for location in change:
            obj = HarvestObject(job=harvest_job,
                                extras=create_extras(location,
                                                     url_to_modified_harvest[location],
                                                     'change'),
                                guid=url_to_ids[location][0],
                                package_id=url_to_ids[location][1],
                               )
            obj.save()
            ids.append(obj.id)

        for location in delete:
            obj = HarvestObject(job=harvest_job,
                                extras=create_extras('', '', 'delete'),
                                guid=url_to_ids[location][0],
                                package_id=url_to_ids[location][1],
                               )
            model.Session.query(HarvestObject).\
                  filter_by(guid=url_to_ids[location][0]).\
                  update({'current': False}, False)

            obj.save()
            ids.append(obj.id)

        if len(ids) > 0:
            log.debug('{0} objects sent to the next stage: {1} new, {2} change, {3} delete'.format(
                len(ids), len(new), len(change), len(delete)))
            return ids
        else:
            if config.get('ckan.harvest.status_mail.all', False):
                self._save_gather_error('No records to change',
                                         harvest_job)
            else:
                log.debug('No records to change')
            return []

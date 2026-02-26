import logging

from ckan import model as ckan_model
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
import ckanext.spatial.harvesters.base as spatial_base

from ckanext.cioos_harvest.harvesters.waf_cioos import WAFHarvesterCIOOS
from ckanext.cioos_harvest.model.harvested_metadata_iso19115_3 import ISODocument_iso19115_3

log = logging.getLogger(__name__)


class WAFHarvesterISO19115_3(WAFHarvesterCIOOS, SingletonPlugin):
    '''
    A dedicated WAF harvester for ISO 19115-3 XML metadata files.

    Accepts only documents with an mdb:MD_Metadata root element. Unlike the
    generic WAFHarvesterCIOOS, this harvester does not perform runtime standard
    detection — it always routes documents to the ISO 19115-3 specific parser
    (ISODocument_iso19115_3) and always skips ISO 19139 XSD validation.

    Use WAFHarvesterCIOOS for ISO 19139 sources instead.
    '''

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

    def _validate_document(self, document_string, harvest_object, validator=None):
        """Skip ISO 19139 XSD validation — this harvester is ISO 19115-3 only."""
        log.debug('Skipping ISO 19139 XSD validation for ISO 19115-3 harvester (GUID: %s)', harvest_object.guid)
        return True, None, []

    def import_stage(self, harvest_object):
        """Always route to ISODocument_iso19115_3 — no runtime standard detection."""
        original_document = self._get_object_extra(harvest_object, 'original_document')
        content = original_document or harvest_object.content or ''

        # Move content to harvest_object.content and clear extras so the base
        # class takes the direct-parse path (not the XSLT transform path).
        harvest_object.content = content
        harvest_object.save()
        for extra in harvest_object.extras:
            if extra.key in ('original_document', 'original_format'):
                extra.value = ''
        ckan_model.Session.flush()

        old_cls = spatial_base.ISODocument
        spatial_base.ISODocument = ISODocument_iso19115_3
        try:
            return super(WAFHarvesterISO19115_3, self).import_stage(harvest_object)
        finally:
            spatial_base.ISODocument = old_cls

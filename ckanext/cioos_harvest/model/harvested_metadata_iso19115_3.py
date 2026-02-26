"""ISO 19115-3 specific metadata parser.

This module provides XML parsing classes for ISO 19115-3 (mdb:MD_Metadata)
metadata documents. Unlike the dual-path classes in harvested_metadata_iso19139.py,
these classes contain ONLY ISO 19115-3 XPaths (mdb:, cit:, mri:, gex:, etc.
namespaces) and do not attempt to parse ISO 19139 (gmd:) documents.
"""

import logging

from ckanext.cioos_harvest.model.harvested_metadata_iso19139 import (
    MappedXmlElement_iso19139,
    ISODocument_iso19139,
)

log = logging.getLogger(__name__)


class ISOElement_iso19115_3(MappedXmlElement_iso19139):
    """Base element class with ISO 19115-3 namespaces only.

    Drops all ISO 19139 specific namespaces (gts, gmx, gsr, gss, gmd, srv, gmi).
    Retains gco (shared) and all ISO 19115-3 domain namespaces.
    """

    namespaces = {
        # ISO 19115-3 gco namespace (different from ISO 19139's http://www.isotc211.org/2005/gco)
        "gco": "http://standards.iso.org/iso/19115/-3/gco/1.0",
        "gml": "http://www.opengis.net/gml",
        "gml32": "http://www.opengis.net/gml/3.2",
        "xlink": "http://www.w3.org/1999/xlink",
        "xml": "http://www.w3.org/XML/1998/namespace",
        # ISO 19115-3 domain namespaces
        "cit": "http://standards.iso.org/iso/19115/-3/cit/2.0",
        "gcx": "http://standards.iso.org/iso/19115/-3/gcx/1.0",
        "gex": "http://standards.iso.org/iso/19115/-3/gex/1.0",
        "lan": "http://standards.iso.org/iso/19115/-3/lan/1.0",
        "mcc": "http://standards.iso.org/iso/19115/-3/mcc/1.0",
        "mco": "http://standards.iso.org/iso/19115/-3/mco/1.0",
        "mdb": "http://standards.iso.org/iso/19115/-3/mdb/2.0",
        "mds": "http://standards.iso.org/iso/19115/-3/mds/2.0",
        "mmi": "http://standards.iso.org/iso/19115/-3/mmi/1.0",
        "mrd": "http://standards.iso.org/iso/19115/-3/mrd/1.0",
        "mri": "http://standards.iso.org/iso/19115/-3/mri/1.0",
        "mrs": "http://standards.iso.org/iso/19115/-3/mrs/1.0",
        "srv": "http://standards.iso.org/iso/19115/-3/srv/2.0",
    }


class ISOResourceLocator_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="url",
            search_paths=["cit:linkage/gco:CharacterString/text()"],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="function",
            search_paths=[
                "cit:function/cit:CI_OnLineFunctionCode/@codeListValue",
                "cit:function/cit:CI_OnLineFunctionCode/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="name",
            search_paths=["cit:name/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="description",
            search_paths=["cit:description/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="protocol",
            search_paths=["cit:protocol/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="protocol-request",
            search_paths=["cit:protocolRequest/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="application-profile",
            search_paths=["cit:applicationProfile/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="distribution-format",
            search_paths=[
                "ancestor::mrd:MD_DigitalTransferOptions/mrd:distributionFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title/gco:CharacterString/text()"
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="distributor-format",
            search_paths=[
                "ancestor::mrd:MD_Distributor/mrd:distributorFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title/gco:CharacterString/text()"
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="offline",
            search_paths=[
                "ancestor::mrd:MD_DigitalTransferOptions/mrd:offLine/mrd:MD_Medium/cit:CI_Citation/cit:title/gco:CharacterString/text()"
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="transfer-size",
            search_paths=[
                "ancestor::mrd:MD_DigitalTransferOptions/mrd:transferSize/gco:Real/text()"
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="units-of-distribution",
            search_paths=[
                "ancestor::mrd:MD_DigitalTransferOptions/mrd:unitsOfDistribution/gco:CharacterString/text()"
            ],
            multiplicity="0..1",
        ),
    ]


class ISOResponsibleParty_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="individual-name",
            search_paths=[
                "cit:party/cit:CI_Individual/cit:name/gco:CharacterString/text()",
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="organisation-name",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="position-name",
            search_paths=[
                "cit:party/cit:CI_Individual/cit:positionName/gco:CharacterString/text()",
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:positionName/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="contact-info",
            search_paths=[
                "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact",
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact",
                "cit:party/cit:CI_Organisation/cit:contactInfo/cit:CI_Contact",
            ],
            multiplicity="0..1",
            elements=[
                ISOElement_iso19115_3(
                    name="email",
                    search_paths=[
                        "cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                ISOResourceLocator_iso19115_3(
                    name="online-resource",
                    search_paths=["cit:onlineResource/cit:CI_OnlineResource"],
                    multiplicity="0..1",
                ),
            ],
        ),
        ISOElement_iso19115_3(
            name="role",
            search_paths=[
                "cit:role/cit:CI_RoleCode/@codeListValue",
                "cit:role/cit:CI_RoleCode/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISOResponsibleParty_cioos_iso19115_3(ISOElement_iso19115_3):
    """Flat parser for cit:CI_Responsibility targeting the CIOOS schema.

    Produces the flat key structure expected by the CIOOS
    ``repeating_subfields`` schema preset (e.g. ``contact-info_email``,
    ``individual-uri_code``, ``organisation-uri_code``).  Unlike the generic
    :class:`ISOResponsibleParty_iso19115_3`, this class does **not** nest
    ``contact-info`` as a sub-dict and adds ORCID/ROR URI extraction.

    Role aggregation (one CI_Responsibility per role in ISO 19115-3) is
    handled separately by
    :meth:`ISODocument_iso19115_3.infer_responsible_party_contacts`.
    """

    elements = [
        ISOElement_iso19115_3(
            name="individual-name",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:name/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="organisation-name",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        # Email: prefer individual contact, fall back to organisation contact
        ISOElement_iso19115_3(
            name="contact-info_email",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
                "cit:party/cit:CI_Organisation/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        # Online resource URL: individual contact only (org URL intentionally omitted)
        ISOElement_iso19115_3(
            name="contact-info_online-resource",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:onlineResource/cit:CI_OnlineResource/cit:linkage/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:onlineResource/cit:CI_OnlineResource/cit:linkage/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        # Individual identifier (ORCID or similar)
        ISOElement_iso19115_3(
            name="individual-uri_code",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="individual-uri_authority",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="individual-uri_code-space",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="individual-uri_version",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()",
                "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        # Organisation identifier (ROR or similar)
        ISOElement_iso19115_3(
            name="organisation-uri_code",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="organisation-uri_authority",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="organisation-uri_code-space",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="organisation-uri_version",
            search_paths=[
                "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="role",
            search_paths=[
                "cit:role/cit:CI_RoleCode/@codeListValue",
                "cit:role/cit:CI_RoleCode/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISODataFormat_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="name",
            search_paths=["gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="version",
            search_paths=[],
            multiplicity="0..1",
        ),
    ]


class ISOReferenceDate_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="type",
            search_paths=[
                "cit:dateType/cit:CI_DateTypeCode/@codeListValue",
                "cit:dateType/cit:CI_DateTypeCode/text()",
            ],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="value",
            search_paths=[
                "cit:date/gco:Date/text()",
                "cit:date/gco:DateTime/text()",
            ],
            multiplicity="1",
        ),
    ]


class ISOBoundingBox_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="west",
            search_paths=["gex:westBoundLongitude/gco:Decimal/text()"],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="east",
            search_paths=["gex:eastBoundLongitude/gco:Decimal/text()"],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="north",
            search_paths=["gex:northBoundLatitude/gco:Decimal/text()"],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="south",
            search_paths=["gex:southBoundLatitude/gco:Decimal/text()"],
            multiplicity="1",
        ),
    ]


class ISOTemporalExtent_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="begin",
            search_paths=[
                "gml:beginPosition/text()",
                "gml32:beginPosition/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="end",
            search_paths=[
                "gml:endPosition/text()",
                "gml32:endPosition/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISOVerticalExtent_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="min",
            search_paths=["gex:minimumValue/gco:Real/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="max",
            search_paths=["gex:maximumValue/gco:Real/text()"],
            multiplicity="0..1",
        ),
    ]


class ISOLocalised_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="default",
            search_paths=["gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="local",
            search_paths=["lan:PT_FreeText/lan:textGroup"],
            multiplicity="*",
            elements=[
                ISOElement_iso19115_3(
                    name="value",
                    search_paths=["lan:LocalisedCharacterString/text()"],
                    multiplicity="0..1",
                ),
                ISOElement_iso19115_3(
                    name="language_code",
                    search_paths=["lan:LocalisedCharacterString/@locale"],
                    multiplicity="0..1",
                ),
            ],
        ),
    ]


class ISOKeyword_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOLocalised_iso19115_3(
            name="keywords",
            search_paths=["mri:keyword"],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="type",
            search_paths=[
                "mri:type/mri:MD_KeywordTypeCode/@codeListValue",
                "mri:type/mri:MD_KeywordTypeCode/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISOIdentifier_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="code",
            search_paths=[
                "mcc:code/gco:CharacterString/text()",
                "mcc:code/gcx:Anchor/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="authority",
            search_paths=[
                "mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
                "mcc:authority/cit:CI_Citation/cit:title/gcx:Anchor/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="code-space",
            search_paths=[
                "mcc:codeSpace/gco:CharacterString/text()",
                "mcc:codeSpace/gcx:Anchor/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="version",
            search_paths=[
                "mcc:version/gco:CharacterString/text()",
                "mcc:version/gcx:Anchor/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISOAggregationInfo_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="aggregate-dataset-name",
            search_paths=[
                "mri:name/cit:CI_Citation/cit:title/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="aggregate-dataset-identifier_code",
            search_paths=[
                "mri:name/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="association-type",
            search_paths=[
                "mri:associationType/mri:DS_AssociationTypeCode/@codeListValue",
                "mri:associationType/mri:DS_AssociationTypeCode/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="initiative-type",
            search_paths=[
                "mri:initiativeType/mri:DS_InitiativeTypeCode/@codeListValue",
                "mri:initiativeType/mri:DS_InitiativeTypeCode/text()",
            ],
            multiplicity="0..1",
        ),
    ]


class ISOBrowseGraphic_iso19115_3(ISOElement_iso19115_3):

    elements = [
        ISOElement_iso19115_3(
            name="file",
            search_paths=["mcc:fileName/gco:CharacterString/text()"],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="description",
            search_paths=["mcc:fileDescription/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="type",
            search_paths=["mcc:fileType/gco:CharacterString/text()"],
            multiplicity="0..1",
        ),
    ]


class ISODocument_iso19115_3(ISODocument_iso19139):
    """ISO 19115-3 specific document parser.

    Inherits all infer_* business logic from ISODocument_iso19139 but overrides
    the elements list to use exclusively ISO 19115-3 XPaths (mdb:, cit:, mri:,
    gex:, lan:, mco:, etc. namespaces).

    ISO 19139 documents (gmd: namespace root) will not be correctly parsed by
    this class. Use ISODocument_iso19139 for ISO 19139 / dual-standard documents.
    """

    # Keys accessed directly (no .get()) by ckanext-spatial's base.get_package_dict().
    # These must never be removed by drop_empty_objects even when their value is [] or {}.
    _SPATIAL_REQUIRED_KEYS = frozenset({
        'spatial-reference-system', 'guid', 'dataset-reference-date',
        'metadata-language', 'metadata-date', 'coupled-resource',
        'contact-email', 'frequency-of-update', 'spatial-data-service-type',
        'tags', 'title', 'abstract', 'responsible-organisation', 'bbox',
        'temporal-extent-begin', 'temporal-extent-end',
    })

    def drop_empty_objects(self, values):
        to_drop = []
        for key, value in values.items():
            if key not in self._SPATIAL_REQUIRED_KEYS and (value == {} or value == []):
                to_drop.append(key)
        for key in to_drop:
            del values[key]

    def infer_values(self, values):
        """Call parent infer_* chain then apply ISO 19115-3 specific transforms."""
        super(ISODocument_iso19115_3, self).infer_values(values)
        self.infer_aggregation_info(values)
        self.infer_responsible_party_contacts(values)

    def infer_responsible_party_contacts(self, values):
        """Merge duplicate contacts that differ only in role.

        ISO 19115-3 encodes one role per ``CI_Responsibility`` element.  When
        the same individual or organisation appears with multiple roles the
        parser produces one dict per role.  This method collapses them into a
        single dict whose ``role`` value is a sorted list of all roles (or a
        plain string when only one role exists).

        Contacts are considered identical when they share the same combination
        of ``individual-name``, ``organisation-name``, ``individual-uri_code``,
        and ``organisation-uri_code``.
        """
        for field in ('metadata-point-of-contact', 'cited-responsible-party', 'distributor'):
            contacts = values.get(field)
            if not isinstance(contacts, list):
                continue

            merged = []
            seen = {}  # dedup key → index into merged

            for contact in contacts:
                if not isinstance(contact, dict):
                    continue
                key = (
                    contact.get('individual-name', '') or '',
                    contact.get('organisation-name', '') or '',
                    contact.get('individual-uri_code', '') or '',
                    contact.get('organisation-uri_code', '') or '',
                )
                role = contact.get('role', '')

                if key in seen:
                    existing = merged[seen[key]]
                    existing_role = existing.get('role')
                    if isinstance(existing_role, list):
                        if role and role not in existing_role:
                            existing_role.append(role)
                    elif existing_role:
                        if role and role != existing_role:
                            existing['role'] = [existing_role, role]
                    else:
                        if role:
                            existing['role'] = role
                else:
                    seen[key] = len(merged)
                    merged.append(dict(contact))

            # Sort role lists for deterministic output
            for contact in merged:
                if isinstance(contact.get('role'), list):
                    contact['role'] = sorted(contact['role'])

            values[field] = merged

    def infer_aggregation_info(self, values):
        """Wrap plain-string aggregate-dataset-name values in a language dict.

        The CIOOS schema declares aggregate-dataset-name with preset fluent_text,
        which expects a language-keyed dict (e.g. {'en': 'Title'}).  The raw XPath
        result from the ISO 19115-3 XML is a plain string, so we wrap it here using
        the document's default language.
        """
        default_lang = (values.get('metadata-language') or 'en')[:2]
        for item in values.get('aggregation-info', []):
            name = item.get('aggregate-dataset-name')
            if isinstance(name, str):
                item['aggregate-dataset-name'] = {default_lang: name}

    def infer_temporal_vertical_extent(self, values):
        """Produce both the CIOOS dict format and the flat list keys required by
        ckanext-spatial's base.get_package_dict (temporal-extent-begin/end as lists).

        The parent collapses the raw list of temporal extent dicts into a single
        merged dict {begin: ..., end: ...}. The scheming schema for temporal-extent
        uses repeating_subfields which expects a list of dicts, so we wrap it back.
        """
        super(ISODocument_iso19115_3, self).infer_temporal_vertical_extent(values)
        # After the parent call, temporal-extent is either:
        #   - a dict {begin: '...', end: '...'} if extent was found, OR
        #   - still the original [] if no extent elements existed
        te = values.get('temporal-extent', {})
        if isinstance(te, dict):
            values['temporal-extent-begin'] = [te['begin']] if te.get('begin') else []
            values['temporal-extent-end'] = [te['end']] if te.get('end') else []
            # Wrap back into a list so scheming repeating_subfields validator is satisfied.
            values['temporal-extent'] = [te]
        else:
            values['temporal-extent-begin'] = []
            values['temporal-extent-end'] = []

    elements = [
        ISOElement_iso19115_3(
            name="metadata-language",
            search_paths=[
                "mdb:defaultLocale/lan:PT_Locale/lan:language/lan:LanguageCode/@codeListValue",
                "mdb:defaultLocale/lan:PT_Locale/lan:language/lan:LanguageCode/text()",
            ],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="metadata-standard-name",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="metadata-standard-version",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="resource-type",
            search_paths=[
                "mdb:metadataScope/mdb:MD_MetadataScope/mdb:resourceScope/mcc:MD_ScopeCode/@codeListValue",
                "mdb:metadataScope/mdb:MD_MetadataScope/mdb:resourceScope/mcc:MD_ScopeCode/text()",
            ],
            multiplicity="*",
        ),
        ISOResponsibleParty_cioos_iso19115_3(
            name="metadata-point-of-contact",
            search_paths=[
                "mdb:contact/cit:CI_Responsibility",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='pointOfContact']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='pointOfContact']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:contact/cit:CI_Responsibility",
            ],
            multiplicity="1..*",
        ),
        ISOResponsibleParty_cioos_iso19115_3(
            name="cited-responsible-party",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility",
            ],
            multiplicity="1..*",
        ),
        ISOReferenceDate_iso19115_3(
            name="metadata-reference-date",
            search_paths=["mdb:dateInfo/cit:CI_Date"],
            multiplicity="1..*",
        ),
        ISOElement_iso19115_3(
            name="metadata-date",
            search_paths=[
                "mdb:dateInfo/cit:CI_Date/cit:date/gco:Date/text() | mdb:dateInfo/cit:CI_Date/cit:date/gco:DateTime/text()",
            ],
            multiplicity="1..*",
        ),
        ISOElement_iso19115_3(
            name="spatial-reference-system",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOLocalised_iso19115_3(
            name="title",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:title",
            ],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="alternate-title",
            search_paths=[],
            multiplicity="*",
        ),
        ISOReferenceDate_iso19115_3(
            name="dataset-reference-date",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:date/cit:CI_Date",
            ],
            multiplicity="1..*",
        ),
        ISOElement_iso19115_3(
            name="unique-resource-identifier",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOIdentifier_iso19115_3(
            name="guid",
            search_paths=["mdb:metadataIdentifier/mcc:MD_Identifier"],
            multiplicity="0..1",
        ),
        ISOIdentifier_iso19115_3(
            # this would commonly be a DOI
            name="unique-resource-identifier-full",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:identifier/mcc:MD_Identifier",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="presentation-form",
            search_paths=[],
            multiplicity="*",
        ),
        ISOLocalised_iso19115_3(
            name="abstract",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:abstract",
            ],
            multiplicity="1",
        ),
        ISOElement_iso19115_3(
            name="purpose",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOResponsibleParty_iso19115_3(
            name="responsible-organisation",
            search_paths=[
                "mdb:contact/cit:CI_Responsibility[cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='pointOfContact' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='originator' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='owner' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='rightsHolder' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='pointOfContact' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='originator' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='owner' and cit:party/cit:CI_Organisation]",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='rightsHolder' and cit:party/cit:CI_Organisation]",
            ],
            multiplicity="1..*",
        ),
        ISOElement_iso19115_3(
            name="frequency-of-update",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceAndUpdateFrequency/mmi:MD_MaintenanceFrequencyCode/@codeListValue",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceAndUpdateFrequency/mmi:MD_MaintenanceFrequencyCode/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="maintenance-note",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceNote/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="progress",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:status/mcc:MD_ProgressCode/@codeListValue",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:status/mcc:MD_ProgressCode/text()",
            ],
            multiplicity="*",
        ),
        ISOKeyword_iso19115_3(
            name="keywords",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:descriptiveKeywords/mri:MD_Keywords",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="keyword-inspire-theme",
            search_paths=[],
            multiplicity="*",
        ),
        # Deprecated: kept for backwards compatibility
        ISOElement_iso19115_3(
            name="keyword-controlled-other",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="usage",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="limitations-on-public-access",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:otherConstraints/gco:CharacterString/text()",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:otherConstraints/gcx:Anchor/text()",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="access-constraints",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:accessConstraints/mco:MD_RestrictionCode/@codeListValue",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:accessConstraints/mco:MD_RestrictionCode/text()",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="use-constraints",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_Constraints/mco:useLimitation/gco:CharacterString/text()",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useLimitation/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="use-constraints-code",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_Constraints/mco:useConstraints/mco:MD_RestrictionCode/@codeListValue",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_Constraints/mco:useConstraints/mco:MD_RestrictionCode/text()",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useConstraints/mco:MD_RestrictionCode/@codeListValue",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useConstraints/mco:MD_RestrictionCode/text()",
            ],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="legal-constraints-reference-code",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:resourceConstraints/mco:MD_LegalConstraints/mco:reference/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ISOAggregationInfo_iso19115_3(
            name="aggregation-info",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:associatedResource/mri:MD_AssociatedResource",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="spatial-data-service-type",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="spatial-resolution",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="spatial-resolution-units",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="equivalent-scale",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="dataset-language",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="topic-category",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:topicCategory/mri:MD_TopicCategoryCode/text()",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="extent-controlled",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="extent-free-text",
            search_paths=[],
            multiplicity="*",
        ),
        ISOBoundingBox_iso19115_3(
            name="bbox",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:geographicElement/gex:EX_GeographicBoundingBox",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="spatial",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:geographicElement/gex:EX_BoundingPolygon/gex:polygon/node()",
            ],
            multiplicity="*",
        ),
        ISOTemporalExtent_iso19115_3(
            name="temporal-extent",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:temporalElement/gex:EX_TemporalExtent/gex:extent/gml32:TimePeriod",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:temporalElement/gex:EX_TemporalExtent/gex:extent/gml:TimePeriod",
            ],
            multiplicity="*",
        ),
        ISOVerticalExtent_iso19115_3(
            name="vertical-extent",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:verticalElement/gex:EX_VerticalExtent",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="vertical-extent-crs",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:extent/gex:EX_Extent/gex:verticalElement/gex:EX_VerticalExtent/gex:verticalCRSId/mrs:MD_ReferenceSystem/mrs:referenceSystemIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="coupled-resource",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="additional-information-source",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISODataFormat_iso19115_3(
            name="data-format",
            search_paths=[
                "mdb:distributionInfo/mrd:MD_Distribution/mrd:distributionFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title",
            ],
            multiplicity="*",
        ),
        ISOResponsibleParty_cioos_iso19115_3(
            name="distributor",
            search_paths=[
                "mdb:distributionInfo/mrd:MD_Distribution/mrd:distributor/mrd:MD_Distributor/mrd:distributorContact/cit:CI_Responsibility",
            ],
            multiplicity="*",
        ),
        ISOResourceLocator_iso19115_3(
            name="resource-locator",
            search_paths=[
                "mdb:distributionInfo/mrd:MD_Distribution/mrd:transferOptions/mrd:MD_DigitalTransferOptions/mrd:onLine/cit:CI_OnlineResource | mdb:distributionInfo/mrd:MD_Distribution/mrd:distributor/mrd:MD_Distributor/mrd:distributorTransferOptions/mrd:MD_DigitalTransferOptions/mrd:onLine/cit:CI_OnlineResource",
            ],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="resource-locator-identification",
            search_paths=[],
            multiplicity="*",
        ),
        ISOElement_iso19115_3(
            name="conformity-specification",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="conformity-pass",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="conformity-explanation",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOElement_iso19115_3(
            name="lineage",
            search_paths=[],
            multiplicity="0..1",
        ),
        ISOBrowseGraphic_iso19115_3(
            name="browse-graphic",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:graphicOverview/mcc:MD_BrowseGraphic",
            ],
            multiplicity="*",
        ),
        ISOResponsibleParty_iso19115_3(
            name="author",
            search_paths=[
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='author']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='originator']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='owner']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='author']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='originator']",
                "mdb:identificationInfo/*[contains(local-name(), 'Identification')]/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='owner']",
            ],
            multiplicity="1..*",
        ),
    ]

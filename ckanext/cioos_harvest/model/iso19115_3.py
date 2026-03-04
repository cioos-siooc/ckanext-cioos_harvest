"""Functional ISO 19115-3 metadata parser.

Direct lxml XPath approach — no class-per-field DSL.
All parsing logic lives in plain functions; ISODocument is a thin wrapper
required for ckanext-spatial monkey-patch compatibility.
"""

import datetime
import json
import logging
import numbers
import re

from lxml import etree
from shapely import wkt as shapely_wkt
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import mapping as shapely_mapping
from shapely.geometry import shape as shapely_shape

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Namespace map
# ---------------------------------------------------------------------------

NS = {
    "gco":  "http://standards.iso.org/iso/19115/-3/gco/1.0",
    "gml":  "http://www.opengis.net/gml",
    "gml32": "http://www.opengis.net/gml/3.2",
    "xlink": "http://www.w3.org/1999/xlink",
    "xml":  "http://www.w3.org/XML/1998/namespace",
    "cit":  "http://standards.iso.org/iso/19115/-3/cit/2.0",
    "gcx":  "http://standards.iso.org/iso/19115/-3/gcx/1.0",
    "gex":  "http://standards.iso.org/iso/19115/-3/gex/1.0",
    "lan":  "http://standards.iso.org/iso/19115/-3/lan/1.0",
    "mcc":  "http://standards.iso.org/iso/19115/-3/mcc/1.0",
    "mco":  "http://standards.iso.org/iso/19115/-3/mco/1.0",
    "mdb":  "http://standards.iso.org/iso/19115/-3/mdb/2.0",
    "mds":  "http://standards.iso.org/iso/19115/-3/mds/2.0",
    "mmi":  "http://standards.iso.org/iso/19115/-3/mmi/1.0",
    "mrd":  "http://standards.iso.org/iso/19115/-3/mrd/1.0",
    "mri":  "http://standards.iso.org/iso/19115/-3/mri/1.0",
    "mrl":  "http://standards.iso.org/iso/19115/-3/mrl/1.0",
    "mrs":  "http://standards.iso.org/iso/19115/-3/mrs/1.0",
    "srv":  "http://standards.iso.org/iso/19115/-3/srv/2.0",
}

# Shorthand XPath prefix for mdb:identificationInfo elements
_ID = "mdb:identificationInfo/*[contains(local-name(), 'Identification')]"

# Keys that ckanext-spatial accesses directly; kept even when empty.
_SPATIAL_REQUIRED_KEYS = frozenset({
    'spatial-reference-system', 'guid', 'dataset-reference-date',
    'metadata-language', 'metadata-date', 'coupled-resource',
    'contact-email', 'frequency-of-update', 'spatial-data-service-type',
    'tags', 'title', 'abstract', 'responsible-organisation', 'bbox',
    'temporal-extent-begin', 'temporal-extent-end',
})


# ---------------------------------------------------------------------------
# GML geometry helper
# ---------------------------------------------------------------------------

def _parse_gml_to_shapely(gml_string):
    """Parse a GML fragment (Polygon/MultiPolygon) to a shapely geometry.

    Supports GML 3.1 and GML 3.2 namespaces. Returns a ShapelyPolygon or None.

    Handles two GML coordinate encodings:
    - gml:posList  — flat space-separated numbers: "x1 y1 x2 y2 ..."
    - gml:coordinates — comma-separated pairs: "x1,y1 x2,y2 ..."
    """
    GML_NS = ('http://www.opengis.net/gml', 'http://www.opengis.net/gml/3.2')
    try:
        content = gml_string.encode('utf-8') if isinstance(gml_string, str) else gml_string
        tree = etree.fromstring(content)
    except etree.XMLSyntaxError:
        return None
    for ns in GML_NS:
        # gml:posList — flat space-separated coordinate sequence
        elem = tree.find('.//{%s}posList' % ns)
        if elem is not None and elem.text:
            parts = elem.text.strip().split()
            try:
                coords = [(float(parts[i]), float(parts[i + 1]))
                          for i in range(0, len(parts) - 1, 2)]
                if len(coords) >= 3:
                    return ShapelyPolygon(coords)
            except (ValueError, IndexError):
                pass

        # gml:coordinates — comma-delimited tuples, space-separated: "lon,lat lon,lat ..."
        elem = tree.find('.//{%s}coordinates' % ns)
        if elem is not None and elem.text:
            try:
                coords = [
                    (float(pair.split(',')[0]), float(pair.split(',')[1]))
                    for pair in elem.text.strip().split()
                    if ',' in pair
                ]
                if len(coords) >= 3:
                    return ShapelyPolygon(coords)
            except (ValueError, IndexError):
                pass
    return None


# ---------------------------------------------------------------------------
# Low-level XPath helpers
# ---------------------------------------------------------------------------

def _x(el, xpath):
    """Execute XPath against el with the ISO 19115-3 namespace map."""
    return el.xpath(xpath, namespaces=NS)


def _text(el, xpath):
    """Return the first non-empty string result of an XPath, or ''.

    All whitespace (including embedded newlines and tabs) is normalised to
    single spaces so that CKAN tag/field validators never see raw line-break
    characters from XML text content.
    """
    for r in _x(el, xpath):
        if isinstance(r, str):
            cleaned = ' '.join(r.split())
            if cleaned:
                return cleaned
    return ''


def _texts(el, xpath):
    """Return all non-empty string results as a list, whitespace normalised."""
    result = []
    for r in _x(el, xpath):
        if isinstance(r, str):
            cleaned = ' '.join(r.split())
            if cleaned:
                result.append(cleaned)
    return result


def _first_x(el, *xpaths):
    """Try each XPath in order; return elements from the first that has results.

    Replicates MappedXmlElement.read_value() 'first match wins' semantics.
    Use this whenever the original class defined multiple search_paths.
    A union XPath (A|B in one string) uses _x/_text/_texts directly.
    """
    for xpath in xpaths:
        results = _x(el, xpath)
        if results:
            return results
    return []


def _first_text(el, *xpaths):
    """Return the first non-empty string from the first XPath that matches."""
    for xpath in xpaths:
        t = _text(el, xpath)
        if t:
            return t
    return ''


def _first_texts(el, *xpaths):
    """Return all strings from the first XPath that has non-empty results."""
    for xpath in xpaths:
        results = _texts(el, xpath)
        if results:
            return results
    return []


def _localised_raw(el):
    """Extract a localised value in ISOLocalised_iso19115_3 format.

    Returns {'default': str, 'local': list} where each local item is
    {'value': str, 'language_code': str}.  This structure is consumed by
    _local_to_dict() and _infer_multilingual().
    """
    default = _text(el, "gco:CharacterString/text()")
    local_items = []
    for tg in _x(el, "lan:PT_FreeText/lan:textGroup"):
        value = _text(tg, "lan:LocalisedCharacterString/text()")
        lang_code = _text(tg, "lan:LocalisedCharacterString/@locale")
        if value or lang_code:
            local_items.append({'value': value, 'language_code': lang_code})
    return {'default': default, 'local': local_items}


# ---------------------------------------------------------------------------
# Utility functions (shared by field parsers and post-processing)
# ---------------------------------------------------------------------------

def _iso_date_time_to_utc(value):
    """Convert an ISO 8601 date/datetime string to 'YYYY-MM-DD HH:MM:SS' UTC."""
    if not value:
        raise ValueError("Empty date value")
    value = value.replace("Z", "+0000")
    post_remove = 99
    if re.search(r'[+-]\d{4}', value):
        post_remove = -5
        timedelta = datetime.timedelta(
            hours=int(value[-5:][1:3]),
            minutes=int(value[-5:][-2:])
        ) * (-1 if value[-5:][0] == '+' else 1)
    else:
        timedelta = datetime.timedelta(hours=0, minutes=0)
    try:
        utc_dt = datetime.datetime.strptime(value, '%Y-%m-%d')
    except ValueError:
        try:
            utc_dt = datetime.datetime.strptime(value[:post_remove], '%Y-%m-%dT%H:%M:%S') + timedelta
        except Exception:
            utc_dt = datetime.datetime.strptime(value[:post_remove], '%Y-%m-%dT%H:%M:%S.%f') + timedelta
    return utc_dt.strftime('%Y-%m-%d %H:%M:%S')


def _clean_lang_key(key):
    """Normalise a locale string to a 2-letter ISO 639-1 code."""
    key = re.sub(r"[^a-zA-Z]+", "", key)
    return key[:2]


def _unescape_unicode(encoded_str):
    """Decode double-escaped unicode sequences in a string."""
    if not encoded_str:
        return encoded_str
    while re.search(r'\\u[0-9a-fA-F]{4}', encoded_str):
        if isinstance(encoded_str, str):
            encoded_str = encoded_str.encode('raw_unicode_escape').decode('unicode_escape')
        else:
            encoded_str = encoded_str.decode().encode('raw_unicode_escape').decode('unicode_escape')
    encoded_str = encoded_str.replace('\\\\n', '\n')
    encoded_str = encoded_str.replace('\\\n', '\n')
    encoded_str = encoded_str.replace('\\n', '\n')
    return encoded_str


def _local_to_dict(item, default_lang_key):
    """Convert a {default, local} localised dict to {lang: text} dict."""
    out = {}
    default = item.get('default', '')
    if default:
        default = default.strip()
    default = _unescape_unicode(default)
    if default and len(default) > 1:
        out[default_lang_key] = default

    local = item.get('local')
    if isinstance(local, dict):
        lang_key = _clean_lang_key(local.get('language_code', ''))
        lang_val = _unescape_unicode((local.get('value') or '').strip())
        if lang_val and len(lang_val) > 1:
            out[lang_key] = lang_val
    elif isinstance(local, list):
        for local_item in local:
            lang_key = _clean_lang_key(local_item.get('language_code', ''))
            lang_val = _unescape_unicode((local_item.get('value') or '').strip())
            if lang_val and len(lang_val) > 1:
                out[lang_key] = lang_val
    return out



# ---------------------------------------------------------------------------
# Field parsers (one function per composite element class)
# ---------------------------------------------------------------------------

def _parse_identifier(el):
    """Parse a mcc:MD_Identifier element → {code, authority, code-space, version}."""
    return {
        'code': _first_text(el,
            "mcc:code/gco:CharacterString/text()",
            "mcc:code/gcx:Anchor/text()",
        ),
        'authority': _first_text(el,
            "mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
            "mcc:authority/cit:CI_Citation/cit:title/gcx:Anchor/text()",
        ),
        'code-space': _first_text(el,
            "mcc:codeSpace/gco:CharacterString/text()",
            "mcc:codeSpace/gcx:Anchor/text()",
        ),
        'version': _first_text(el,
            "mcc:version/gco:CharacterString/text()",
            "mcc:version/gcx:Anchor/text()",
        ),
    }


def _parse_reference_date(el):
    """Parse a cit:CI_Date element → {type, value}."""
    return {
        'type': _first_text(el,
            "cit:dateType/cit:CI_DateTypeCode/@codeListValue",
            "cit:dateType/cit:CI_DateTypeCode/text()",
        ),
        'value': _first_text(el,
            "cit:date/gco:Date/text()",
            "cit:date/gco:DateTime/text()",
        ),
    }


def _parse_responsible_party_cioos(el):
    """Flat responsible party for CIOOS schema (ISOResponsibleParty_cioos_iso19115_3).

    Returns the flat key structure expected by the CIOOS repeating_subfields
    schema preset (contact-info_email, individual-uri_code, etc.).
    """
    return {
        'individual-name': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:name/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:name/gco:CharacterString/text()",
        ),
        'organisation-name': _text(el,
            "cit:party/cit:CI_Organisation/cit:name/gco:CharacterString/text()"),
        'contact-info_email': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
            "cit:party/cit:CI_Organisation/cit:contactInfo/cit:CI_Contact/cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()",
        ),
        'contact-info_online-resource': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:onlineResource/cit:CI_OnlineResource/cit:linkage/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact/cit:onlineResource/cit:CI_OnlineResource/cit:linkage/gco:CharacterString/text()",
        ),
        'individual-uri_code': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
        ),
        'individual-uri_authority': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()",
        ),
        'individual-uri_code-space': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()",
        ),
        'individual-uri_version': _first_text(el,
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()",
            "cit:party/cit:CI_Individual/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()",
        ),
        'organisation-uri_code': _text(el,
            "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()"),
        'organisation-uri_authority': _text(el,
            "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:authority/cit:CI_Citation/cit:title/gco:CharacterString/text()"),
        'organisation-uri_code-space': _text(el,
            "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:codeSpace/gco:CharacterString/text()"),
        'organisation-uri_version': _text(el,
            "cit:party/cit:CI_Organisation/cit:partyIdentifier/mcc:MD_Identifier/mcc:version/gco:CharacterString/text()"),
        'role': _first_text(el,
            "cit:role/cit:CI_RoleCode/@codeListValue",
            "cit:role/cit:CI_RoleCode/text()",
        ),
    }


def _parse_responsible_party(el):
    """Nested responsible party format (ISOResponsibleParty_iso19115_3).

    Used for responsible-organisation and author fields.
    contact-info is '' when absent, or {'email': ..., 'online-resource': ...}.
    """
    contact_els = _first_x(el,
        "cit:party/cit:CI_Individual/cit:contactInfo/cit:CI_Contact",
        "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:contactInfo/cit:CI_Contact",
        "cit:party/cit:CI_Organisation/cit:contactInfo/cit:CI_Contact",
    )
    contact_info = ''
    if contact_els:
        ci = contact_els[0]
        email = _text(ci, "cit:address/cit:CI_Address/cit:electronicMailAddress/gco:CharacterString/text()")
        or_els = _x(ci, "cit:onlineResource/cit:CI_OnlineResource")
        online_resource = _parse_resource_locator(or_els[0]) if or_els else ''
        contact_info = {'email': email, 'online-resource': online_resource}

    return {
        'individual-name': _first_text(el,
            "cit:party/cit:CI_Individual/cit:name/gco:CharacterString/text()",
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:name/gco:CharacterString/text()",
        ),
        'organisation-name': _text(el,
            "cit:party/cit:CI_Organisation/cit:name/gco:CharacterString/text()"),
        'position-name': _first_text(el,
            "cit:party/cit:CI_Individual/cit:positionName/gco:CharacterString/text()",
            "cit:party/cit:CI_Organisation/cit:individual/cit:CI_Individual/cit:positionName/gco:CharacterString/text()",
        ),
        'contact-info': contact_info,
        'role': _first_text(el,
            "cit:role/cit:CI_RoleCode/@codeListValue",
            "cit:role/cit:CI_RoleCode/text()",
        ),
    }


def _parse_resource_locator(el):
    """Parse a cit:CI_OnlineResource element."""
    name_els = _x(el, "cit:name")
    desc_els = _x(el, "cit:description")
    _empty_localised = {'default': '', 'local': []}
    return {
        'url': _text(el, "cit:linkage/gco:CharacterString/text()"),
        'function': _first_text(el,
            "cit:function/cit:CI_OnLineFunctionCode/@codeListValue",
            "cit:function/cit:CI_OnLineFunctionCode/text()",
        ),
        'name': _localised_raw(name_els[0]) if name_els else dict(_empty_localised),
        'description': _localised_raw(desc_els[0]) if desc_els else dict(_empty_localised),
        'protocol': _text(el, "cit:protocol/gco:CharacterString/text()"),
        'protocol-request': _text(el, "cit:protocolRequest/gco:CharacterString/text()"),
        'application-profile': _text(el, "cit:applicationProfile/gco:CharacterString/text()"),
        'distribution-format': _texts(el,
            "ancestor::mrd:MD_DigitalTransferOptions/mrd:distributionFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title/gco:CharacterString/text()"),
        'distributor-format': _texts(el,
            "ancestor::mrd:MD_Distributor/mrd:distributorFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title/gco:CharacterString/text()"),
        'offline': _texts(el,
            "ancestor::mrd:MD_DigitalTransferOptions/mrd:offLine/mrd:MD_Medium/cit:CI_Citation/cit:title/gco:CharacterString/text()"),
        'transfer-size': _text(el,
            "ancestor::mrd:MD_DigitalTransferOptions/mrd:transferSize/gco:Real/text()"),
        'units-of-distribution': _text(el,
            "ancestor::mrd:MD_DigitalTransferOptions/mrd:unitsOfDistribution/gco:CharacterString/text()"),
    }


def _parse_keyword_group(el):
    """Parse a mri:MD_Keywords element → {keywords: [localised_raw, ...], type: str}.

    The type is taken from mri:MD_KeywordTypeCode when present; if absent, the
    thesaurus name (mri:thesaurusName/cit:CI_Citation/cit:title) is used as a
    fallback so that controlled-vocabulary blocks (e.g. the CIOOS EOV thesaurus)
    are classified correctly.
    """
    ktype = _first_text(el,
        "mri:type/mri:MD_KeywordTypeCode/@codeListValue",
        "mri:type/mri:MD_KeywordTypeCode/text()",
    )
    if not ktype:
        ktype = _first_text(el,
            "mri:thesaurusName/cit:CI_Citation/cit:title/gco:CharacterString/text()",
        )
    return {
        'keywords': [_localised_raw(kw) for kw in _x(el, "mri:keyword")],
        'type': ktype,
    }


def _parse_bbox(el):
    """Parse a gex:EX_GeographicBoundingBox element."""
    return {
        'west': _text(el, "gex:westBoundLongitude/gco:Decimal/text()"),
        'east': _text(el, "gex:eastBoundLongitude/gco:Decimal/text()"),
        'north': _text(el, "gex:northBoundLatitude/gco:Decimal/text()"),
        'south': _text(el, "gex:southBoundLatitude/gco:Decimal/text()"),
    }


def _parse_temporal_extent(el):
    """Parse a gml:TimePeriod element → {begin, end}."""
    return {
        'begin': _first_text(el,
            "gml:beginPosition/text()",
            "gml32:beginPosition/text()",
        ),
        'end': _first_text(el,
            "gml:endPosition/text()",
            "gml32:endPosition/text()",
        ),
    }


def _parse_vertical_extent(el):
    """Parse a gex:EX_VerticalExtent element → {min, max}."""
    return {
        'min': _text(el, "gex:minimumValue/gco:Real/text()"),
        'max': _text(el, "gex:maximumValue/gco:Real/text()"),
    }


def _parse_aggregation_info(el):
    """Parse a mri:MD_AssociatedResource element."""
    name_els = _x(el, "mri:name/cit:CI_Citation/cit:title")
    identifier_els = _x(el, "mri:name/cit:CI_Citation/cit:identifier/mcc:MD_Identifier")
    return {
        'aggregate-dataset-name': (
            _localised_raw(name_els[0]) if name_els else {'default': '', 'local': []}
        ),
        'aggregate-dataset-identifier': (
            _parse_identifier(identifier_els[0]) if identifier_els
            else {'code': '', 'authority': '', 'code-space': '', 'version': ''}
        ),
        'association-type': _first_text(el,
            "mri:associationType/mri:DS_AssociationTypeCode/@codeListValue",
            "mri:associationType/mri:DS_AssociationTypeCode/text()",
        ),
        'initiative-type': _first_text(el,
            "mri:initiativeType/mri:DS_InitiativeTypeCode/@codeListValue",
            "mri:initiativeType/mri:DS_InitiativeTypeCode/text()",
        ),
    }


def _parse_browse_graphic(el):
    """Parse a mcc:MD_BrowseGraphic element."""
    return {
        'file': _text(el, "mcc:fileName/gco:CharacterString/text()"),
        'description': _text(el, "mcc:fileDescription/gco:CharacterString/text()"),
        'type': _text(el, "mcc:fileType/gco:CharacterString/text()"),
    }


def _parse_data_format(el):
    """Parse a cit:title element from a mrd:MD_Format entry."""
    return {
        'name': _text(el, "gco:CharacterString/text()"),
        'version': '',
    }


def _parse_lineage_citation_inner(el, default_lang):
    """Parse a cit:CI_Citation element for use inside lineage sub-fields.

    Used by additional-documentation, source.citation, and processing-step.reference.
    Returns a plain dict (callers JSON-encode it before storing).

    Fields extracted:
        title      — localised citation title ({lang: text})
        identifier — from cit:identifier/mcc:MD_Identifier
        url        — link from cit:onlineResource/cit:CI_OnlineResource

    Args:
        el:           The cit:CI_Citation lxml element.
        default_lang: Two-letter ISO 639-1 code for the record's default locale.
    """
    title_els = _x(el, "cit:title")
    id_els = _x(el, "cit:identifier/mcc:MD_Identifier")
    or_els = _x(el, "cit:onlineResource/cit:CI_OnlineResource")
    title_raw = _localised_raw(title_els[0]) if title_els else {'default': '', 'local': []}
    return {
        'title': _local_to_dict(title_raw, default_lang),
        'identifier': _parse_identifier(id_els[0]) if id_els else {},
        'url': _text(or_els[0], "cit:linkage/gco:CharacterString/text()") if or_els else '',
    }


def _parse_lineage(el, default_lang):
    """Parse a mrl:LI_Lineage element into the CIOOS schema repeating_subfields format.

    Returns a dict matching the 'lineage' schema field's repeating_subfields:
        statment               — {lang: text} for the lineage statement
                                 (fluent_text preset; note: 'statment' is an
                                  intentional schema field_name typo)
        scope                  — MD_ScopeCode string (select preset);
                                 defaults to 'dataset' when absent
        additional-documentation — list of JSON-encoded citation dicts
        source                 — list of JSON-encoded source dicts
        processing-step        — list of JSON-encoded processing-step dicts

    The nested collections (additional-documentation, source, processing-step)
    are serialised as JSON strings because their schema subfields use the
    ``scheming_multiple_text`` input validator + ``scheming_load_json`` output
    validator: CKAN receives them as strings and decodes them on read-back.

    Args:
        el:           The mrl:LI_Lineage lxml element.
        default_lang: Two-letter ISO 639-1 code for the record's default locale.
    """
    # XPath spelling is correct ('statement'); 'statment' is the schema typo.
    stmt_els = _x(el, "mrl:statement")
    statment = _local_to_dict(
        _localised_raw(stmt_els[0]) if stmt_els else {'default': '', 'local': []},
        default_lang,
    )

    scope = _first_text(el,
        "mrl:scope/mcc:MD_Scope/mcc:level/mcc:MD_ScopeCode/@codeListValue",
        "mrl:scope/mcc:MD_Scope/mcc:level/mcc:MD_ScopeCode/text()",
    ) or 'dataset'

    additional_docs = [
        json.dumps(_parse_lineage_citation_inner(cit_el, default_lang))
        for cit_el in _x(el, "mrl:additionalDocumentation/cit:CI_Citation")
    ]

    sources = []
    for src_el in _x(el, "mrl:source/mrl:LI_Source"):
        desc_els = _x(src_el, "mrl:description")
        desc_raw = _localised_raw(desc_els[0]) if desc_els else {'default': '', 'local': []}
        cit_els = _x(src_el, "mrl:sourceCitation/cit:CI_Citation")
        sources.append(json.dumps({
            'description': _local_to_dict(desc_raw, default_lang),
            'citation': _parse_lineage_citation_inner(cit_els[0], default_lang) if cit_els else {},
        }))

    steps = []
    for step_el in _x(el, "mrl:processStep/mrl:LI_ProcessStep"):
        desc_els = _x(step_el, "mrl:description")
        desc_raw = _localised_raw(desc_els[0]) if desc_els else {'default': '', 'local': []}
        ref_els = _x(step_el, "mrl:reference/cit:CI_Citation")
        steps.append(json.dumps({
            'description': _local_to_dict(desc_raw, default_lang),
            'reference': _parse_lineage_citation_inner(ref_els[0], default_lang) if ref_els else {},
        }))

    return {
        'statment': statment,
        'scope': scope,
        'additional-documentation': additional_docs,
        'source': sources,
        'processing-step': steps,
    }


def _infer_citation(values):
    """Build a CSL-JSON citation per language for the 'citation' fluent_markdown field.

    Assembles a citation.js-compatible JSON array string per language using
    data already parsed into ``values``:

    - ``guid``                      → CSL ``id``
    - ``cited-responsible-party``   → ``author`` list (publisher role excluded)
    - ``dataset-reference-date``    → ``issued`` (first non-creation date)
    - ``title`` / ``abstract``      → per-language title / abstract (JSON strings
                                       after ``_infer_multilingual`` has run)
    - ``_cit_edition``              → ``edition``  (private key from _parse_document)
    - ``_cit_edition_date``         → ``edition-date`` (private key from _parse_document)
    - publisher from cited-responsible-party (publisher role) → ``publisher``

    The ``URL`` field is left empty; plugin.py injects the CKAN dataset URL
    because ``ckan.site_url`` requires an active CKAN app context.

    Args:
        values: The iso_values dict (modified in-place).
    """
    default_lang = (values.get('metadata-language') or 'en')[:2]
    guid = values.get('guid', '')

    # Authors: all non-publisher cited-responsible-party entries, deduplicated
    authors = []
    publisher = ''
    seen_authors = set()
    for party in values.get('cited-responsible-party', []):
        role = party.get('role') or ''
        # role is a plain string before _infer_normalize_contact_roles runs
        role_lower = role.lower() if isinstance(role, str) else ''
        name = party.get('organisation-name') or party.get('individual-name') or ''
        if role_lower == 'publisher':
            if not publisher and name:
                publisher = name
        else:
            if name and name not in seen_authors:
                authors.append({'literal': name})
                seen_authors.add(name)

    # Issued: first non-creation dataset-reference-date, fall back to metadata-reference-date
    issued = []
    for rd in values.get('dataset-reference-date', []):
        if (rd.get('type') or '').lower() == 'creation':
            continue
        date_str = (rd.get('value') or '')[:10]
        if date_str:
            issued = [{'date-parts': date_str.split('-')}]
            break
    if not issued:
        for rd in values.get('metadata-reference-date', []):
            date_str = (rd.get('value') or '')[:10]
            if date_str:
                issued = [{'date-parts': date_str.split('-')}]
                break

    # title and abstract are JSON strings after _infer_multilingual has run
    try:
        titles = json.loads(values.get('title') or '{}')
    except Exception:
        titles = {default_lang: ''}
    try:
        abstracts = json.loads(values.get('abstract') or '{}')
    except Exception:
        abstracts = {default_lang: ''}

    # Pop transient private keys set by _parse_document from the CI_Citation element
    edition = values.pop('_cit_edition', '') or ''
    edition_date = values.pop('_cit_edition_date', '') or ''

    all_langs = set(list(titles.keys()) + list(abstracts.keys()))
    if not all_langs:
        all_langs = {default_lang}

    citation = {}
    for lang in sorted(all_langs):
        csl_obj = {
            'type': 'dataset',
            'id': guid,
            'author': authors,
            'issued': issued,
            'abstract': abstracts.get(lang) or abstracts.get(default_lang, ''),
            'edition': edition,
            'edition-date': edition_date,
            'publisher': publisher,
            'title': titles.get(lang) or titles.get(default_lang, ''),
            'language': lang,
            'URL': '',
        }
        citation[lang] = json.dumps([csl_obj], ensure_ascii=False)

    if citation:
        values['citation'] = citation


# ---------------------------------------------------------------------------
# Post-processing functions (infer_* chain)
# ---------------------------------------------------------------------------

def _infer_clean_metadata_reference_date(values):
    dates = []
    for date in values['metadata-reference-date']:
        try:
            date['value'] = _iso_date_time_to_utc(date['value'])[:10]
        except Exception as e:
            log.warning('Problem converting metadata-reference-date to UTC: %s', e)
            continue
        dates.append(date)
    if dates:
        dates.sort(key=lambda x: x['value'])
        values['metadata-reference-date'] = dates


def _infer_clean_dataset_reference_date(values):
    dates = []
    for date in values['dataset-reference-date']:
        try:
            date['value'] = _iso_date_time_to_utc(date['value'])[:10]
        except Exception:
            date['value'] = date['value'][:10]
            log.warning('Problem converting dataset-reference-date to UTC. Defaulting to %s', date['value'])
        dates.append(date)
    if dates:
        dates.sort(key=lambda x: x['value'])
        values['dataset-reference-date'] = dates


def _infer_date_released(values):
    value = ''
    for date in values['dataset-reference-date']:
        if date['type'] == 'publication':
            value = date['value']
            break
    values['dataset-released'] = value


def _infer_date_updated(values):
    dates = [d['value'] for d in values['dataset-reference-date'] if d['type'] == 'revision']
    if dates:
        dates.sort(reverse=True)
    values['dataset-updated'] = dates[0] if dates else ''


def _infer_date_created(values):
    value = ''
    for date in values['dataset-reference-date']:
        if date['type'] == 'creation':
            value = date['value']
            break
    values['dataset-created'] = value


def _infer_metadata_date(values):
    dates = values.get('metadata-date', [])
    if len(dates):
        dates.sort(reverse=False)
        oldest = dates[0]
        dates.sort(reverse=True)
        values['metadata-date'] = newest = dates[0]
        if not values.get('metadata-reference-date'):
            values['metadata-reference-date'] = [
                {"type": "Creation", "value": oldest[:10]},
                {"type": "Revision", "value": newest[:10]},
            ]


def _infer_url(values):
    value = ''
    for locator in values['resource-locator']:
        if locator['function'] == 'information':
            value = locator['url']
            break
    values['url'] = value


def _infer_tags(values):
    """Build tags from keyword-inspire-theme and keyword-controlled-other.

    For ISO 19115-3 both lists are always empty; _infer_tags_from_keywords
    repopulates tags from the processed keywords list afterwards.
    """
    tags = []
    for key in ['keyword-inspire-theme', 'keyword-controlled-other']:
        for item in values[key]:
            if item not in tags:
                tags.append(item)
    values['tags'] = tags


def _infer_publisher(values):
    value = ''
    for party in values['responsible-organisation']:
        if party.get('role') == 'publisher':
            value = party.get('organisation-name', '')
        if value:
            break
    values['publisher'] = value


def _infer_contact(values):
    value = ''
    for party in values['responsible-organisation']:
        value = party.get('organisation-name', '')
        if value:
            break
    values['contact'] = value


def _infer_contact_email(values):
    value = ''
    for party in values['responsible-organisation']:
        if (isinstance(party, dict) and
                isinstance(party.get('contact-info'), dict) and
                'email' in party['contact-info']):
            value = party['contact-info']['email']
            if value:
                break
    values['contact-email'] = value


def _infer_spatial(values):
    geom = None
    for xml_geom in values.get('spatial', []):
        try:
            xml_geom = xml_geom.decode()
        except (UnicodeDecodeError, AttributeError):
            pass
        if isinstance(xml_geom, list):
            for n, x in enumerate(xml_geom):
                try:
                    xml_geom[n] = x.decode()
                except (UnicodeDecodeError, AttributeError):
                    pass
            if len(xml_geom) == 1:
                xml_geom = xml_geom[0]
        try:
            geom = shapely_shape(json.loads(xml_geom))
        except Exception:
            try:
                geom = shapely_wkt.loads(xml_geom)
            except Exception:
                geom = _parse_gml_to_shapely(xml_geom)
                if geom is None:
                    log.warning('Spatial element is not GeoJSON, WKT, or GML — skipping.')
                    continue
    if geom:
        values['spatial'] = json.dumps(shapely_mapping(geom))
        if not values.get('bbox'):
            bounds = geom.bounds  # (minx, miny, maxx, maxy)
            extent = (bounds[0], bounds[2], bounds[1], bounds[3])
            if extent:
                values['bbox'].append({'west': '', 'east': '', 'north': '', 'south': ''})
                (values['bbox'][0]['west'], values['bbox'][0]['east'],
                 values['bbox'][0]['north'], values['bbox'][0]['south']) = extent


def _infer_metadata_language(values):
    if values.get('metadata-language'):
        values['metadata-language'] = values['metadata-language'][:2].lower()


def _infer_keywords(values):
    keywords = values['keywords']
    default_lang = _clean_lang_key(values.get('metadata-language', 'en'))
    result = []
    if isinstance(keywords, list):
        for kgroup in keywords:
            ktype = kgroup.get('type')
            for item in kgroup.get('keywords', []):
                lang_dict = _local_to_dict(item, default_lang)
                if lang_dict:
                    result.append({'keyword': json.dumps(lang_dict), 'type': ktype})
    else:
        for item in keywords:
            lang_dict = _local_to_dict(item, default_lang)
            if lang_dict:
                result.append({'keyword': json.dumps(lang_dict), 'type': item.get('type')})
    values['keywords'] = result


def _infer_multilingual(values):
    """Convert top-level {default, local} dicts to JSON lang-dict strings."""
    for key in values:
        value = values[key]
        if (isinstance(value, dict) and
                (('default' in value and 'local' in value and len(value) == 2) or
                 ('default' in value and len(value) == 1))):
            default_lang = _clean_lang_key(values.get('metadata-language', 'en'))
            lang_dict = _local_to_dict(value, default_lang)
            values[key] = json.dumps(lang_dict)


def _infer_temporal_vertical_extent(values):
    """Merged base + ISO 19115-3 logic for temporal and vertical extents.

    Base class collapses the temporal-extent list into a min/max dict.
    ISO 19115-3 override wraps it back into a list for repeating_subfields,
    and restores the vertical-extent list (bypassing the base's numeric check).
    """
    # Save raw vertical-extent before the base logic discards string values
    ve_raw = list(values.get('vertical-extent', []))

    # Base class: collapse temporal-extent list → single dict
    te_list = values.get('temporal-extent', [])
    te_dict = {}
    if te_list:
        blist = [x.get('begin', '') for x in te_list]
        elist = [x.get('end', '') for x in te_list]
        try:
            te_dict['begin'] = _iso_date_time_to_utc(min(blist))[:10]
            if max(elist):
                te_dict['end'] = _iso_date_time_to_utc(max(elist))[:10]
        except Exception:
            te_dict['begin'] = (min(blist) or '')[:10]
            if max(elist):
                te_dict['end'] = max(elist)[:10]
            log.warning('Problem converting temporal-extent dates to UTC format. '
                        'Defaulting to %s and %s',
                        te_dict.get('begin', ''), te_dict.get('end', ''))
        values['temporal-extent'] = te_dict

    # ISO 19115-3 override: wrap dict back into list; set flat begin/end keys
    te = values.get('temporal-extent', {})
    if isinstance(te, dict) and te:
        values['temporal-extent-begin'] = [te['begin']] if te.get('begin') else []
        values['temporal-extent-end'] = [te.get('end')] if te.get('end') else []
        values['temporal-extent'] = [te]
    else:
        values['temporal-extent-begin'] = []
        values['temporal-extent-end'] = []

    # Restore vertical-extent as a list for repeating_subfields schema
    if ve_raw:
        valid = [item for item in ve_raw
                 if item.get('min') is not None or item.get('max') is not None]
        if valid:
            values['vertical-extent'] = valid
        else:
            values.pop('vertical-extent', None)
    else:
        values.pop('vertical-extent', None)


def _drop_empty(values):
    """Remove empty dicts/lists, preserving keys required by ckanext-spatial."""
    to_drop = [k for k, v in values.items()
               if k not in _SPATIAL_REQUIRED_KEYS and v in ({}, [])]
    for k in to_drop:
        del values[k]


def _infer_aggregation_info(values):
    """Convert aggregate-dataset-name to a {lang: text} dict."""
    default_lang = (values.get('metadata-language') or 'en')[:2]
    for item in values.get('aggregation-info', []):
        name = item.get('aggregate-dataset-name')
        if isinstance(name, dict) and 'default' in name:
            # _localised_raw format: convert to {lang: text}
            item['aggregate-dataset-name'] = _local_to_dict(name, default_lang)
        elif isinstance(name, str):
            item['aggregate-dataset-name'] = {default_lang: name}


def _infer_responsible_party_contacts(values):
    """Merge duplicate contacts that differ only in role (ISO 19115-3 pattern)."""
    for field in ('metadata-point-of-contact', 'cited-responsible-party', 'distributor'):
        contacts = values.get(field)
        if not isinstance(contacts, list):
            continue
        merged = []
        seen = {}
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
        values[field] = merged


def _infer_normalize_contact_roles(values):
    """Ensure role is always a list in CIOOS schema contact fields.

    Excludes ``responsible-organisation`` whose role must remain a plain string
    so the parent SpatialHarvester can correctly group and merge it into the
    ``responsible-party`` extras entry (it appends ``party['role']`` directly
    into a list — wrapping it in a list first produces list-of-lists).
    """
    for field in (
        'metadata-point-of-contact',
        'cited-responsible-party',
        'distributor',
        'author',
    ):
        for contact in values.get(field) or []:
            if not isinstance(contact, dict):
                continue
            role = contact.get('role')
            if isinstance(role, list):
                contact['role'] = sorted(role)
            elif role:
                contact['role'] = [role]
            else:
                contact['role'] = []


# Keyword types that have no dedicated CKAN schema field — their keywords flow
# through as regular tags via _infer_tags_from_keywords without a warning.
_TAGS_ONLY_KEYWORD_TYPES = frozenset({
    'taxa',
    'Government of Canada Core Subject Thesaurus',
})


def _infer_keyword_types(values):
    """Split keywords by type → keyword-project, keyword-datacentre, projects."""
    keywords = values.get('keywords', [])
    default_lang = (values.get('metadata-language') or 'en')[:2]
    projects = []
    eovs = []
    datacentres = []
    for kw in keywords:
        ktype = kw.get('type') or ''
        keyword_json = kw.get('keyword', '')
        if not keyword_json:
            continue
        try:
            lang_dict = json.loads(keyword_json)
            value = (lang_dict.get(default_lang) or next(iter(lang_dict.values()), '')
                     if isinstance(lang_dict, dict) else str(lang_dict))
        except (ValueError, TypeError):
            value = keyword_json
        if not value or ktype == 'default':
            continue
        if ktype == 'project' and value not in projects:
            projects.append(value)
        elif ktype == 'datacentre' and value not in datacentres:
            datacentres.append(value)
        elif ktype == "eov":
            # EOV codes are always English identifiers — resolve 'en' key only;
            # French-text-only keyword elements are intentionally skipped.
            try:
                parsed = json.loads(keyword_json)
                eov_value = parsed.get('en') if isinstance(parsed, dict) else value
            except (ValueError, TypeError):
                eov_value = value
            if eov_value and eov_value not in eovs:
                eovs.append(eov_value)
        elif ktype in _TAGS_ONLY_KEYWORD_TYPES:
            # Known thesaurus types with no dedicated schema field — keywords
            # are already captured as regular tags by _infer_tags_from_keywords.
            pass
        elif ktype:
            log.warning('Unknown keyword type "%s" for keyword "%s". Skipping.', ktype, value)

    if projects:
        values['keyword-project'] = projects
        values['projects'] = projects
    if datacentres:
        values['keyword-datacentre'] = datacentres
    if eovs:
        values['eov'] = eovs


def _infer_tags_from_keywords(values):
    """Populate tags from the processed keywords list (JSON lang-dict strings)."""
    tags = []
    for kw in values.get('keywords', []):
        keyword_json = kw.get('keyword', '')
        if keyword_json and keyword_json not in tags:
            tags.append(keyword_json)
    values['tags'] = tags


def _infer_resource_locator_multilingual(values):
    """Convert resource-locator name/description {default,local} dicts to JSON strings."""
    default_lang = _clean_lang_key(values.get('metadata-language', 'en'))
    for locator in values.get('resource-locator', []):
        for field in ('name', 'description'):
            val = locator.get(field)
            if isinstance(val, dict) and 'default' in val:
                lang_dict = _local_to_dict(val, default_lang)
                locator[field] = json.dumps(lang_dict, ensure_ascii=False) if lang_dict else ''

def _infer_guid(values):
    """Convert guid dict to a string with authority + code"""
    guid = values.get('guid')
    if isinstance(guid, dict):
        authority = guid.get('authority', '')
        code = guid.get('code', '')
        if authority and code:
            values['guid'] = f"{authority}_{code}"
        elif code:
            values['guid'] = code
        else:
            values['guid'] = ''


def _guess_format_from_url(url):
    """Return a CIOOS format string for *url*, or None if unknown.

    Matches the same patterns as the old waf_cioos.py post-processing so that
    resource format labels are consistent across all harvested records.
    """
    url_lower = url.lower()
    url_path = url_lower.split('?')[0].split('#')[0]
    url_extension = url_path.rsplit('.', 1)[-1] if '.' in url_path else ''

    if url_extension in ('csv', 'json', 'xml', 'nc', 'zip'):
        return url_extension.upper() if url_extension == 'csv' else url_extension
    if 'erddap' in url_lower:
        return 'ERDDAP'
    if 'thredds' in url_lower:
        return 'THREDDS'
    if 'obis' in url_lower:
        return 'OBIS'
    if 'gbif' in url_lower:
        return 'GBIF'
    if url_extension in ('html', 'htm') or url_lower.startswith('http'):
        return 'HTML'
    return None


def _infer_resource_types(values):
    """Annotate each resource-locator entry with a CIOOS format label.

    Iterates ``values['resource-locator']`` (populated during parsing) and
    sets a ``'format'`` key on each entry so that waf.py can apply it after
    the parent class builds the package resources.
    """
    for locator in values.get('resource-locator', []):
        url = locator.get('url', '')
        if url:
            fmt = _guess_format_from_url(url)
            if fmt:
                locator['format'] = fmt

# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def _parse_document(root):
    """Extract all iso_values from an mdb:MD_Metadata lxml element.

    Returns a flat dict matching the keys expected by WAFHarvesterISO19115_3
    and plugin.py's get_package_dict() override.
    """
    values = {}

    # --- Metadata-level fields ---
    values['metadata-language'] = _first_text(root,
        "mdb:defaultLocale/lan:PT_Locale/lan:language/lan:LanguageCode/@codeListValue",
        "mdb:defaultLocale/lan:PT_Locale/lan:language/lan:LanguageCode/text()",
    )
    values['metadata-standard-name'] = ''
    values['metadata-standard-version'] = ''
    values['resource-type'] = _first_texts(root,
        "mdb:metadataScope/mdb:MD_MetadataScope/mdb:resourceScope/mcc:MD_ScopeCode/@codeListValue",
        "mdb:metadataScope/mdb:MD_MetadataScope/mdb:resourceScope/mcc:MD_ScopeCode/text()",
    )
    values['metadata-point-of-contact'] = [
        _parse_responsible_party_cioos(el) for el in _first_x(root,
            "mdb:contact/cit:CI_Responsibility",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='pointOfContact']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='pointOfContact']",
            _ID + "/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:contact/cit:CI_Responsibility",
        )
    ]
    values['cited-responsible-party'] = [
        _parse_responsible_party_cioos(el) for el in _x(root,
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility")
    ]
    values['metadata-reference-date'] = [
        _parse_reference_date(el) for el in _x(root, "mdb:dateInfo/cit:CI_Date")
    ]
    # Union XPath in one string — returns all Date and DateTime values together
    values['metadata-date'] = _texts(root,
        "mdb:dateInfo/cit:CI_Date/cit:date/gco:Date/text() | "
        "mdb:dateInfo/cit:CI_Date/cit:date/gco:DateTime/text()")
    values['spatial-reference-system'] = ''

    # --- Identification info ---
    title_els = _x(root, _ID + "/mri:citation/cit:CI_Citation/cit:title")
    values['title'] = _localised_raw(title_els[0]) if title_els else {'default': '', 'local': []}
    values['alternate-title'] = []
    values['dataset-reference-date'] = [
        _parse_reference_date(el) for el in _x(root,
            _ID + "/mri:citation/cit:CI_Citation/cit:date/cit:CI_Date")
    ]
    values['unique-resource-identifier'] = _text(root,
        _ID + "/mri:citation/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()")
    guid_els = _x(root, "mdb:metadataIdentifier/mcc:MD_Identifier")
    values['guid'] = _parse_identifier(guid_els[0]) if guid_els else ''
    values['unique-resource-identifier-full'] = [
        _parse_identifier(el) for el in _x(root,
            _ID + "/mri:citation/cit:CI_Citation/cit:identifier/mcc:MD_Identifier")
    ]
    values['presentation-form'] = []

    abstract_els = _x(root, _ID + "/mri:abstract")
    values['abstract'] = _localised_raw(abstract_els[0]) if abstract_els else {'default': '', 'local': []}
    values['purpose'] = ''

    # Extract edition / edition-date from the resource CI_Citation as transient
    # private keys.  _infer_citation() (called later in the post-processing
    # chain) pops these and embeds them into the CSL-JSON citation object.
    _cit_els = _x(root, _ID + "/mri:citation/cit:CI_Citation")
    if _cit_els:
        _cit = _cit_els[0]
        values['_cit_edition'] = _first_text(_cit,
            "cit:edition/gco:CharacterString/text()",
        ) or ''
        values['_cit_edition_date'] = _first_text(_cit,
            "cit:editionDate/gco:Date/text()",
            "cit:editionDate/gco:DateTime/text()",
        ) or ''
    else:
        values['_cit_edition'] = ''
        values['_cit_edition_date'] = ''

    values['responsible-organisation'] = [
        _parse_responsible_party(el) for el in _first_x(root,
            "mdb:contact/cit:CI_Responsibility[cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='pointOfContact' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='originator' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='owner' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='rightsHolder' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='pointOfContact' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='originator' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='owner' and cit:party/cit:CI_Organisation]",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='rightsHolder' and cit:party/cit:CI_Organisation]",
        )
    ]
    values['frequency-of-update'] = _first_text(root,
        _ID + "/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceAndUpdateFrequency/mmi:MD_MaintenanceFrequencyCode/@codeListValue",
        _ID + "/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceAndUpdateFrequency/mmi:MD_MaintenanceFrequencyCode/text()",
    )
    values['maintenance-note'] = _text(root,
        _ID + "/mri:resourceMaintenance/mmi:MD_MaintenanceInformation/mmi:maintenanceNote/gco:CharacterString/text()")
    values['progress'] = _first_texts(root,
        _ID + "/mri:status/mcc:MD_ProgressCode/@codeListValue",
        _ID + "/mri:status/mcc:MD_ProgressCode/text()",
    )
    values['keywords'] = [
        _parse_keyword_group(el) for el in _x(root,
            _ID + "/mri:descriptiveKeywords/mri:MD_Keywords")
    ]
    values['keyword-inspire-theme'] = []
    values['keyword-controlled-other'] = []
    values['usage'] = []

    # --- Constraints ---
    values['limitations-on-public-access'] = _first_texts(root,
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:otherConstraints/gco:CharacterString/text()",
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:otherConstraints/gcx:Anchor/text()",
    )
    values['access-constraints'] = _first_texts(root,
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:accessConstraints/mco:MD_RestrictionCode/@codeListValue",
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:accessConstraints/mco:MD_RestrictionCode/text()",
    )
    values['use-constraints'] = _first_text(root,
        _ID + "/mri:resourceConstraints/mco:MD_Constraints/mco:useLimitation/gco:CharacterString/text()",
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useLimitation/gco:CharacterString/text()",
    )
    values['use-constraints-code'] = _first_text(root,
        _ID + "/mri:resourceConstraints/mco:MD_Constraints/mco:useConstraints/mco:MD_RestrictionCode/@codeListValue",
        _ID + "/mri:resourceConstraints/mco:MD_Constraints/mco:useConstraints/mco:MD_RestrictionCode/text()",
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useConstraints/mco:MD_RestrictionCode/@codeListValue",
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:useConstraints/mco:MD_RestrictionCode/text()",
    )
    values['legal-constraints-reference-code'] = _text(root,
        _ID + "/mri:resourceConstraints/mco:MD_LegalConstraints/mco:reference/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()")

    # --- Aggregation / associations ---
    values['aggregation-info'] = [
        _parse_aggregation_info(el) for el in _x(root,
            _ID + "/mri:associatedResource/mri:MD_AssociatedResource")
    ]

    # --- Spatial / extent fields ---
    values['spatial-data-service-type'] = ''
    values['spatial-resolution'] = ''
    values['spatial-resolution-units'] = ''
    values['equivalent-scale'] = []
    values['dataset-language'] = []
    values['topic-category'] = _texts(root,
        _ID + "/mri:topicCategory/mri:MD_TopicCategoryCode/text()")
    values['extent-controlled'] = []
    values['extent-free-text'] = []
    values['bbox'] = [
        _parse_bbox(el) for el in _x(root,
            _ID + "/mri:extent/gex:EX_Extent/gex:geographicElement/gex:EX_GeographicBoundingBox")
    ]
    spatial_nodes = _x(root,
        _ID + "/mri:extent/gex:EX_Extent/gex:geographicElement/gex:EX_BoundingPolygon/gex:polygon/*")
    values['spatial'] = [
        etree.tostring(node) if not isinstance(node, str) else node
        for node in spatial_nodes
    ]
    values['temporal-extent'] = [
        _parse_temporal_extent(el) for el in _first_x(root,
            _ID + "/mri:extent/gex:EX_Extent/gex:temporalElement/gex:EX_TemporalExtent/gex:extent/gml32:TimePeriod",
            _ID + "/mri:extent/gex:EX_Extent/gex:temporalElement/gex:EX_TemporalExtent/gex:extent/gml:TimePeriod",
        )
    ]
    values['vertical-extent'] = [
        _parse_vertical_extent(el) for el in _x(root,
            _ID + "/mri:extent/gex:EX_Extent/gex:verticalElement/gex:EX_VerticalExtent")
    ]
    values['vertical-extent-crs'] = _texts(root,
        _ID + "/mri:extent/gex:EX_Extent/gex:verticalElement/gex:EX_VerticalExtent/gex:verticalCRSId/mrs:MD_ReferenceSystem/mrs:referenceSystemIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()")

    # --- Distribution ---
    values['coupled-resource'] = []
    values['additional-information-source'] = ''
    values['data-format'] = [
        _parse_data_format(el) for el in _x(root,
            "mdb:distributionInfo/mrd:MD_Distribution/mrd:distributionFormat/mrd:MD_Format/mrd:formatSpecificationCitation/cit:CI_Citation/cit:title")
    ]
    values['distributor'] = [
        _parse_responsible_party_cioos(el) for el in _x(root,
            "mdb:distributionInfo/mrd:MD_Distribution/mrd:distributor/mrd:MD_Distributor/mrd:distributorContact/cit:CI_Responsibility")
    ]
    # Union XPath in one string — collects from both transferOptions and distributor paths
    values['resource-locator'] = [
        _parse_resource_locator(el) for el in _x(root,
            "mdb:distributionInfo/mrd:MD_Distribution/mrd:transferOptions/mrd:MD_DigitalTransferOptions/mrd:onLine/cit:CI_OnlineResource | "
            "mdb:distributionInfo/mrd:MD_Distribution/mrd:distributor/mrd:MD_Distributor/mrd:distributorTransferOptions/mrd:MD_DigitalTransferOptions/mrd:onLine/cit:CI_OnlineResource")
    ]
    values['resource-locator-identification'] = []

    # --- Quality / conformity ---
    values['conformity-specification'] = ''
    values['conformity-pass'] = ''
    values['conformity-explanation'] = ''

    # lineage: list of LI_Lineage dicts.  In ISO 19115-3, lineage lives at
    # mdb:resourceLineage (a top-level sibling of mdb:identificationInfo),
    # not inside mdb:dataQualityInfo as in ISO 19139.
    _lineage_lang = _clean_lang_key(values.get('metadata-language', 'en') or 'en')
    values['lineage'] = [
        _parse_lineage(el, _lineage_lang)
        for el in _x(root, "mdb:resourceLineage/mrl:LI_Lineage")
    ]

    # --- Browse graphic / author ---
    values['browse-graphic'] = [
        _parse_browse_graphic(el) for el in _x(root,
            _ID + "/mri:graphicOverview/mcc:MD_BrowseGraphic")
    ]
    values['author'] = [
        _parse_responsible_party(el) for el in _first_x(root,
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='author']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='originator']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/@codeListValue ='owner']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='author']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='originator']",
            _ID + "/mri:citation/cit:CI_Citation/cit:citedResponsibleParty/cit:CI_Responsibility[cit:role/cit:CI_RoleCode/text() ='owner']",
        )
    ]

    # --- Post-processing (infer_* chain, same order as original) ---
    _infer_clean_metadata_reference_date(values)
    _infer_clean_dataset_reference_date(values)
    _infer_date_released(values)
    _infer_date_updated(values)
    _infer_date_created(values)
    _infer_metadata_date(values)
    _infer_url(values)
    _infer_tags(values)
    _infer_publisher(values)
    _infer_contact(values)
    _infer_contact_email(values)
    _infer_spatial(values)
    _infer_metadata_language(values)
    _infer_keywords(values)
    _infer_multilingual(values)
    _infer_temporal_vertical_extent(values)
    _infer_guid(values)
    _infer_citation(values)
    _infer_resource_types(values)
    _drop_empty(values)

    # ISO 19115-3 specific post-processing
    _infer_aggregation_info(values)
    _infer_responsible_party_contacts(values)
    _infer_normalize_contact_roles(values)
    _infer_keyword_types(values)
    _infer_tags_from_keywords(values)
    _infer_resource_locator_multilingual(values)

    return values


# ---------------------------------------------------------------------------
# Thin wrapper class for ckanext-spatial monkey-patch compatibility
# ---------------------------------------------------------------------------

class ISODocument:
    """Thin wrapper around _parse_document() for ckanext-spatial compatibility.

    ckanext-spatial's import_stage calls:
        doc = ISODocument(xml_str)
        values = doc.read_values()

    WAFHarvesterISO19115_3 monkey-patches spatial_base.ISODocument with this
    class inside a try/finally block so the original is always restored.
    """

    def __init__(self, xml_str):
        if isinstance(xml_str, str):
            xml_str = xml_str.encode('utf-8')
        self.xml_str = xml_str
        parser = etree.XMLParser(remove_blank_text=True)
        self.root = etree.fromstring(xml_str, parser=parser)
        self.xml_tree = self.root  # ckanext-spatial base.py accesses iso_parser.xml_tree

    def read_values(self):
        return _parse_document(self.root)

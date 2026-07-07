"""Duplicate-detection helpers for the OBIS harvester.

Ported from the standalone analysis script ``find_obis_cioos_duplicates.py``
(originally in the ckan-harvester analysis workspace).  The OBIS harvester uses
these helpers in its gather stage to skip datasets that already exist in the
target CKAN catalogue — matched by **DOI** or by **OBIS dataset UUID** — and to
report them rather than re-importing (see issue cioos-ckan-docker-base#20).

The functions here are deliberately free of CKAN imports (except the lazily
imported ``build_catalogue_index`` helper) so they can be unit-tested against
cached fixtures without a running CKAN.
"""

import re

# DOI URL/identifier prefixes that should be stripped to get a bare DOI.
DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
)

# Matches an OBIS dataset link anywhere in a blob of text/JSON.
_OBIS_UUID_RE = re.compile(r"obis\.org/dataset/([a-f0-9-]{36})", re.IGNORECASE)


def normalize_doi(raw):
    """Extract a bare, lower-cased DOI (e.g. ``10.xxxx/yyyy``) from a URL or string.

    Returns ``None`` when the input is empty or does not look like a DOI.
    """
    if not raw:
        return None
    s = str(raw).strip().lower()
    for prefix in DOI_PREFIXES:
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    if s.startswith("10."):
        return s
    return None


def obis_dataset_dois(dataset):
    """All candidate normalized DOIs for an OBIS ``/v3/dataset`` record.

    OBIS exposes the citation DOI under ``doi`` and/or ``citation_id`` (the
    latter is frequently populated when ``doi`` is null), so both are checked.
    """
    out = set()
    for key in ("doi", "citation_id"):
        doi = normalize_doi(dataset.get(key))
        if doi:
            out.add(doi)
    return out


def package_dois(pkg):
    """Normalized DOIs found in a CKAN package's ``unique-resource-identifier-full``."""
    out = set()
    for uri in (pkg.get("unique-resource-identifier-full") or []):
        if isinstance(uri, dict):
            doi = normalize_doi(uri.get("code", ""))
            if doi:
                out.add(doi)
    return out


def package_obis_uuids(pkg):
    """OBIS dataset UUIDs referenced anywhere in a CKAN package.

    Scans ``harvest_document_content``, resource URLs and unique-resource
    identifier codes for ``obis.org/dataset/<uuid>`` links — this is how a
    record harvested from another source (SLGO, DFO, an ERDDAP/IPT feed, ...)
    advertises that it originates from a given OBIS dataset.
    """
    uuids = set()
    hdc = pkg.get("harvest_document_content") or ""
    for m in _OBIS_UUID_RE.finditer(hdc):
        uuids.add(m.group(1).lower())
    for res in (pkg.get("resources") or []):
        for m in _OBIS_UUID_RE.finditer(res.get("url") or ""):
            uuids.add(m.group(1).lower())
    for uri in (pkg.get("unique-resource-identifier-full") or []):
        if isinstance(uri, dict):
            for m in _OBIS_UUID_RE.finditer(uri.get("code", "") or ""):
                uuids.add(m.group(1).lower())
    return uuids


class CatalogueIndex:
    """Lookup of DOIs and OBIS UUIDs already present in a target CKAN catalogue."""

    def __init__(self):
        self.dois = {}        # normalized DOI -> package name/id
        self.obis_uuids = {}  # OBIS uuid (lower-case) -> package name/id

    def add_package(self, pkg):
        ref = pkg.get("name") or pkg.get("id")
        for doi in package_dois(pkg):
            self.dois.setdefault(doi, ref)
        for uuid in package_obis_uuids(pkg):
            self.obis_uuids.setdefault(uuid, ref)

    def match(self, obis_uuid, dois):
        """Return ``(matched_package_ref, method)`` or ``(None, None)``.

        UUID matches take precedence over DOI matches. ``method`` is one of
        ``"uuid"`` / ``"doi"``.
        """
        uuid = (obis_uuid or "").lower()
        if uuid in self.obis_uuids:
            return self.obis_uuids[uuid], "uuid"
        for doi in dois:
            if doi in self.dois:
                return self.dois[doi], "doi"
        return None, None

    def __len__(self):
        return len(self.dois) + len(self.obis_uuids)


def classify_datasets(source, db_modified, index=None, max_delete_fraction=0.2):
    """Pure new/change/delete/skip classification for the OBIS gather stage.

    Args:
        source:      ``{obis_uuid: dataset_dict}`` discovered from the OBIS API.
        db_modified: ``{guid: obis_modified_date}`` for the harvest objects
                     already current for this source.
        index:       optional :class:`CatalogueIndex`; when given, datasets that
                     are new to this source but already present in the catalogue
                     (by DOI or OBIS UUID) are moved from ``new`` to ``skipped``.
        max_delete_fraction:
                     delete-safety guard. If the proposed deletes exceed this
                     fraction of the datasets already held for the source, the
                     deletes are suppressed for this run (see ``delete_suppressed``
                     in the result). Protects against a truncated / transient
                     short OBIS response wiping a large slice of the catalogue.

    Returns a dict with keys ``new`` (set), ``change`` (list), ``delete`` (set),
    ``skipped`` (list of ``(uuid, matched_ref, method)``), ``delete_suppressed``
    (bool) and ``delete_candidates`` (int — deletes proposed before the guard).
    """
    source_guids = set(source.keys())
    existing_guids = set(db_modified.keys())

    new = source_guids - existing_guids
    delete = existing_guids - source_guids

    change = []
    for guid in source_guids & existing_guids:
        ds = source[guid]
        new_mod = ds.get("updated") or ds.get("published") or ""
        old_mod = db_modified.get(guid) or ""
        # Re-import only when OBIS advertises a (newer) date. A date-less record
        # is treated as unchanged rather than re-fetched on every harvest.
        if new_mod and (not old_mod or new_mod > old_mod):
            change.append(guid)

    # Delete-safety guard: a truncated or transient short OBIS response would
    # otherwise mark large numbers of still-valid datasets for deletion. If the
    # proposed deletes exceed max_delete_fraction of what we already hold,
    # suppress them for this run (the caller logs a loud warning).
    delete_candidates = len(delete)
    delete_suppressed = False
    if existing_guids and delete and (
        len(delete) / len(existing_guids) > max_delete_fraction
    ):
        delete_suppressed = True
        delete = set()

    skipped = []
    if index is not None and new:
        surviving = set()
        for guid in new:
            matched_ref, method = index.match(guid, obis_dataset_dois(source[guid]))
            if matched_ref:
                skipped.append((guid, matched_ref, method))
            else:
                surviving.add(guid)
        new = surviving

    return {
        "new": new,
        "change": change,
        "delete": delete,
        "skipped": skipped,
        "delete_suppressed": delete_suppressed,
        "delete_candidates": delete_candidates,
    }


def build_catalogue_index(get_action=None, context=None, exclude_source_id=None, rows=1000):
    """Build a :class:`CatalogueIndex` by paging ``package_search`` over the target CKAN.

    ``get_action``/``context`` default to CKAN's; tests pass a stub ``get_action``
    so the helper can run without a live CKAN.  ``exclude_source_id`` omits the
    OBIS harvester's own datasets so a re-harvest is never mistaken for a
    cross-source duplicate.
    """
    if get_action is None:
        from ckan.plugins import toolkit
        get_action = toolkit.get_action
    if context is None:
        from ckan import model
        context = {"model": model, "session": model.Session, "ignore_auth": True}

    index = CatalogueIndex()
    fq = f'-harvest_source_id:"{exclude_source_id}"' if exclude_source_id else "*:*"
    start = 0
    while True:
        result = get_action("package_search")(
            context, {"fq": fq, "rows": rows, "start": start}
        )
        results = result.get("results", [])
        if not results:
            break
        for pkg in results:
            index.add_package(pkg)
        start += rows
        if start >= result.get("count", 0):
            break
    return index

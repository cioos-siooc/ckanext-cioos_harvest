"""OBIS harvester for CIOOS.

Harvests dataset metadata from OBIS (Ocean Biodiversity Information System,
https://obis.org) into CKAN.  See issue cioos-ckan-docker-base#20.

Design
------
The heavy lifting of turning an OBIS dataset into a CIOOS metadata record is
done by the ``cioos-metadata-conversion`` package; this harvester only owns
*discovery* and *deduplication*, because that package converts one dataset at a
time and has no enumeration or spatial filtering of its own.

Conversion route (chosen in #20): OBIS -> ``cioos-metadata-conversion`` ->
ISO 19115-3 XML -> the existing :class:`WAFHarvesterISO19115_3` import pipeline.
We therefore subclass that harvester and override only ``gather_stage`` and
``fetch_stage``; ``import_stage`` and the ISO19115-3 -> package mapping in
``get_package_dict`` are inherited.

Stages
------
``gather_stage``
    Query ``GET https://api.obis.org/v3/dataset`` (optionally filtered to a
    region with ``?geometry=<WKT polygon>``), diff against the harvest objects
    already produced for this source (new / change / delete), and — for
    datasets new to this source — skip+report any that already exist in the
    target catalogue by DOI or OBIS UUID.  One ``HarvestObject`` per surviving
    dataset, ``guid`` = the OBIS UUID.

``fetch_stage``
    For each object, run ``Record(obis_id).load().convert_to_cioos_schema()``
    then ``convert_to("iso19115-3_xml")`` and store the XML in
    ``harvest_object.content``.

``get_package_dict``
    Force ``iso_values['guid']`` to the OBIS UUID (the converter leaves the
    metadata identifier empty, which would otherwise abort package-name
    generation) and apply the optional, env-gated translation hook.

Source configuration (JSON)
---------------------------
``spatial_filter``        Inline WKT ``POLYGON``/``MULTIPOLYGON`` restricting the
                          harvest to a region (server-side OBIS ``geometry=``).
``spatial_filter_file``   Path to a file containing the same WKT (takes
                          precedence over ``spatial_filter``).
``nodeid`` / ``instituteid`` / ``startdate`` / ``enddate``
                          Optional pass-through OBIS ``/v3/dataset`` filters.
``page_size``             OBIS page size (default 1000).
``gather_timeout``        Read timeout (s) for the dataset-list discovery request
                          (default 300). Raise it when harvesting all of OBIS —
                          the un-paged /v3/dataset list of the full catalogue is
                          large and slow.
``request_delay_seconds`` Delay between per-dataset fetch calls (default 0.5) to
                          stay polite to the OBIS API.
``fetch_retries``         Retry attempts for transient network errors during
                          fetch (default 3).
``max_delete_fraction``   Delete-safety guard (default 0.2): if a harvest would
                          delete more than this fraction of the datasets already
                          held, the deletes are suppressed and a warning logged.
Omitting every filter harvests *all* OBIS datasets.

Operational note: because ``fetch_stage`` makes live OBIS calls per dataset,
this source MUST be run with a **single fetch worker** — parallel workers would
hit the public OBIS API with uncoordinated bursts.
"""

import json
import logging
import os
import re
import time
from datetime import datetime

import requests

from ckan import model
from ckan.plugins.core import SingletonPlugin, implements

from ckanext.cioos_harvest.harvesters.base import get_object_extra, singleton_new
from ckanext.cioos_harvest.harvesters.obis_dedup import (
    build_catalogue_index,
    classify_datasets,
    normalize_doi,
    obis_dataset_dois,
)
from ckanext.cioos_harvest.harvesters.waf import WAFHarvesterISO19115_3
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

log = logging.getLogger(__name__)

OBIS_DATASET_API = "https://api.obis.org/v3/dataset"
# Public OBIS dataset page and per-dataset open-data export (used for link-back).
OBIS_DATASET_PAGE = "https://obis.org/dataset/"
OBIS_PARQUET_URL = "https://obis-open-data.s3.amazonaws.com/occurrence/{uuid}.parquet"

# Identify CIOOS on every OBIS request so OBIS ops can attribute/contact us.
USER_AGENT = (
    "CIOOS-OBIS-Harvester/1.0 "
    "(+https://cioos.ca; ckanext-cioos_harvest; contact: info@cioos.ca)"
)
_HTTP_HEADERS = {"User-Agent": USER_AGENT}

# Default read timeout (seconds) for the OBIS dataset-list discovery request.
# OBIS /v3/dataset has no offset paging and returns full records, so a
# whole-catalogue list can be large and slow; keep this generous and override
# with the `gather_timeout` config key when harvesting all of OBIS.
DEFAULT_GATHER_TIMEOUT = 300

_GATHER_SESSION = None


def _get_gather_session():
    """Shared requests.Session for gather discovery calls (retry + User-Agent)."""
    global _GATHER_SESSION
    if _GATHER_SESSION is None:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.0,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(_HTTP_HEADERS)
        _GATHER_SESSION = session
    return _GATHER_SESSION

# Defaults for the fetch-stage politeness / resilience knobs (overridable in the
# source config). fetch_stage makes live OBIS calls per object, so the source
# MUST run with a single fetch worker; the delay keeps us polite regardless.
DEFAULT_REQUEST_DELAY_SECONDS = 0.5
DEFAULT_FETCH_RETRIES = 3
DEFAULT_MAX_DELETE_FRACTION = 0.2

# Requests exception types we treat as transient (worth retrying) vs. a genuine
# conversion/schema failure (permanent — do not retry).
_TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)

# Matches a leading XML declaration (<?xml … ?>) and any whitespace after it.
_XML_DECL_RE = re.compile(r"^\s*<\?xml[^>]*\?>\s*", re.IGNORECASE)


def _strip_xml_declaration(xml):
    """Remove a leading ``<?xml … ?>`` declaration from an XML string."""
    return _XML_DECL_RE.sub("", xml, count=1)

# Hard ceiling on a single OBIS list response (the /v3/dataset endpoint pages by
# `size` only, with no offset). The global dataset count is ~7k; region polygons
# cut this dramatically. If a harvest ever exceeds this, we log and truncate.
_MAX_OBIS_SIZE = 20000

# Environment variable that turns on the (not-yet-implemented) Cohere EN->FR
# translation of newly harvested records. Off unless explicitly enabled.
TRANSLATION_ENV_VAR = "CKANEXT__CIOOS_OBIS__ENABLE_TRANSLATION"


class OBISHarvester(WAFHarvesterISO19115_3):
    """Harvest OBIS datasets into CKAN via the ISO 19115-3 import pipeline."""

    implements(IHarvester)

    __new__ = singleton_new

    # ------------------------------------------------------------------
    # Harvester info / config
    # ------------------------------------------------------------------

    def info(self):
        return {
            "name": "obis_harvester",
            "title": "OBIS Harvester",
            "description": (
                "Harvests dataset metadata from OBIS (https://obis.org). Optionally "
                "restrict to a region with a WKT polygon (spatial_filter), and skip "
                "datasets already in the catalogue (matched by DOI or OBIS UUID)."
            ),
        }

    # Config keys this harvester understands (for typo detection).
    _KNOWN_CONFIG_KEYS = {
        "spatial_filter",
        "spatial_filter_file",
        "nodeid",
        "instituteid",
        "startdate",
        "enddate",
        "page_size",
        "gather_timeout",
        "request_delay_seconds",
        "fetch_retries",
        "max_delete_fraction",
        # shared cioos_harvest / ckanext-spatial config keys tolerated on the source
        "default_extras",
        "override_extras",
        "clean_tags",
    }

    def validate_config(self, source_config):
        if not source_config:
            return source_config
        try:
            config_obj = json.loads(source_config)
        except ValueError as e:
            raise ValueError("Config must be valid JSON: %s" % e)
        if not isinstance(config_obj, dict):
            raise ValueError("Config must be a JSON object")

        spatial_filter = config_obj.get("spatial_filter")
        if spatial_filter and not str(spatial_filter).upper().startswith(
            ("POLYGON", "MULTIPOLYGON")
        ):
            raise ValueError(
                "spatial_filter must be a WKT POLYGON or MULTIPOLYGON string"
            )

        # A bad spatial_filter_file must fail loudly at config time, not silently
        # at gather (an unreadable file would otherwise crash the gather stage).
        spatial_filter_file = config_obj.get("spatial_filter_file")
        if spatial_filter_file:
            try:
                with open(spatial_filter_file) as f:
                    wkt = f.read().strip()
            except OSError as e:
                raise ValueError(
                    "spatial_filter_file could not be read: %s" % e
                )
            if not wkt.upper().startswith(("POLYGON", "MULTIPOLYGON")):
                raise ValueError(
                    "spatial_filter_file must contain a WKT POLYGON/MULTIPOLYGON"
                )

        page_size = config_obj.get("page_size")
        if page_size is not None and (
            not isinstance(page_size, int) or page_size <= 0
        ):
            raise ValueError("page_size must be a positive integer")
        if isinstance(page_size, int) and page_size > _MAX_OBIS_SIZE:
            raise ValueError(
                "page_size must not exceed %s (the single-response cap)"
                % _MAX_OBIS_SIZE
            )

        for key in ("startdate", "enddate"):
            value = config_obj.get(key)
            if value:
                try:
                    datetime.strptime(str(value), "%Y-%m-%d")
                except ValueError:
                    raise ValueError("%s must be an ISO date (YYYY-MM-DD)" % key)

        gather_timeout = config_obj.get("gather_timeout")
        if gather_timeout is not None and (
            not isinstance(gather_timeout, int) or gather_timeout <= 0
        ):
            raise ValueError("gather_timeout must be a positive integer (seconds)")

        retries = config_obj.get("fetch_retries")
        if retries is not None and (not isinstance(retries, int) or retries < 0):
            raise ValueError("fetch_retries must be a non-negative integer")

        delay = config_obj.get("request_delay_seconds")
        if delay is not None and (
            not isinstance(delay, (int, float)) or delay < 0
        ):
            raise ValueError("request_delay_seconds must be a non-negative number")

        frac = config_obj.get("max_delete_fraction")
        if frac is not None and (
            not isinstance(frac, (int, float)) or not 0 <= frac <= 1
        ):
            raise ValueError("max_delete_fraction must be a number between 0 and 1")

        unknown = set(config_obj) - self._KNOWN_CONFIG_KEYS
        if unknown:
            # A typo like `spatial_fltr` would otherwise be silently ignored and
            # accidentally harvest the whole OBIS catalogue. Warn loudly.
            log.warning(
                "OBIS harvester: ignoring unknown config key(s): %s",
                ", ".join(sorted(unknown)),
            )

        return source_config

    # ------------------------------------------------------------------
    # OBIS discovery
    # ------------------------------------------------------------------

    def _resolve_spatial_filter(self):
        """Return the configured WKT polygon (inline or from file), or None."""
        path = self.source_config.get("spatial_filter_file")
        if path:
            with open(path) as f:
                return f.read().strip()
        return self.source_config.get("spatial_filter")

    def _obis_query_params(self):
        """Build the OBIS /v3/dataset query params from the source config."""
        params = {}
        wkt = self._resolve_spatial_filter()
        if wkt:
            params["geometry"] = wkt
        for key in ("nodeid", "instituteid", "startdate", "enddate"):
            value = self.source_config.get(key)
            if value:
                params[key] = value
        return params

    def _fetch_obis_datasets(self, harvest_job):
        """Fetch the list of OBIS datasets matching the source config.

        Returns a list of dataset dicts, or None on error (after recording a
        gather error).
        """
        page_size = int(self.source_config.get("page_size", 1000))
        gather_timeout = int(
            self.source_config.get("gather_timeout", DEFAULT_GATHER_TIMEOUT)
        )

        # Resolve query params first (this may read spatial_filter_file). Kept in
        # its own try so a file error is reported as such — note requests
        # exceptions subclass OSError, so this must NOT wrap the HTTP call or an
        # API timeout would be mislabeled as a file error.
        try:
            params = self._obis_query_params()
        except OSError as e:
            self._save_gather_error(
                "Unable to read spatial_filter_file: %r" % e, harvest_job
            )
            return None

        session = _get_gather_session()
        try:
            params["size"] = page_size
            response = session.get(
                OBIS_DATASET_API, params=params, timeout=gather_timeout
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as e:
            self._save_gather_error(
                "Unable to query OBIS dataset API (%s): %r"
                % (OBIS_DATASET_API, e),
                harvest_job,
            )
            return None

        total = payload.get("total", 0)
        results = payload.get("results", []) or []

        # Delete-safety: the /v3/dataset endpoint has no offset paging, so we can
        # only fetch the whole list in one request. If OBIS ever exceeds the
        # single-response cap we would silently see a truncated list, which the
        # classifier could then read as "the rest were deleted". Fail the gather
        # instead so nothing is mistakenly removed.
        if total > _MAX_OBIS_SIZE:
            self._save_gather_error(
                "OBIS returned %s datasets, exceeding the single-response cap of "
                "%s. Refusing to proceed on a truncated list (raise _MAX_OBIS_SIZE "
                "or narrow the spatial_filter)." % (total, _MAX_OBIS_SIZE),
                harvest_job,
            )
            return None

        # The first request used size=page_size; re-request in one shot with
        # size=total when it did not cover everything.
        if total > len(results):
            try:
                params["size"] = total
                response = session.get(
                    OBIS_DATASET_API, params=params, timeout=gather_timeout
                )
                response.raise_for_status()
                results = response.json().get("results", []) or []
            except (requests.RequestException, ValueError) as e:
                self._save_gather_error(
                    "Unable to fetch full OBIS dataset list: %r" % e, harvest_job
                )
                return None

        return results

    # ------------------------------------------------------------------
    # Gather stage
    # ------------------------------------------------------------------

    def gather_stage(self, harvest_job):
        log.debug("OBISHarvester gather_stage for job: %r", harvest_job)
        self.harvest_job = harvest_job
        self._set_source_config(harvest_job.source.config)

        datasets = self._fetch_obis_datasets(harvest_job)
        if datasets is None:
            return None
        # Map OBIS UUID -> dataset record (lower-cased keys for stable matching).
        source = {}
        for ds in datasets:
            uuid = (ds.get("id") or "").lower()
            if uuid:
                source[uuid] = ds

        # Harvest objects already produced for this source (current ones).
        db_modified = {}    # guid -> obis_modified_date
        db_package_id = {}  # guid -> package_id
        query = (
            model.Session.query(
                HarvestObject.guid, HarvestObject.package_id, HOExtra.value
            )
            .join(HOExtra, HarvestObject.extras)
            .filter(HOExtra.key == "obis_modified_date")
            .filter(HarvestObject.current == True)  # noqa: E712
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        )
        for guid, package_id, modified_date in query:
            db_modified[guid] = modified_date
            db_package_id[guid] = package_id

        # Cross-source dedup: build a catalogue index once (excluding this
        # source's own datasets) so datasets already present from another source
        # are skipped and reported rather than re-imported.
        try:
            index = build_catalogue_index(exclude_source_id=harvest_job.source.id)
        except Exception as e:  # never fail the whole gather on a search hiccup
            log.warning("Could not build catalogue dedup index: %r", e)
            index = None

        max_delete_fraction = float(
            self.source_config.get(
                "max_delete_fraction", DEFAULT_MAX_DELETE_FRACTION
            )
        )
        classified = classify_datasets(
            source, db_modified, index, max_delete_fraction=max_delete_fraction
        )
        new = classified["new"]
        change = classified["change"]
        delete = classified["delete"]
        skipped = classified["skipped"]

        if classified["delete_suppressed"]:
            log.warning(
                "OBIS delete-safety guard tripped: this harvest would delete %s "
                "of %s datasets (> %.0f%%). Deletes SUPPRESSED for this run — the "
                "OBIS list response looks truncated or partial. Investigate before "
                "re-running.",
                classified["delete_candidates"],
                len(db_modified),
                max_delete_fraction * 100,
            )

        for guid, matched_ref, method in skipped:
            log.info(
                "OBIS dedup: skipping dataset %s — already in catalogue as %r "
                "(matched by %s)",
                guid,
                matched_ref,
                method,
            )

        ids = []

        def make_object(guid, status, package_id=None):
            ds = source.get(guid, {})
            modified = ds.get("updated") or ds.get("published") or ""
            extras = [
                HOExtra(key="status", value=status),
                HOExtra(key="obis_id", value=guid),
                HOExtra(key="obis_modified_date", value=modified),
                # The cioos_harvest plugin maps waf_location/waf_modified_date
                # to the required xml_location_url/xml_modified_date fields.
                # OBIS has no hosted ISO XML, so point at the source metadata.
                HOExtra(key="waf_location", value=OBIS_DATASET_API + "/" + guid),
                HOExtra(key="waf_modified_date", value=modified),
            ]
            # Capture the dataset DOI (OBIS exposes it under doi/citation_id) so
            # get_package_dict can store it and link back — it is not otherwise
            # available at fetch/import time.
            dois = sorted(obis_dataset_dois(ds))
            if dois:
                extras.append(HOExtra(key="obis_doi", value=dois[0]))
            kwargs = {"job": harvest_job, "extras": extras, "guid": guid}
            if package_id:
                kwargs["package_id"] = package_id
            obj = HarvestObject(**kwargs)
            obj.save()
            return obj.id

        for guid in new:
            ids.append(make_object(guid, "new"))

        for guid in change:
            ids.append(make_object(guid, "change", package_id=db_package_id[guid]))

        for guid in delete:
            model.Session.query(HarvestObject).filter_by(guid=guid).update(
                {"current": False}, False
            )
            ids.append(make_object(guid, "delete", package_id=db_package_id[guid]))

        log.info(
            "OBIS gather complete: %s new, %s change, %s delete, %s skipped (duplicates)",
            len(new),
            len(change),
            len(delete),
            len(skipped),
        )
        return ids

    # ------------------------------------------------------------------
    # Fetch stage
    # ------------------------------------------------------------------

    def fetch_stage(self, harvest_object):
        status = get_object_extra(harvest_object, "status")
        if status == "delete":
            return True

        # fetch runs in a separate worker process from gather, so load the
        # source config here for the throttle / retry knobs.
        self._set_source_config(harvest_object.source.config)

        obis_id = get_object_extra(harvest_object, "obis_id") or harvest_object.guid
        if not obis_id:
            self._save_object_error(
                "No OBIS id on harvest object %s" % harvest_object.id,
                harvest_object,
            )
            return False

        # Imported lazily so the gather/dedup code (and its tests) do not require
        # the converter package to be importable.
        try:
            from cioos_metadata_conversion.record import InputSchemas, Record
        except ImportError as e:
            self._save_object_error(
                "cioos-metadata-conversion is not installed: %r" % e, harvest_object
            )
            return False

        delay = float(
            self.source_config.get(
                "request_delay_seconds", DEFAULT_REQUEST_DELAY_SECONDS
            )
        )
        retries = int(
            self.source_config.get("fetch_retries", DEFAULT_FETCH_RETRIES)
        )

        # Politeness: pause between per-dataset OBIS calls.
        if delay > 0:
            time.sleep(delay)

        started = time.monotonic()
        iso_xml = None
        # attempt 1 is the initial try; `retries` further attempts on transient
        # network errors, with exponential backoff. Conversion/schema errors are
        # permanent and never retried.
        for attempt in range(1, retries + 2):
            try:
                record = Record(source=obis_id, schema=InputSchemas.obis)
                record.load()
                record.convert_to_cioos_schema()
                iso_xml = record.convert_to("iso19115-3_xml")
                break
            except _TRANSIENT_ERRORS as e:
                if attempt <= retries:
                    backoff = (delay or 0.5) * (2 ** (attempt - 1))
                    log.warning(
                        "OBIS fetch %s: transient network error (attempt %s/%s): "
                        "%r — retrying in %.1fs",
                        obis_id, attempt, retries + 1, e, backoff,
                    )
                    time.sleep(backoff)
                    continue
                self._save_object_error(
                    "Transient network error fetching OBIS dataset %s after %s "
                    "attempts: %r" % (obis_id, retries + 1, e),
                    harvest_object,
                )
                return False
            except Exception as e:
                # Permanent (conversion / schema / data) failure — do not retry.
                self._save_object_error(
                    "Failed to convert OBIS dataset %s to ISO 19115-3: %r"
                    % (obis_id, e),
                    harvest_object,
                )
                return False

        if not iso_xml:
            self._save_object_error(
                "Empty ISO 19115-3 document for OBIS dataset %s" % obis_id,
                harvest_object,
            )
            return False

        # The converter emits a leading `<?xml … encoding=…?>` declaration.
        # It is stored verbatim as the package's harvest_document_content, and
        # cioos_theme's before_dataset_index parses that with lxml.fromstring()
        # on the *str*, which rejects a unicode string carrying an encoding
        # declaration. Other CIOOS ISO sources store declaration-free XML, so
        # strip it here to match and keep the record indexable.
        iso_xml = _strip_xml_declaration(iso_xml)

        harvest_object.content = iso_xml
        harvest_object.save()
        # Per-object progress line so an operator can gauge throughput on a long
        # unattended run (the queue model gives no cheap N/total here).
        log.info(
            "OBIS fetched %s in %.1fs", obis_id, time.monotonic() - started
        )
        return True

    # ------------------------------------------------------------------
    # Package dict
    # ------------------------------------------------------------------

    def get_package_dict(self, iso_values, harvest_object):
        # The OBIS->CIOOS converter leaves the metadata identifier empty, so the
        # parsed ISO guid is blank. The parent get_package_dict needs a non-empty
        # guid to derive the package name and would otherwise raise. Force it to
        # the OBIS UUID (== harvest_object.guid set in gather) which keeps the id
        # stable and re-harvests idempotent.
        if not iso_values.get("guid"):
            iso_values["guid"] = harvest_object.guid

        package_dict = super().get_package_dict(iso_values, harvest_object)

        # NB: the final package name/URL slug is not set here. The shared
        # cioos_harvest plugin's ISpatialHarvester get_package_dict hook runs
        # after this and rewrites the name to munge_name(guid) for every CIOOS
        # harvester, so the OBIS datasets get the catalogue-standard guid-based
        # slug (the OBIS UUID). Overriding it here would be dead code.
        self._add_obis_links(package_dict, harvest_object)
        package_dict = self._maybe_translate(package_dict, harvest_object)
        return package_dict

    def _add_obis_links(self, package_dict, harvest_object):
        """Link the package back to OBIS: dataset page, data export, and DOI.

        The converter already produces resources for the *original* source
        archive (IPT / Darwin Core Archive). Here we add OBIS-side links and
        record the DOI so the dataset points back to where CIOOS harvested it.
        Idempotent: import rebuilds resources from a fresh package_dict each run.
        """
        obis_id = (
            get_object_extra(harvest_object, "obis_id") or harvest_object.guid or ""
        )
        if not obis_id:
            return

        resources = package_dict.setdefault("resources", [])
        existing_urls = {r.get("url") for r in resources}

        page_url = OBIS_DATASET_PAGE + obis_id
        parquet_url = OBIS_PARQUET_URL.format(uuid=obis_id)
        for res in (
            {
                "name": "OBIS Dataset Page",
                "url": page_url,
                "format": "HTML",
                "description": "View this dataset on OBIS (obis.org).",
            },
            {
                "name": "OBIS Occurrence Records (GeoParquet)",
                "url": parquet_url,
                "format": "Parquet",
                "description": (
                    "Full occurrence records for this dataset from the OBIS "
                    "open-data export on AWS (anonymous access)."
                ),
            },
        ):
            if res["url"] not in existing_urls:
                resources.append(res)

        # Store the DOI in unique-resource-identifier-full (subfields:
        # authority / code-space / code / version) if not already populated.
        doi = normalize_doi(get_object_extra(harvest_object, "obis_doi"))
        if doi and not package_dict.get("unique-resource-identifier-full"):
            package_dict["unique-resource-identifier-full"] = [
                {
                    "authority": "https://doi.org",
                    "code-space": "doi",
                    "code": doi,
                    "version": "",
                }
            ]

    # ------------------------------------------------------------------
    # Optional translation (env-gated hook — disabled by default)
    # ------------------------------------------------------------------

    def _translation_enabled(self):
        return os.environ.get(TRANSLATION_ENV_VAR, "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

    def _maybe_translate(self, package_dict, harvest_object):
        """Optionally translate EN -> FR on newly harvested records.

        Disabled unless ``CKANEXT__CIOOS_OBIS__ENABLE_TRANSLATION`` is truthy.
        The actual Cohere translation is not implemented yet (issue #20 scopes
        it as optional); when enabled we log a warning and return the record
        unchanged so harvesting still succeeds.
        """
        if not self._translation_enabled():
            return package_dict
        log.warning(
            "OBIS translation is enabled (%s) but not yet implemented; "
            "leaving record %s untranslated.",
            TRANSLATION_ENV_VAR,
            harvest_object.guid,
        )
        # TODO(#20): call the Cohere EN->FR translation here on title_translated,
        # notes_translated and keywords, gated by COHERE_API_KEY.
        return package_dict

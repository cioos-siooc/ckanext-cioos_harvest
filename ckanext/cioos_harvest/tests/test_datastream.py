"""Golden-file tests for the DataStream sitemap harvester pipeline.

Tests the complete transformation from ISO 19115-2 XML to a CKAN-ready
package dict, including keyword/title/notes translation (mocked):

    XML (gmi:MI_Metadata)
    → ckanext-spatial ISODocument.read_values()
    → DatastreamSitemapHarvester.get_package_dict()

Fixture files
-------------
- ``tests/fixtures/datastream/00-xml/<name>.xml``          — input XML
- ``tests/fixtures/datastream/01-ckan-package/<name>.json`` — expected package dict

The AWS Translate calls are replaced by a deterministic mock so the test
never hits the network or requires AWS credentials.

Workflow
--------
1. Add an ISO 19115-2/gmi:MI_Metadata XML file to ``00-xml/``.
2. Run ``pytest`` — if no package JSON exists, one is generated from the
   current pipeline output and the test is skipped.
3. Review/edit the generated JSON in ``01-ckan-package/<name>.json``.
4. Re-run — test now asserts pipeline output == stored JSON.

Regenerate all golden files after intentional pipeline changes::

    pytest --regenerate-fixtures
"""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ckanext.cioos_harvest.harvesters.waf_datastream import DatastreamSitemapHarvester

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).parent
_FIXTURES_DIR = _TESTS_DIR / "fixtures" / "datastream"
_XML_DIR = _FIXTURES_DIR / "00-xml"
_JSON_DIR = _FIXTURES_DIR / "01-ckan-package"

# ---------------------------------------------------------------------------
# Fields excluded from golden-file comparison
# ---------------------------------------------------------------------------

_SKIP_FIELDS = frozenset(
    {
        # CKAN-assigned fields (not set by the harvester)
        "isopen",
        "creator_user_id",
        # Fields set by plugin.py (ISpatialHarvester.get_package_dict),
        # which runs after the harvester and is not invoked in these tests.
        # License title/url depend on the CKAN license register configuration
        # (licenses_group_url) which may differ between environments.
        # Relationship/org context fields populated by CKAN storage layer
        "groups",
        "organization",
        "owner_org",
        "state",
        "type",
        "private",
        "relationships_as_object",
        "relationships_as_subject",
        # Misc fields skipped from golden comparison
        "xml_location_url",
        "author",
        "author_email",
        "maintainer",
        "maintainer_email",
        "url",
    }
)

_EXTRAS_SKIP_KEYS = frozenset(
    {
        # Harvest job/source IDs (change per run)
        "h_job_id",
        "h_object_id",
        "h_source_id",
        "h_source_title",
        "h_source_url",
        "harvest_object_id",
        "harvest_source_id",
        "harvest_source_title",
        "harvest_source_organization",
        # Set by plugin.py (ISpatialHarvester), not by the harvester itself
        "xml_modified_date",
        "harvest_source_quality_level",
        # Environment-specific or CKAN-internal
        "encoding",
        "uri",
    }
)

# Translated/fluent fields where the 'fr' sub-key is derived from AWS Translate
# at runtime.  The test uses a deterministic mock that produces '[FR:...]' strings
# instead of real French, so we strip 'fr' before comparison to avoid false failures.
_STRIP_FR_FIELDS = frozenset(
    {
        "keywords",
        "title_translated",
        "notes_translated",
    }
)

_RESOURCE_SKIP_FIELDS = frozenset(
    {
        "cache_last_updated",
        "cache_url",
        "created",
        "datastore_active",
        "hash",
        "id",
        "last_modified",
        "metadata_modified",
        "mimetype",
        "mimetype_inner",
        "package_id",
        "position",
        "resource_type",
        "size",
        "state",
        "url_type",
        "created_source",
        "metadata_modified_source",
    }
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_default(obj):
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _normalise(value):
    """Round-trip through JSON to convert lxml bytes → str."""
    return json.loads(json.dumps(value, default=_json_default))


def _normalise_extras(extras) -> dict:
    if isinstance(extras, dict):
        items = extras.items()
    else:
        items = ((e["key"], e["value"]) for e in extras)

    result = {}
    for key, value in items:
        if key in _EXTRAS_SKIP_KEYS:
            continue
        if isinstance(value, bool):
            value = str(value).lower()
        result[key] = value
    return dict(sorted(result.items()))


def _filter_resource(resource: dict) -> dict:
    return {k: v for k, v in resource.items() if k not in _RESOURCE_SKIP_FIELDS}


def _filter(pkg: dict) -> dict:
    result = {k: v for k, v in pkg.items() if k not in _SKIP_FIELDS}
    if "extras" in result:
        result["extras"] = _normalise_extras(result["extras"])
    if "resources" in result:
        result["resources"] = [_filter_resource(r) for r in result["resources"]]
    for field in _STRIP_FR_FIELDS:
        if field in result and isinstance(result[field], dict):
            result[field] = {k: v for k, v in result[field].items() if k != "fr"}
    return result


def _make_mock_harvest_object(guid: str) -> MagicMock:
    ho = MagicMock()
    ho.guid = guid
    ho.package = None
    ho.source.id = "00000000-0000-0000-0000-000000000001"
    ho.source.config = "{}"
    ho.source.title = "Test DataStream Harvest Source"
    ho.source.url = "https://datastream.org/dataset/sitemap.xml"
    ho.job.source.id = "00000000-0000-0000-0000-000000000001"
    ho.job.source.url = "https://datastream.org/dataset/sitemap.xml"
    ho.job.source.title = "Test DataStream Harvest Source"
    ho.job.id = "00000000-0000-0000-0000-000000000002"
    ho.id = "00000000-0000-0000-0000-000000000003"
    return ho


def _mock_translate(redis_conn, text, source_lang='en', target_lang='fr'):
    """Deterministic stand-in for AWS Translate — wraps text in language tags."""
    return f"[{target_lang.upper()}:{text}]"


def _fixture_names():
    if not _XML_DIR.exists():
        return []
    return sorted(p.stem for p in _XML_DIR.glob("*.xml"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDatastreamHarvester:
    """Golden-file tests for the DataStream ISO 19115-2 → CKAN package dict pipeline.

    AWS Translate is mocked with ``_mock_translate`` so tests are deterministic
    and require no AWS credentials or network access beyond loading the XML
    fixture from disk.
    """

    @pytest.fixture(autouse=True)
    def setup_harvester(self):
        """Prepare a singleton harvester instance with an empty source config."""
        self.harvester = DatastreamSitemapHarvester()
        self.harvester.config = {}

    @pytest.mark.parametrize("name", _fixture_names())
    def test_package_dict_matches_expected(self, request, name, app):
        """Parse a DataStream XML fixture, build the package dict, compare to golden JSON.

        The ``app`` fixture (provided by pytest-ckan) ensures the full CKAN
        plugin stack is initialised so ``get_package_dict()`` can load the
        dataset schema and language list.

        Missing JSON
        ~~~~~~~~~~~~
        If no JSON file exists for *name*, the current pipeline output is
        written as the new expected file and the test is skipped.  Review
        then re-run.

        Regenerating
        ~~~~~~~~~~~~
        Pass ``--regenerate-fixtures`` to overwrite all package JSON files.
        """
        from ckanext.spatial.harvesters.base import ISODocument as SpatialISODocument

        xml_path = _XML_DIR / f"{name}.xml"
        json_path = _JSON_DIR / f"{name}.json"

        regenerate = request.config.getoption("--regenerate-fixtures", default=False)

        # --- Step 1: parse XML with ckanext-spatial's ISO 19139 parser ---
        # ckanext-spatial's fetch stage strips the XML declaration via regex
        # (base.py line ~858) before storing content as a str.  Replicate that
        # here so SpatialISODocument receives the same string the real harvester
        # would pass.
        import re
        xml_str = xml_path.read_bytes().decode("utf-8")
        xml_str = re.sub(r'<\?xml.*?\?>', '', xml_str)
        iso_doc = SpatialISODocument(xml_str)
        iso_values = iso_doc.read_values()

        guid = iso_values.get("guid") or name
        mock_ho = _make_mock_harvest_object(guid)
        # Provide raw XML content so the harvester can parse gmd:useLimitation
        # (ckanext-spatial's ISODocument skips it, so _extract_use_limitation_url
        # needs to read it from harvest_object.content directly).
        mock_ho.content = xml_str

        # --- Step 2: build DataStream package dict ---
        mock_source_pkg = MagicMock()
        mock_source_pkg.owner_org = None

        # mock_redis always misses the cache → translate_string is always called
        mock_redis = MagicMock()
        mock_redis.hget.return_value = None

        with (
            patch(
                "ckanext.cioos_harvest.harvesters.waf_datastream.get_connection_redis",
                return_value=mock_redis,
            ),
            patch.object(
                self.harvester,
                "translate_string",
                side_effect=_mock_translate,
            ),
            patch("ckan.model.Package.get", return_value=mock_source_pkg),
            patch.object(
                self.harvester,
                "_save_object_error",
                side_effect=lambda msg, *a, **kw: log.warning("harvest error: %s", msg),
            ),
        ):
            package_dict = self.harvester.get_package_dict(iso_values, mock_ho)

        result = _filter(_normalise(package_dict))

        # --- Generate golden file if missing or regeneration requested ---
        if not json_path.exists() or regenerate:
            _JSON_DIR.mkdir(parents=True, exist_ok=True)
            json_path.write_text(
                json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False),
                encoding="utf-8",
            )
            action = "Regenerated" if regenerate else "Generated"
            pytest.skip(
                f"{action} expected package JSON for '{name}' → "
                f"tests/fixtures/datastream/01-ckan-package/{name}.json  "
                "Review the file and re-run to validate."
            )

        # --- Compare ---
        expected_raw = json.loads(json_path.read_text(encoding="utf-8"))
        expected = _filter(expected_raw)

        assert result == expected, (
            f"Package dict for DataStream '{name}' does not match expected JSON.\n"
            f"  Expected : {json_path}\n"
            f"  To regenerate all fixtures: pytest --regenerate-fixtures"
        )

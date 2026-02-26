"""Unit tests for the ISO 19115-3 XML → dict conversion.

Workflow
--------
1. Add ISO 19115-3 XML files to ``tests/fixtures/iso19115_3/00-xml/``.
   File names become the test case identifiers (e.g. ``my_record.xml``).

2. Run ``pytest``.  For any XML that has no matching JSON yet, the parser
   output is written to ``tests/fixtures/iso19115_3/01-harvester-output/<name>.json``
   and that test case is skipped with a notice.

3. Review each generated JSON file.  Edit values if the expected output
   should differ from what the parser currently produces.

4. Run ``pytest`` again – tests now assert that the parser output matches
   the stored JSON exactly.

Regenerating golden files
-------------------------
After intentional parser changes, regenerate all expected JSON files with::

    pytest --regenerate-fixtures

This overwrites every JSON file with the current parser output and skips
each test.  Re-run without the flag to confirm the new output is stable.
"""

import json
import logging
from pathlib import Path

import pytest
from lxml import etree

from ckanext.cioos_harvest.model.harvested_metadata_iso19115_3 import (
    ISODocument_iso19115_3,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).parent
_FIXTURES_DIR = _TESTS_DIR / "fixtures" / "iso19115_3"
_XML_DIR = _FIXTURES_DIR / "00-xml"
_JSON_DIR = _FIXTURES_DIR / "01-harvester-output"

# ISO 19115-3 metadata namespace
_MDB_NS = "http://standards.iso.org/iso/19115/-3/mdb/2.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_default(obj):
    """Serialise types the standard JSON encoder cannot handle.

    lxml XPath occasionally returns byte strings (e.g. from serialised
    sub-trees).  Convert them to UTF-8 text so the comparison is stable.
    """
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


def _normalise(value):
    """Round-trip a parsed dict through JSON to canonicalise bytes → str.

    This ensures the in-memory result and the file-loaded expected value
    use the same types before comparison.
    """
    return json.loads(json.dumps(value, default=_json_default))


def _fixture_names():
    """Return the stems of all XML files in the fixtures directory.

    Called at collection time so pytest.mark.parametrize has the full list.
    Returns an empty list when the directory does not exist yet, which
    simply means no tests are generated until fixtures are added.
    """
    if not _XML_DIR.exists():
        return []
    return sorted(p.stem for p in _XML_DIR.glob("*.xml"))


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestISO19115_3Parser:
    """Golden-file tests for the ISO 19115-3 XML → dict conversion.

    Each XML fixture is paired with a JSON file that stores the expected
    output of ``ISODocument_iso19115_3.read_values()``.  Tests fail when
    the parser output diverges from the stored expectation.
    """

    @pytest.mark.parametrize("name", _fixture_names())
    def test_parse_matches_expected_json(self, request, name):
        """Parse an XML fixture and compare the result to its expected JSON.

        Missing JSON
        ~~~~~~~~~~~~
        If no JSON file exists for *name*, the current parser output is
        written as the new expected file and the test is skipped.  Review
        the generated file then re-run without ``--regenerate-fixtures`` to
        confirm it is correct.

        Regenerating
        ~~~~~~~~~~~~
        Pass ``--regenerate-fixtures`` to overwrite all JSON files with
        fresh parser output (useful after intentional parser changes).
        """
        xml_path = _XML_DIR / f"{name}.xml"
        json_path = _JSON_DIR / f"{name}.json"

        regenerate = request.config.getoption("--regenerate-fixtures", default=False)

        # --- Parse ---
        xml_bytes = xml_path.read_bytes()
        doc = ISODocument_iso19115_3(xml_bytes)
        result = _normalise(doc.read_values())

        # --- Generate golden file if missing or regeneration requested ---
        if not json_path.exists() or regenerate:
            _JSON_DIR.mkdir(parents=True, exist_ok=True)
            json_path.write_text(
                json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False),
                encoding="utf-8",
            )
            action = "Regenerated" if regenerate else "Generated"
            pytest.skip(
                f"{action} expected JSON for '{name}' → "
                f"tests/fixtures/iso19115_3/01-harvester-output/{name}.json  "
                "Review the file and re-run to validate."
            )

        # --- Compare ---
        expected = json.loads(json_path.read_text(encoding="utf-8"))
        assert result == expected, (
            f"Parser output for '{name}' does not match the expected JSON.\n"
            f"  Expected : {json_path}\n"
            f"  To regenerate all fixtures: pytest --regenerate-fixtures"
        )

    @pytest.mark.parametrize("name", _fixture_names())
    def test_xml_root_element_is_mdb_md_metadata(self, name):
        """Every XML fixture must have ``mdb:MD_Metadata`` as its root element.

        The ISO 19115-3 harvester rejects anything that is not a proper
        ISO 19115-3 document (e.g. ISO 19139 ``gmd:MD_Metadata`` records).
        """
        xml_path = _XML_DIR / f"{name}.xml"
        tree = etree.fromstring(xml_path.read_bytes())
        expected_tag = f"{{{_MDB_NS}}}MD_Metadata"
        assert tree.tag == expected_tag, (
            f"'{name}.xml' has root element <{tree.tag}>, "
            f"expected <{{{{{_MDB_NS}}}}}MD_Metadata>.  "
            "This harvester only accepts ISO 19115-3 documents."
        )

"""Unit tests for the OBIS harvester's deduplication / classification helpers.

These tests are pure Python — they import only ``obis_dedup`` (no CKAN stack),
so they run anywhere with ``pytest``.
"""

from ckanext.cioos_harvest.harvesters.obis_dedup import (
    CatalogueIndex,
    build_catalogue_index,
    classify_datasets,
    normalize_doi,
    obis_dataset_dois,
    package_dois,
    package_obis_uuids,
)

UUID_A = "00017595-e015-4ec6-bf8a-b013e0dca521"
UUID_B = "4bc13a6a-71a3-4a2d-b23d-3ce50a9aef41"
UUID_C = "11111111-2222-3333-4444-555555555555"


# ---------------------------------------------------------------------------
# normalize_doi
# ---------------------------------------------------------------------------


class TestNormalizeDoi:
    def test_strips_url_prefixes(self):
        assert normalize_doi("https://doi.org/10.123/ABC") == "10.123/abc"
        assert normalize_doi("http://dx.doi.org/10.9/x") == "10.9/x"
        assert normalize_doi("doi:10.5/Y") == "10.5/y"

    def test_bare_doi_passthrough(self):
        assert normalize_doi("10.26071/mxtr-gp72") == "10.26071/mxtr-gp72"

    def test_non_doi_and_empty(self):
        assert normalize_doi("") is None
        assert normalize_doi(None) is None
        assert normalize_doi("not a doi") is None
        assert normalize_doi("https://obis.org/dataset/%s" % UUID_A) is None


# ---------------------------------------------------------------------------
# OBIS dataset / CKAN package extraction
# ---------------------------------------------------------------------------


class TestExtraction:
    def test_obis_dataset_dois_checks_doi_and_citation_id(self):
        assert obis_dataset_dois({"doi": "10.1/a"}) == {"10.1/a"}
        # citation_id is used when doi is null (common in OBIS)
        assert obis_dataset_dois({"doi": None, "citation_id": "10.2/b"}) == {"10.2/b"}
        assert obis_dataset_dois({"doi": "10.1/a", "citation_id": "10.2/b"}) == {
            "10.1/a",
            "10.2/b",
        }
        assert obis_dataset_dois({"doi": None, "citation_id": None}) == set()

    def test_package_dois_from_unique_resource_identifier_full(self):
        pkg = {
            "unique-resource-identifier-full": [
                {"code": "https://doi.org/10.1/a"},
                {"code": "not-a-doi"},
                {"authority": "x"},  # no code
            ]
        }
        assert package_dois(pkg) == {"10.1/a"}

    def test_package_obis_uuids_from_multiple_locations(self):
        pkg = {
            "harvest_document_content": "see https://obis.org/dataset/%s here" % UUID_A,
            "resources": [{"url": "https://obis.org/dataset/%s.zip" % UUID_B}],
            "unique-resource-identifier-full": [
                {"code": "http://obis.org/dataset/%s" % UUID_C}
            ],
        }
        assert package_obis_uuids(pkg) == {UUID_A, UUID_B, UUID_C}

    def test_package_obis_uuids_empty(self):
        assert package_obis_uuids({"notes": "no obis links here"}) == set()


# ---------------------------------------------------------------------------
# CatalogueIndex
# ---------------------------------------------------------------------------


class TestCatalogueIndex:
    def _index(self):
        index = CatalogueIndex()
        index.add_package(
            {
                "name": "slgo-dataset",
                "unique-resource-identifier-full": [{"code": "https://doi.org/10.1/a"}],
            }
        )
        index.add_package(
            {
                "name": "dfo-dataset",
                "harvest_document_content": "https://obis.org/dataset/%s" % UUID_B,
            }
        )
        return index

    def test_match_by_doi(self):
        index = self._index()
        assert index.match(UUID_C, {"10.1/a"}) == ("slgo-dataset", "doi")

    def test_match_by_uuid_takes_precedence(self):
        index = self._index()
        # Even with an unrelated DOI, a UUID hit wins and is reported as "uuid".
        assert index.match(UUID_B, {"10.999/zzz"}) == ("dfo-dataset", "uuid")

    def test_no_match(self):
        index = self._index()
        assert index.match(UUID_C, {"10.999/zzz"}) == (None, None)


# ---------------------------------------------------------------------------
# classify_datasets — the core gather logic
# ---------------------------------------------------------------------------


class TestClassifyDatasets:
    def test_new_change_delete(self):
        source = {
            UUID_A: {"id": UUID_A, "updated": "2026-01-01"},  # new
            UUID_B: {"id": UUID_B, "updated": "2026-06-01"},  # changed (newer)
            UUID_C: {"id": UUID_C, "updated": "2025-01-01"},  # unchanged
        }
        db_modified = {
            UUID_B: "2026-01-01",  # older than source -> change
            UUID_C: "2025-01-01",  # same -> no change
            "deadbeef-0000-0000-0000-000000000000": "2020-01-01",  # gone -> delete
        }
        # max_delete_fraction=1.0 disables the delete-safety guard so this test
        # exercises raw classification (1-of-3 delete would otherwise trip it).
        result = classify_datasets(
            source, db_modified, index=None, max_delete_fraction=1.0
        )
        assert result["new"] == {UUID_A}
        assert result["change"] == [UUID_B]
        assert result["delete"] == {"deadbeef-0000-0000-0000-000000000000"}
        assert result["skipped"] == []

    def test_dateless_datasets_treated_as_unchanged(self):
        # Neither side has a date -> treat as unchanged so we don't re-fetch a
        # date-less dataset on every single harvest (idempotency).
        source = {UUID_A: {"id": UUID_A}}  # no updated/published
        db_modified = {UUID_A: ""}
        result = classify_datasets(source, db_modified, index=None)
        assert result["change"] == []
        assert result["new"] == set()

    def test_change_when_source_gains_a_date(self):
        # Stored object had no date but OBIS now advertises one -> re-import.
        source = {UUID_A: {"id": UUID_A, "updated": "2026-01-01"}}
        db_modified = {UUID_A: ""}
        result = classify_datasets(source, db_modified, index=None)
        assert result["change"] == [UUID_A]

    def test_dedup_skips_only_new_datasets(self):
        source = {
            UUID_A: {"id": UUID_A, "doi": "10.1/a", "updated": "2026-01-01"},
            UUID_B: {"id": UUID_B, "updated": "2026-01-01"},
        }
        index = CatalogueIndex()
        index.add_package(
            {
                "name": "existing",
                "unique-resource-identifier-full": [{"code": "10.1/a"}],
            }
        )
        result = classify_datasets(source, db_modified={}, index=index)
        # UUID_A matches an existing catalogue DOI -> skipped, not new.
        assert result["new"] == {UUID_B}
        assert result["skipped"] == [(UUID_A, "existing", "doi")]

    def test_existing_source_dataset_not_dedup_skipped(self):
        # A dataset already owned by this source (in db_modified) is a 'change',
        # never a cross-source duplicate, even if its DOI is in the index.
        source = {UUID_A: {"id": UUID_A, "doi": "10.1/a", "updated": "2026-06-01"}}
        db_modified = {UUID_A: "2026-01-01"}
        index = CatalogueIndex()
        index.add_package(
            {"name": "self", "unique-resource-identifier-full": [{"code": "10.1/a"}]}
        )
        result = classify_datasets(source, db_modified, index=index)
        assert result["change"] == [UUID_A]
        assert result["skipped"] == []


# ---------------------------------------------------------------------------
# Delete-safety guard (A1)
# ---------------------------------------------------------------------------


class TestDeleteSafetyGuard:
    @staticmethod
    def _existing(n):
        return {"%08d-0000-0000-0000-000000000000" % i: "2020-01-01" for i in range(n)}

    def test_suppresses_mass_delete_on_short_response(self):
        # 100 held, OBIS returns an (empty) short list -> 100% delete -> suppress.
        db_modified = self._existing(100)
        result = classify_datasets({}, db_modified, index=None)
        assert result["delete_suppressed"] is True
        assert result["delete_candidates"] == 100
        assert result["delete"] == set()

    def test_allows_small_delete(self):
        db_modified = self._existing(100)
        keep = list(db_modified)[:95]  # 5 genuinely gone = 5% < 20%
        source = {g: {"id": g, "updated": "2020-01-01"} for g in keep}
        result = classify_datasets(source, db_modified, index=None)
        assert result["delete_suppressed"] is False
        assert len(result["delete"]) == 5

    def test_guard_fraction_is_configurable(self):
        db_modified = self._existing(10)
        # 100% delete, but a threshold of 1.0 permits it (guard triggers on >).
        result = classify_datasets(
            {}, db_modified, index=None, max_delete_fraction=1.0
        )
        assert result["delete_suppressed"] is False
        assert len(result["delete"]) == 10

    def test_no_existing_no_suppression(self):
        # First-ever harvest (nothing held) never suppresses.
        result = classify_datasets({}, {}, index=None)
        assert result["delete_suppressed"] is False
        assert result["delete"] == set()


# ---------------------------------------------------------------------------
# build_catalogue_index (paging) with a stub get_action
# ---------------------------------------------------------------------------


class TestBuildCatalogueIndex:
    def test_pages_through_results(self):
        all_pkgs = [
            {"name": "p%d" % i, "unique-resource-identifier-full": [{"code": "10.1/%d" % i}]}
            for i in range(2500)
        ]

        calls = []

        def stub_get_action(_name):
            def action(_context, data_dict):
                calls.append((data_dict["start"], data_dict["rows"]))
                start, rows = data_dict["start"], data_dict["rows"]
                return {"count": len(all_pkgs), "results": all_pkgs[start:start + rows]}

            return action

        index = build_catalogue_index(
            get_action=stub_get_action, context={}, rows=1000
        )
        # 2500 packages over rows=1000 -> 3 pages (0, 1000, 2000).
        assert calls == [(0, 1000), (1000, 1000), (2000, 1000)]
        assert index.match(UUID_A, {"10.1/0"}) == ("p0", "doi")
        assert index.match(UUID_A, {"10.1/2499"}) == ("p2499", "doi")

    def test_exclude_source_id_sets_fq(self):
        seen_fq = {}

        def stub_get_action(_name):
            def action(_context, data_dict):
                seen_fq["fq"] = data_dict["fq"]
                return {"count": 0, "results": []}

            return action

        build_catalogue_index(
            get_action=stub_get_action, context={}, exclude_source_id="src-123"
        )
        assert seen_fq["fq"] == '-harvest_source_id:"src-123"'

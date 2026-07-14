"""Tests for the OBISHarvester stages.

The pure new/change/delete + dedup classification is covered in
``test_obis_dedup.py``.  These tests exercise the harvester object itself
(config validation, OBIS query-param building, the guid-forcing in
``get_package_dict``, the env-gated translation hook, and ``fetch_stage``
conversion).  Importing the harvester pulls in the CKAN/ckanext-spatial stack,
so these run under the same environment as ``test_iso19115_3.py``.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from ckanext.cioos_harvest.harvesters.obis import OBISHarvester, TRANSLATION_ENV_VAR
from ckanext.cioos_harvest.harvesters.waf import WAFHarvesterISO19115_3

POLYGON = "POLYGON((-65 44,-60 44,-60 48,-65 48,-65 44))"


def _extra(key, value):
    return SimpleNamespace(key=key, value=value)


def _make_ho(guid, status="new", obis_id=None, content=None, doi=None, config=None):
    """Minimal stand-in HarvestObject (extras is a real iterable)."""
    ho = MagicMock()
    ho.guid = guid
    ho.package = None
    ho.content = content
    ho.extras = [
        _extra("status", status),
        _extra("obis_id", obis_id or guid),
        _extra("obis_modified_date", "2026-01-01"),
    ]
    if doi is not None:
        ho.extras.append(_extra("obis_doi", doi))
    # fetch_stage loads the source config for throttle/retry knobs; keep tests
    # fast (no delay) and deterministic (no retries) unless a test overrides it.
    ho.source.config = config or '{"request_delay_seconds": 0, "fetch_retries": 0}'
    return ho


# ---------------------------------------------------------------------------
# Config validation + OBIS query params
# ---------------------------------------------------------------------------


class TestConfigAndParams:
    @pytest.fixture(autouse=True)
    def harvester(self):
        self.h = OBISHarvester()

    def test_info(self):
        assert self.h.info()["name"] == "obis_harvester"

    def test_validate_config_rejects_bad_json(self):
        with pytest.raises(ValueError):
            self.h.validate_config("{not valid json")

    def test_validate_config_rejects_non_polygon(self):
        with pytest.raises(ValueError):
            self.h.validate_config(json.dumps({"spatial_filter": "BOX(0 0,1 1)"}))

    def test_validate_config_rejects_bad_page_size(self):
        with pytest.raises(ValueError):
            self.h.validate_config(json.dumps({"page_size": -5}))

    def test_validate_config_accepts_polygon(self):
        cfg = json.dumps({"spatial_filter": POLYGON, "page_size": 500})
        assert self.h.validate_config(cfg) == cfg

    def test_validate_config_allows_empty(self):
        assert self.h.validate_config("") == ""

    def test_validate_config_discovery_values(self):
        assert self.h.validate_config(json.dumps({"discovery": "s3"}))
        assert self.h.validate_config(json.dumps({"discovery": "api"}))
        with pytest.raises(ValueError):
            self.h.validate_config(json.dumps({"discovery": "ftp"}))

    def test_validate_config_s3_rejects_spatial_filter(self):
        with pytest.raises(ValueError):
            self.h.validate_config(
                json.dumps({"discovery": "s3", "spatial_filter": POLYGON})
            )

    def test_query_params_include_geometry_and_filters(self):
        self.h.source_config = {
            "spatial_filter": POLYGON,
            "nodeid": "abc",
            "startdate": "2020-01-01",
        }
        params = self.h._obis_query_params()
        assert params["geometry"] == POLYGON
        assert params["nodeid"] == "abc"
        assert params["startdate"] == "2020-01-01"

    def test_query_params_empty_when_no_filters(self):
        self.h.source_config = {}
        assert self.h._obis_query_params() == {}

    def test_spatial_filter_file_takes_precedence(self, tmp_path):
        wkt_file = tmp_path / "region.wkt"
        wkt_file.write_text(POLYGON + "\n")
        self.h.source_config = {
            "spatial_filter_file": str(wkt_file),
            "spatial_filter": "POLYGON((9 9,9 9,9 9,9 9))",
        }
        assert self.h._resolve_spatial_filter() == POLYGON


# ---------------------------------------------------------------------------
# get_package_dict — guid forcing + readable name
# ---------------------------------------------------------------------------


class TestGetPackageDict:
    @pytest.fixture(autouse=True)
    def harvester(self):
        self.h = OBISHarvester()

    def test_forces_guid_from_harvest_object(self):
        ho = _make_ho("AB12-XyZ", obis_id="AB12-XyZ")
        iso_values = {}  # converter leaves the metadata identifier empty -> no guid

        with patch.object(
            WAFHarvesterISO19115_3,
            "get_package_dict",
            return_value={"id": "x", "name": "from-super"},
        ) as mock_super:
            self.h.get_package_dict(iso_values, ho)

        # The parent was handed a non-empty guid (the OBIS UUID), which is what
        # lets it derive a package id/name instead of raising. (The final URL
        # slug is set later by the shared cioos_harvest plugin hook from the
        # guid, so we assert on what we pass to super, not the returned name.)
        passed_iso = mock_super.call_args.args[0]
        assert passed_iso["guid"] == "AB12-XyZ"

    def test_existing_guid_is_not_overwritten(self):
        ho = _make_ho("uuid-1", obis_id="uuid-1")
        iso_values = {"guid": "already-there"}
        with patch.object(
            WAFHarvesterISO19115_3, "get_package_dict", return_value={"name": "n"}
        ) as mock_super:
            self.h.get_package_dict(iso_values, ho)
        assert mock_super.call_args.args[0]["guid"] == "already-there"

    def test_adds_obis_links_and_doi(self):
        ho = _make_ho("AB12-XyZ", obis_id="AB12-XyZ", doi="10.5/xyz")
        iso_values = {"guid": "AB12-XyZ"}
        # super() returns the converter's original-source resources; ours append.
        with patch.object(
            WAFHarvesterISO19115_3,
            "get_package_dict",
            return_value={
                "id": "x",
                "name": "n",
                "resources": [
                    {"name": "Darwin Core Archive", "url": "http://ipt/archive"}
                ],
            },
        ):
            pkg = self.h.get_package_dict(iso_values, ho)

        urls = [r["url"] for r in pkg["resources"]]
        assert "https://obis.org/dataset/AB12-XyZ" in urls
        assert (
            "https://obis-open-data.s3.amazonaws.com/occurrence/AB12-XyZ.parquet"
            in urls
        )
        assert "http://ipt/archive" in urls  # original source preserved
        assert pkg["unique-resource-identifier-full"] == [
            {
                "authority": "https://doi.org",
                "code-space": "doi",
                "code": "10.5/xyz",
                "version": "",
            }
        ]

    def test_links_added_without_doi(self):
        ho = _make_ho("AB12-XyZ", obis_id="AB12-XyZ")  # no obis_doi extra
        iso_values = {"guid": "AB12-XyZ"}
        with patch.object(
            WAFHarvesterISO19115_3,
            "get_package_dict",
            return_value={"id": "x", "name": "n"},
        ):
            pkg = self.h.get_package_dict(iso_values, ho)
        assert any("obis.org/dataset" in r["url"] for r in pkg["resources"])
        assert not pkg.get("unique-resource-identifier-full")


# ---------------------------------------------------------------------------
# Translation hook (env-gated, disabled by default)
# ---------------------------------------------------------------------------


class TestTranslationHook:
    @pytest.fixture(autouse=True)
    def harvester(self):
        self.h = OBISHarvester()

    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv(TRANSLATION_ENV_VAR, raising=False)
        assert self.h._translation_enabled() is False
        pkg = {"title": "x"}
        assert self.h._maybe_translate(pkg, _make_ho("u1")) is pkg

    def test_enabled_via_env_returns_record_unchanged(self, monkeypatch):
        monkeypatch.setenv(TRANSLATION_ENV_VAR, "true")
        assert self.h._translation_enabled() is True
        pkg = {"title": "x"}
        # Stub: still returns the record unchanged (not yet implemented).
        assert self.h._maybe_translate(pkg, _make_ho("u1")) == pkg


# ---------------------------------------------------------------------------
# S3-listing discovery
# ---------------------------------------------------------------------------


class TestS3Discovery:
    @pytest.fixture(autouse=True)
    def harvester(self):
        self.h = OBISHarvester()
        self.h.source_config = {}

    @staticmethod
    def _page(contents, token=None):
        items = "".join(
            "<Contents><Key>%s</Key><LastModified>%s</LastModified></Contents>"
            % (k, lm)
            for k, lm in contents
        )
        trunc = "true" if token else "false"
        nt = "<NextContinuationToken>%s</NextContinuationToken>" % token if token else ""
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<IsTruncated>%s</IsTruncated>%s%s</ListBucketResult>"
            % (trunc, nt, items)
        ).encode()

    def test_parses_uuids_and_paginates(self):
        u1 = "00017595-e015-4ec6-bf8a-b013e0dca521"
        u2 = "0001aa41-e3e4-40a0-9193-9a5c81c627bf"
        pages = [
            self._page(
                [("occurrence/%s.parquet" % u1, "2025-09-11T19:32:34.000Z")],
                token="TOKEN",
            ),
            self._page(
                [
                    ("occurrence/%s.parquet" % u2, "2025-09-23T16:32:55.000Z"),
                    ("occurrence/", "x"),  # prefix placeholder key -> ignored
                    ("occurrence/not-a-uuid.parquet", "x"),  # ignored
                ]
            ),
        ]
        responses = [MagicMock(content=p) for p in pages]
        session = MagicMock()
        session.get.side_effect = responses
        with patch(
            "ckanext.cioos_harvest.harvesters.obis._get_gather_session",
            return_value=session,
        ):
            out = self.h._fetch_obis_datasets_s3(MagicMock())
        assert [d["id"] for d in out] == [u1, u2]
        assert out[0]["updated"] == "2025-09-11T19:32:34.000Z"
        assert session.get.call_count == 2  # followed the continuation token


# ---------------------------------------------------------------------------
# fetch_stage — OBIS -> ISO 19115-3 conversion
# ---------------------------------------------------------------------------


class TestFetchStage:
    @pytest.fixture(autouse=True)
    def harvester(self):
        self.h = OBISHarvester()

    def test_delete_status_short_circuits(self):
        assert self.h.fetch_stage(_make_ho("u1", status="delete")) is True

    def _patched_record(self, record):
        """Patch the lazily-imported cioos_metadata_conversion.record module."""
        fake_mod = SimpleNamespace(
            Record=MagicMock(return_value=record),
            InputSchemas=SimpleNamespace(obis="obis"),
        )
        return patch.dict(
            "sys.modules",
            {
                "cioos_metadata_conversion": MagicMock(),
                "cioos_metadata_conversion.record": fake_mod,
            },
        ), fake_mod

    def test_fetch_converts_and_stores_iso_xml(self):
        ho = _make_ho("u1", obis_id="u1")
        record = MagicMock()
        record.convert_to.return_value = "<mdb:MD_Metadata/>"
        ctx, fake_mod = self._patched_record(record)
        with ctx:
            assert self.h.fetch_stage(ho) is True

        assert ho.content == "<mdb:MD_Metadata/>"
        fake_mod.Record.assert_called_once_with(source="u1", schema="obis")
        record.load.assert_called_once()
        record.convert_to_cioos_schema.assert_called_once()
        record.convert_to.assert_called_once_with("iso19115-3_xml")

    def test_fetch_strips_xml_declaration(self):
        # The converter emits a leading <?xml … encoding=…?> declaration, which
        # must be stripped before storing so cioos_theme's index hook can parse
        # the stored content with lxml.fromstring() on the str.
        ho = _make_ho("u1", obis_id="u1")
        record = MagicMock()
        record.convert_to.return_value = (
            '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n'
            "<mdb:MD_Metadata/>"
        )
        ctx, _ = self._patched_record(record)
        with ctx:
            assert self.h.fetch_stage(ho) is True
        assert ho.content == "<mdb:MD_Metadata/>"
        assert "<?xml" not in ho.content

    def test_fetch_errors_on_empty_xml(self):
        ho = _make_ho("u1", obis_id="u1")
        record = MagicMock()
        record.convert_to.return_value = ""
        ctx, _ = self._patched_record(record)
        with ctx, patch.object(self.h, "_save_object_error") as err:
            assert self.h.fetch_stage(ho) is False
        err.assert_called_once()

    def test_fetch_errors_on_conversion_exception(self):
        ho = _make_ho("u1", obis_id="u1")
        record = MagicMock()
        record.load.side_effect = RuntimeError("boom")
        ctx, _ = self._patched_record(record)
        with ctx, patch.object(self.h, "_save_object_error") as err:
            assert self.h.fetch_stage(ho) is False
        err.assert_called_once()

    def test_fetch_retries_transient_then_succeeds(self):
        ho = _make_ho(
            "u1", obis_id="u1",
            config='{"request_delay_seconds": 0, "fetch_retries": 2}',
        )
        record = MagicMock()
        # First load() blips, second succeeds.
        record.load.side_effect = [
            requests.exceptions.ConnectionError("blip"),
            None,
        ]
        record.convert_to.return_value = "<mdb:MD_Metadata/>"
        ctx, _ = self._patched_record(record)
        with ctx, patch("ckanext.cioos_harvest.harvesters.obis.time.sleep"):
            assert self.h.fetch_stage(ho) is True
        assert ho.content == "<mdb:MD_Metadata/>"
        assert record.load.call_count == 2

    def test_fetch_transient_exhausts_retries(self):
        ho = _make_ho(
            "u1", obis_id="u1",
            config='{"request_delay_seconds": 0, "fetch_retries": 1}',
        )
        record = MagicMock()
        record.load.side_effect = requests.exceptions.ConnectionError("down")
        ctx, _ = self._patched_record(record)
        with ctx, patch("ckanext.cioos_harvest.harvesters.obis.time.sleep"), \
                patch.object(self.h, "_save_object_error") as err:
            assert self.h.fetch_stage(ho) is False
        # 1 initial attempt + 1 retry.
        assert record.load.call_count == 2
        err.assert_called_once()

    def test_fetch_permanent_error_not_retried(self):
        ho = _make_ho(
            "u1", obis_id="u1",
            config='{"request_delay_seconds": 0, "fetch_retries": 3}',
        )
        record = MagicMock()
        record.load.side_effect = ValueError("bad schema")  # not transient
        ctx, _ = self._patched_record(record)
        with ctx, patch.object(self.h, "_save_object_error") as err:
            assert self.h.fetch_stage(ho) is False
        assert record.load.call_count == 1  # no retries on a permanent error
        err.assert_called_once()

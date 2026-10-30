"""Tests for error_reporting.py."""

import logging
from types import SimpleNamespace

from ckanext.cioos_harvest import error_reporting


class _Connection:
    def __init__(self, row):
        self.row = row

    def execute(self, statement, params):
        return SimpleNamespace(first=lambda: self.row)


def _errors(caplog):
    return [r for r in caplog.records if r.levelno == logging.ERROR]


def test_gather_error_logged_at_error_grouped_by_source(caplog):
    target = SimpleNamespace(harvest_job_id="job-1", message="502 Bad gateway")
    error_reporting._on_gather_error(
        None, _Connection(("cioos-pacific", "ckan_cioos")), target
    )

    (record,) = _errors(caplog)
    assert record.msg == "Harvest gather error on cioos-pacific: %s"
    assert (
        record.getMessage() == "Harvest gather error on cioos-pacific: 502 Bad gateway"
    )
    assert record.harvest_job_id == "job-1"
    assert record.harvest_type == "ckan_cioos"


def test_object_error_includes_stage_line_and_guid(caplog):
    target = SimpleNamespace(
        harvest_object_id="obj-1", message="Invalid package", line=12, stage="Import"
    )
    error_reporting._on_object_error(
        None,
        _Connection(("slgo", "waf_iso19115_3_harvester", "guid-1", "job-1")),
        target,
    )

    (record,) = _errors(caplog)
    assert record.msg == "Harvest import error on slgo: %s"
    assert record.getMessage().endswith("Invalid package, line 12")
    assert record.harvest_object_guid == "guid-1"
    assert record.harvest_job_id == "job-1"


def test_reporting_failure_does_not_raise(caplog):
    class Broken:
        def execute(self, *a):
            raise RuntimeError("db gone")

    target = SimpleNamespace(harvest_job_id="job-1", message="x")
    error_reporting._on_gather_error(None, Broken(), target)

    assert not _errors(caplog)

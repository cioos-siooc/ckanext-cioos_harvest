"""Log every harvest gather/object error at ERROR so it reaches Sentry.

ckanext-harvest records harvester failures as HarvestGatherError and
HarvestObjectError rows and logs them at INFO/DEBUG at most, below the level
ckanext-sentry turns into events. Some callers (ckanext-harvest's queue and
job actions, our own base.py) build those rows directly instead of going
through ``.create()``, so the one place that sees all of them is the insert.

The log template names the source and stage, so Sentry groups the errors into
one issue per harvest source and stage rather than one per dataset.
"""

import logging

from sqlalchemy import event, text

from ckanext.harvest.model import HarvestGatherError, HarvestObjectError

log = logging.getLogger(__name__)

_GATHER_SOURCE = text(
    "SELECT p.name, s.type FROM harvest_job j "
    "JOIN harvest_source s ON s.id = j.source_id "
    "LEFT JOIN package p ON p.id = s.id WHERE j.id = :id"
)
_OBJECT_SOURCE = text(
    "SELECT p.name, s.type, o.guid, o.harvest_job_id FROM harvest_object o "
    "JOIN harvest_source s ON s.id = o.harvest_source_id "
    "LEFT JOIN package p ON p.id = s.id WHERE o.id = :id"
)


def _report(stage, source_name, source_type, message, extra):
    # Escape so a '%' in a source name can't break the log template.
    source = (source_name or "unknown source").replace("%", "%%")
    log.error(
        "Harvest %s error on %s: %%s" % (stage.lower(), source),
        message,
        extra={"harvest_source": source_name, "harvest_type": source_type, **extra},
    )


def _on_gather_error(mapper, connection, target):
    try:
        row = connection.execute(_GATHER_SOURCE, {"id": target.harvest_job_id}).first()
        _report(
            "gather",
            row and row[0],
            row and row[1],
            target.message,
            {"harvest_job_id": target.harvest_job_id},
        )
    except Exception:  # reporting must never break the harvest
        log.warning("Could not report harvest gather error", exc_info=True)


def _on_object_error(mapper, connection, target):
    try:
        row = connection.execute(
            _OBJECT_SOURCE, {"id": target.harvest_object_id}
        ).first()
        message = (
            "%s, line %s" % (target.message, target.line)
            if target.line
            else target.message
        )
        _report(
            target.stage or "object",
            row and row[0],
            row and row[1],
            message,
            {
                "harvest_object_id": target.harvest_object_id,
                "harvest_object_guid": row and row[2],
                "harvest_job_id": row and row[3],
            },
        )
    except Exception:
        log.warning("Could not report harvest object error", exc_info=True)


def register():
    for model, listener in (
        (HarvestGatherError, _on_gather_error),
        (HarvestObjectError, _on_object_error),
    ):
        if not event.contains(model, "after_insert", listener):
            event.listen(model, "after_insert", listener)

"""Endpoint entities task rules — config for the "endpoint gives its organisation
no entities" task (config#3002).

SWAPPABLE SEAM, same discipline as authoritativeness_task.py: these values are
hardcoded here because nothing in the specification defines a generated-task-rule
table yet. Shaped like the row such a table would hold, so promoting it later is a
change to the loader body, not to anything that calls it.

Nothing downstream may branch on these literals directly — read the whole rule via
load_endpoint_entities_task_rule() and apply it.
"""

from jobs.config.quality_dimensions import COMPLETENESS

# Why an endpoint gives its organisation no entities, carried in details.reason.
# The task is about the outcome, so it is raised whatever the reason; the reason
# is there so a reader can see which other task (if any) explains it.
# NO_RESOURCE: nothing has ever been collected from the endpoint.
# NO_ENTITIES: its current resource produced no entities for the dataset.
# OTHER_ORGANISATION: its current resource produced entities, but every one of
# them belongs to another organisation.
NO_RESOURCE = "no-resource"
NO_ENTITIES = "no-entities"
OTHER_ORGANISATION = "other-organisation"

ENDPOINT_ENTITIES_TASK_RULE = {
    "task_type": "endpoint_has_no_entities",
    "task_source": "endpoint",
    "severity": "warning",
    "responsibility": "external",
    "quality_dimension": COMPLETENESS,
    # Organisations whose endpoints are not checked. MHCLG's endpoints are seed
    # and national files whose entities belong to the councils, so nearly all of
    # them would be raised, and none is for a council to act on.
    "exempt_organisations": ["government-organisation:D1342"],
}

# Population toggle, as in authoritativeness_task.py: an abolished council can
# still have live source rows, but its entities have moved to its successor, so
# without this it would get a task nobody can close.
RESTRICT_TO_ACTIVE_ORGS = True


def load_endpoint_entities_task_rule():
    """SWAPPABLE SEAM: the fixed task-row values for the endpoint entities task.
    Body is a literal now; a future version reads a config-repo row instead."""
    rule = dict(ENDPOINT_ENTITIES_TASK_RULE)
    rule["exempt_organisations"] = list(rule["exempt_organisations"])
    return rule

"""Authoritativeness task rules — config for the "provide authoritative data" task.

SWAPPABLE SEAM, same discipline as quality_dimensions.py: this task's values are
hardcoded here because nothing in the specification defines a generated-task-rule
table yet. Shaped like the row such a table would hold, so promoting it later is a
change to the loader body, not to anything that calls it.

Nothing downstream may branch on these literals directly — read the whole rule via
load_authoritativeness_task_rule() and apply it.
"""

from jobs.config.quality_dimensions import AUTHORITATIVENESS

# One row, shaped like the future reference table: task_type -> the fixed
# severity/responsibility/task_source/quality_dimension it always emits.
AUTHORITATIVENESS_TASK_RULE = {
    "task_type": "provide_authoritative_data",
    "task_source": "provision",
    "severity": "error",
    "responsibility": "external",
    "quality_dimension": AUTHORITATIVENESS,
}

# Population toggle — separate from the rule above because it is pipeline
# behaviour (who gets evaluated), not a task-row value. Kept here so both
# concerns are edited in one place. An abolished org can still have live
# source rows, so without this an org nobody can act for would get a task
# it can never close.
RESTRICT_TO_ACTIVE_ORGS = True


def load_authoritativeness_task_rule():
    """SWAPPABLE SEAM: the fixed task-row values for the authoritativeness task.
    Body is a literal now; a future version reads a config-repo row instead."""
    return dict(AUTHORITATIVENESS_TASK_RULE)

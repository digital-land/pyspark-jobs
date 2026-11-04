"""Documentation URL task rules — config for the "endpoint is not linked from its
documentation page" task.

SWAPPABLE SEAM, same discipline as authoritativeness_task.py: these values are
hardcoded here because nothing in the specification defines a generated-task-rule
table yet. Shaped like the row such a table would hold, so promoting it later is a
change to the loader body, not to anything that calls it.

Nothing downstream may branch on these literals directly — read the whole rule via
load_documentation_url_task_rule() and apply it.
"""

from jobs.config.quality_dimensions import INTEGRITY

# NOT_FOUND: at least one documentation page was read, and none links to the
# endpoint. warning rather than error: a page that builds its links with
# JavaScript, or that serves a bot a different page, reads as not-found too.
# DOCUMENTATION_URL_GONE: every documentation page returned 404 or 410, so the
# page itself has been removed — usually because the data has moved.
# UNREACHABLE: no documentation page could be read for any other reason (a 403
# from bot protection, a timeout), so nothing is known about the link. notice,
# which provision_quality does not score.
NOT_FOUND = "not-found"
DOCUMENTATION_URL_GONE = "documentation-url-gone"
UNREACHABLE = "unreachable"

DOCUMENTATION_URL_TASK_RULE = {
    "task_type": "endpoint_url_in_documentation_url",
    "task_source": "documentation-url",
    "severity": {
        NOT_FOUND: "warning",
        DOCUMENTATION_URL_GONE: "warning",
        UNREACHABLE: "notice",
    },
    "responsibility": "external",
    # "Coherent and logical, with no broken links"
    "quality_dimension": INTEGRITY,
}


def load_documentation_url_task_rule():
    """SWAPPABLE SEAM: the fixed task-row values for the documentation URL task.
    Body is a literal now; a future version reads a config-repo row instead."""
    rule = dict(DOCUMENTATION_URL_TASK_RULE)
    rule["severity"] = dict(rule["severity"])
    return rule

"""Licence task rules — config for the "documentation page does not say the data
is under the Open Government Licence" task.

SWAPPABLE SEAM, same discipline as documentation_url_task.py: these values are
hardcoded here because nothing in the specification defines a generated-task-rule
table yet. Shaped like the row such a table would hold, so promoting it later is a
change to the loader body, not to anything that calls it.

Nothing downstream may branch on these literals directly — read the whole rule via
load_licence_task_rule() and apply it.
"""

from jobs.config.quality_dimensions import PROVENANCE

# OGL_FOUND: at least one of the endpoint's documentation pages mentions the Open
# Government Licence, in its body or its site-wide footer.
# OGL_NOT_FOUND: at least one documentation page was read, and none mentions it.
# warning rather than error: a page that builds its content with JavaScript reads
# as not-found too, and the licence statement is not yet a compliance blocker.
#
# A page that could not be read raises no licence task: nothing is known about its
# licence, and the endpoint_url_in_documentation_url task already reports it.
OGL_FOUND = "ogl-found"
OGL_NOT_FOUND = "ogl-not-found"

# The dataset groups, read from the specification's provision-rule.csv and
# provision.csv (jobs.pipeline.task._licence_scope):
# ODP: an organisation's provision of a dataset in the Open Digital Planning
# project, for the organisations in that project.
# MANDATED: a dataset that is statutory, or encouraged for local planning
# authorities, from any organisation.
# SINGLE_SOURCE: every other dataset with a provision rule, from any organisation.
ODP = "odp"
MANDATED = "mandated"
SINGLE_SOURCE = "single-source"

LICENCE_TASK_RULE = {
    "task_type": "ogl_in_documentation_url",
    "task_source": "documentation-url-licence",
    "severity": {
        OGL_NOT_FOUND: "warning",
    },
    "responsibility": "external",
    # "Metadata about where it came from and how it was made"
    "quality_dimension": PROVENANCE,
    # Which data is checked, by the groups the data quality reports use
    # (reporting-task measure_odp_mandated_data_quality.py and
    # measure_single_source_data_quality.py). Narrow by removing a group.
    "dataset_groups": [ODP, MANDATED, SINGLE_SOURCE],
}


def load_licence_task_rule():
    """SWAPPABLE SEAM: the fixed task-row values for the licence task.
    Body is a literal now; a future version reads a config-repo row instead."""
    rule = dict(LICENCE_TASK_RULE)
    rule["severity"] = dict(rule["severity"])
    rule["dataset_groups"] = list(rule["dataset_groups"])
    return rule

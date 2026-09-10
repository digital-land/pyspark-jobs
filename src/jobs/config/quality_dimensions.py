"""Quality dimensions — which aspect of provision quality a task speaks to.

Provision quality scores several dimensions rather than reducing everything to one
number, so every task has to say which one it contributes to. Each task speaks to
exactly one.

SWAPPABLE SEAM. The tables below are hardcoded deliberately: the vocabulary is still
being agreed and the specification does not define it yet. They are shaped like the
reference data they are expected to become, so moving them into the specification
later is a change to the loader bodies and to nothing that calls them.

Nothing downstream may branch on a dimension literal — group by the column and carry
whatever values appear — or adding a dimension becomes a code change rather than a row.
"""

CORRECTNESS = "correctness"
AUTHORITATIVENESS = "authoritativeness"

# The vocabulary, shaped like the reference dataset it is expected to become:
# (quality-dimension, name, description). `timeliness` is expected to join this
# later; it has no agreed definition yet so it is not listed. Nothing produces
# `authoritativeness` yet either — that arrives with the make-authoritative task.
QUALITY_DIMENSIONS = [
    (CORRECTNESS, "Correctness", "Whether the data has outstanding problems to fix"),
    (
        AUTHORITATIVENESS,
        "Authoritativeness",
        "Whether the data comes from the organisation responsible for it",
    ),
]

# issue-type.csv carries a finer-grained dimension per issue type. All five are
# statements about whether the data itself is right, so they roll up to correctness.
# A mapping rather than a default, so an unrecognised value stays visible as an
# unmapped task instead of being silently absorbed into correctness.
ISSUE_TYPE_DIMENSIONS = {
    "validity": CORRECTNESS,
    "accuracy": CORRECTNESS,
    "completeness": CORRECTNESS,
    "uniqueness": CORRECTNESS,
    "consistency": CORRECTNESS,
}

# The dimension is a property of the operation, not of the expect.csv row it is
# deployed in: a check means the same thing whichever dataset it runs against,
# whereas its severity legitimately varies per row. Only operations that can produce
# tasks — severity critical/error/warning — need an entry here.
EXPECTATION_DIMENSIONS = {
    "check_fields_required_after_plan_event": CORRECTNESS,
    "duplicate_name_check": CORRECTNESS,
    "name_is_a_code_check": CORRECTNESS,
    "name_is_a_placeholder_check": CORRECTNESS,
}

# A collection log task means the endpoint did not respond. That is arguably closer
# to timeliness than to correctness, but timeliness is not defined yet. The task table
# is rebuilt in full on every run, so reclassifying this later is a one-line change
# with no backfill.
LOG_DIMENSION = CORRECTNESS


def load_quality_dimensions():
    """SWAPPABLE SEAM: the quality dimension vocabulary, as
    (quality-dimension, name, description) triples."""
    return list(QUALITY_DIMENSIONS)


def load_issue_type_dimensions():
    """SWAPPABLE SEAM: issue-type.csv's fine-grained dimension to ours."""
    return dict(ISSUE_TYPE_DIMENSIONS)


def load_expectation_dimensions():
    """SWAPPABLE SEAM: expectation operation to dimension."""
    return dict(EXPECTATION_DIMENSIONS)

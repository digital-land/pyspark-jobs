"""Quality dimensions — which aspect of provision quality a task speaks to.

Provision quality scores several dimensions rather than reducing everything to one
number, so every task says which one it contributes to. Each task speaks to exactly
one dimension, or to none.

SWAPPABLE SEAM. The specification declares a `quality_dimension` field but no
reference dataset defining its values, so the vocabulary below is hardcoded. Where
the specification does hold the data we read it instead: issue-type.csv carries a
dimension per issue type and those values pass straight through untouched. Only the
mappings the specification has no home for are kept here.

Nothing downstream may branch on a dimension literal — group by the column and carry
whatever values appear — or adding a dimension becomes a code change rather than a row.
"""

VALIDITY = "validity"
ACCURACY = "accuracy"
COMPLETENESS = "completeness"
UNIQUENESS = "uniqueness"
CONSISTENCY = "consistency"
INTEGRITY = "integrity"
PROVENANCE = "provenance"
AUTHORITATIVENESS = "authoritativeness"
TIMELINESS = "timeliness"
CURRENT = "current"

# The agreed vocabulary, shaped like the reference dataset it would become:
# (quality-dimension, name, description). Five of these are already the values
# issue-type.csv carries. integrity, provenance and timeliness have no producer
# yet — the vocabulary was agreed ahead of the checks that will populate them.
QUALITY_DIMENSIONS = [
    (AUTHORITATIVENESS, "Authoritativeness", "Comes from the source of truth"),
    (VALIDITY, "Validity", "Meets data standards"),
    (UNIQUENESS, "Uniqueness", "No duplicates"),
    (COMPLETENESS, "Completeness", "All MUSTs exist, SHOULDs where expected"),
    (INTEGRITY, "Integrity", "Coherent and logical, with no broken links"),
    (
        PROVENANCE,
        "Provenance",
        "Metadata about where it came from and how it was made",
    ),
    (ACCURACY, "Accuracy", "A true reflection of reality"),
    (CONSISTENCY, "Consistency", "Recorded in the same way across the dataset"),
    (TIMELINESS, "Timeliness", "Available soon after the event"),
    (CURRENT, "Current", "Reflects the most recent changes"),
]

# The dimension is a property of the operation, not of the expect.csv row it is
# deployed in: a check means the same thing whichever dataset it runs against,
# whereas its severity legitimately varies per row. Only operations that can
# produce tasks — severity critical/error/warning — need an entry here.
#
# Each follows the precedent issue-type.csv sets: a malformed value is validity
# (`invalid date`, `invalid WKT`), a well-formed value that cannot be true of the
# real world is accuracy (`too large`, `WGS84 out of bounds`). A `name` is free
# text, so a code or a placeholder in it is well-formed but untrue — accuracy.
EXPECTATION_DIMENSIONS = {
    "check_fields_required_after_plan_event": COMPLETENESS,  # cf. `missing value`
    "duplicate_name_check": UNIQUENESS,  # cf. `reference values are not unique`
    "name_is_a_code_check": ACCURACY,
    "name_is_a_placeholder_check": ACCURACY,
}

# A collection log task means the endpoint did not respond, so the data cannot
# reflect anything the publisher has changed since it broke.
LOG_DIMENSION = CURRENT


def load_quality_dimensions():
    """SWAPPABLE SEAM: the quality dimension vocabulary, as
    (quality-dimension, name, description) triples."""
    return list(QUALITY_DIMENSIONS)


def load_expectation_dimensions():
    """SWAPPABLE SEAM: expectation operation to dimension."""
    return dict(EXPECTATION_DIMENSIONS)


def known_dimensions():
    """The agreed vocabulary, for checking values that arrive from the specification."""
    return {reference for reference, _, _ in QUALITY_DIMENSIONS}

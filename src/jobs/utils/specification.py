"""Reads of published specification reference data.

Fetched over HTTP at runtime rather than vendored, following the pattern already
used for issue-type.csv in the task pipeline. The specification owns these values;
nothing here decides what they mean.

NOTE the two different paths. quality.csv and severity.csv are hand-authored under
content/. provision.csv is GENERATED into specification/ by the spec repo's own
bin/provision.py — content/provision.csv is a 404.
"""

import csv
import logging
import urllib.request

logger = logging.getLogger(__name__)

_RAW = "https://raw.githubusercontent.com/digital-land/specification/main"
QUALITY_URL = f"{_RAW}/content/quality.csv"
SEVERITY_URL = f"{_RAW}/content/severity.csv"
PROVISION_URL = f"{_RAW}/specification/provision.csv"


def _fetch_rows(url):
    with urllib.request.urlopen(url) as response:
        lines = [line.decode("utf-8") for line in response.readlines()]
    return list(csv.DictReader(lines))


def load_quality_priorities():
    """quality.csv as {quality: priority} — the seven-level provision quality ladder.

    Priorities are read, never assumed: content/field/priority.md says verbatim
    "Do not depend on the absolute value as these values maybe renumbered".
    """
    priorities = {
        row["quality"]: int(row["priority"])
        for row in _fetch_rows(QUALITY_URL)
        if row.get("priority")
    }
    logger.info(f"specification: quality ladder {priorities}")
    return priorities


def load_severity_priorities():
    """severity.csv as {severity: priority}.

    LOWER IS MORE SEVERE — critical 1, error 2. So "error or worse" is
    `priority <= error's priority`, which reads backwards from the English.
    """
    priorities = {
        row["severity"]: int(row["priority"])
        for row in _fetch_rows(SEVERITY_URL)
        if row.get("priority")
    }
    logger.info(f"specification: severity priorities {priorities}")
    return priorities


def load_declared_provisions(spark):
    """provision.csv's active (dataset, organisation) pairs — the universe of
    provisions the specification says should exist.

    This is what makes rule 4, "no entities means no data", expressible at all:
    without it a provision with no endpoint and no entities has no row, so the
    table cannot distinguish "provided nothing" from "does not exist".

    Only the key is taken. provision-reason is deliberately NOT carried: measured
    2026-09-14 it answers the OBLIGATION question (statutory / expected /
    encouraged) for 98.5% of rows rather than the authority question, and where it
    does claim authority it contradicts our own classification on 44 of the 113
    rows that carry it. Authority stays single-sourced on
    owner_side_classification in authority.py.
    """
    rows = [
        row
        for row in _fetch_rows(PROVISION_URL)
        if not (row.get("end-date") or "").strip()
    ]
    pairs = sorted(
        {
            (row["dataset"], row["organisation"])
            for row in rows
            if row.get("dataset") and row.get("organisation")
        }
    )
    logger.info(f"specification: {len(pairs)} active declared provisions")
    return spark.createDataFrame(pairs, ["dataset", "organisation"])

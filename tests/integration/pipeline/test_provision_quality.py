"""
Integration tests for ProvisionQualityPipeline and its module-level helpers.

Uses a real Spark session and local filesystem for reads/writes.
"""

from datetime import date

from jobs.pipeline.authority import load_entity_quality
from jobs.pipeline.provision_quality import (
    _assign_quality,
    _build_dataset_quality,
    _build_organisation_quality,
    _build_provision_quality,
    _drop_blank_organisations,
    _provision_start_dates,
    _task_state,
)

from ._test_helpers import write_csv

PQ_DATASET = "conservation-area"
PQ_ADU = "local-authority:ADU"
PQ_LEW = "local-authority:LEW"
PQ_MHCLG = "government-organisation:MHCLG"
PQ_NEW = "local-authority:NEW"
PQ_DEC = "local-authority:DEC"

# specification/content/quality.csv. Hardcoded here because the pipeline reads it
# over HTTP at runtime; the values are read, never assumed, so a renumbering in the
# specification changes this fixture and nothing else.
QUALITY_PRIORITIES = {
    "none": 0,
    "some": 1,
    "indicative": 2,
    "verifiable": 3,
    "authoritative": 4,
    "usable": 5,
    "trustworthy": 6,
}
AUTHORITATIVE_PRIORITY = QUALITY_PRIORITIES["authoritative"]

# Explicit, because Spark cannot infer a schema from zero rows and "no tasks at
# all" is the case most worth testing.
TASK_STATE_SCHEMA = (
    "dataset string, organisation string, has_error_task int, task_count bigint"
)


def _task_state_df(spark, rows=()):
    """(dataset, organisation, has_error_task, task_count), as _task_state emits."""
    return spark.createDataFrame(list(rows), TASK_STATE_SCHEMA)


# specification/content/severity.csv. Hardcoded for the same reason as
# QUALITY_PRIORITIES: the pipeline reads it over HTTP at runtime. LOWER IS MORE
# SEVERE, so "error or worse" is `priority <= error`.
SEVERITY_PRIORITIES = {
    "critical": 1,
    "error": 2,
    "warning": 3,
    "notice": 4,
    "info": 5,
    "debug": 6,
}

# dataset, organisation, severity, responsibility, quality_dimension — the five
# columns load_tasks selects.
TASK_COLUMNS = [
    "dataset",
    "organisation",
    "severity",
    "responsibility",
    "quality_dimension",
]


def _task_row(severity="error", responsibility="external", dimension="validity"):
    return (PQ_DATASET, PQ_ADU, severity, responsibility, dimension)


def _task_state_for(spark, rows):
    """_task_state over the given task rows, scoring severities down to `warning`."""
    return _task_state(
        spark.createDataFrame(list(rows), TASK_COLUMNS),
        SEVERITY_PRIORITIES,
        SEVERITY_PRIORITIES["error"],
        SEVERITY_PRIORITIES["warning"],
    )


def _provision_quality_inputs(spark):
    """Conservation-area style scenario covering the provider/owner mismatches:
    - Adur  : active endpoint + owns authoritative entities            -> authoritative
    - Lewes : no endpoint, owns entities seeded on its behalf ('some') -> some
    - MHCLG : national seeder, has endpoint, owns nothing, seeded Lewes -> some
    - New LA: active endpoint but nothing arriving (kept + flagged, null)
    - Declared DC: in the specification's provision.csv only — no endpoint, no
      entities, nothing seeded. Exists solely to exercise the union.
    """
    providers_df = spark.createDataFrame(
        [
            (PQ_DATASET, PQ_ADU, True),
            (PQ_DATASET, PQ_MHCLG, True),
            (PQ_DATASET, PQ_NEW, None),  # endpoint configured, no resource arriving
        ],
        ["dataset", "organisation", "has_active_resource"],
    )
    org_df = spark.createDataFrame(
        [
            (PQ_ADU, "100", "Adur DC", True),
            (PQ_LEW, "200", "Lewes DC", True),
            (PQ_MHCLG, "300", "MHCLG", True),
            (PQ_NEW, "400", "New LA", True),
            (PQ_DEC, "500", "Declared DC", True),
        ],
        ["organisation", "organisation_entity", "organisation_name", "org_active"],
    )
    entity_org_df = spark.createDataFrame(
        [(PQ_DATASET, PQ_ADU), (PQ_DATASET, PQ_LEW)],  # designated provisions
        ["dataset", "organisation"],
    )
    lookup_df = spark.createDataFrame(
        [("3", PQ_MHCLG), ("4", PQ_MHCLG)],  # MHCLG seeded entities 3 & 4
        ["entity", "organisation"],
    )
    entity_quality_df = spark.createDataFrame(
        [
            (PQ_DATASET, "100", "authoritative", "1"),  # owned by Adur
            (PQ_DATASET, "100", "authoritative", "2"),  # owned by Adur
            (PQ_DATASET, "200", "some", "3"),  # owned by Lewes (seeded)
            (PQ_DATASET, "200", "some", "4"),  # owned by Lewes (seeded)
        ],
        ["dataset", "organisation_entity", "quality", "entity"],
    )
    declared_df = spark.createDataFrame(
        [
            (PQ_DATASET, PQ_ADU),  # already in the key set — must not duplicate
            (PQ_DATASET, PQ_LEW),
            (PQ_DATASET, PQ_DEC),  # declared, but has supplied nothing
        ],
        ["dataset", "organisation"],
    )
    # Pre-aggregated in execute(): one row per (dataset, organisation) that has
    # ever had a source row. Lewes and Declared DC are deliberately absent —
    # neither ever supplied through an endpoint of its own, so neither has a date.
    start_dates_df = spark.createDataFrame(
        [
            (PQ_DATASET, PQ_ADU, date(2021, 3, 4)),
            (PQ_DATASET, PQ_MHCLG, date(2022, 7, 1)),
            (PQ_DATASET, PQ_NEW, date(2024, 11, 12)),
        ],
        ["dataset", "organisation", "start_date"],
    )
    return (
        providers_df,
        org_df,
        entity_org_df,
        lookup_df,
        entity_quality_df,
        declared_df,
        start_dates_df,
    )


def _scored(spark, task_rows=()):
    """The base table with the ladder applied — what execute() writes."""
    return _assign_quality(
        _build_provision_quality(*_provision_quality_inputs(spark)),
        _task_state_df(spark, task_rows),
        QUALITY_PRIORITIES,
    )


def _scored_rows(spark, task_rows=()):
    return {r["organisation"]: r.asDict() for r in _scored(spark, task_rows).collect()}


class TestProvisionQuality:

    def test_flags_and_quality(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        rows = {r["organisation"]: r.asDict() for r in pq.collect()}

        assert set(rows) == {PQ_ADU, PQ_LEW, PQ_MHCLG, PQ_NEW, PQ_DEC}

        adu = rows[PQ_ADU]
        assert adu["has_active_endpoint"] is True
        assert adu["has_active_resource"] is True
        assert adu["owns_entities"] is True
        assert adu["is_designated_provider"] is True
        assert adu["entity_quality"] == "authoritative"
        assert adu["entity_count"] == 2

        lew = rows[PQ_LEW]
        assert lew["has_active_endpoint"] is False  # owns but never submitted
        assert lew["has_active_resource"] is False  # no endpoint, so nothing arriving
        assert lew["owns_entities"] is True
        assert lew["is_designated_provider"] is True
        assert lew["entity_quality"] == "some"
        assert lew["entity_count"] == 2

        mhclg = rows[PQ_MHCLG]
        assert mhclg["has_active_endpoint"] is True
        assert mhclg["has_active_resource"] is True
        assert mhclg["owns_entities"] is False  # provider that owns nothing
        assert mhclg["is_designated_provider"] is False
        assert mhclg["entity_quality"] == "some"  # via seeder detection
        assert mhclg["entity_count"] == 2  # seeded count

        new = rows[PQ_NEW]
        assert new["has_active_endpoint"] is True
        # endpoint is configured but its resource has stopped arriving
        assert new["has_active_resource"] is False
        assert new["owns_entities"] is False
        assert new["entity_quality"] is None  # endpoint but no data — kept + flagged
        assert new["entity_count"] == 0

        # Union-only: no endpoint, no entities, not designated, not a seeder.
        dec = rows[PQ_DEC]
        assert dec["has_active_endpoint"] is False
        assert dec["owns_entities"] is False
        assert dec["is_designated_provider"] is False
        assert dec["entity_quality"] is None
        assert dec["entity_count"] == 0

    def test_provision_start_dates_takes_earliest_including_ended_rows(self, spark):
        """The decision this column rests on: a provider whose URL changed has an
        end-dated source row and a live one, and the provision began at the older of
        the two. Counting only live rows would report a 2021 provider as starting in
        2025, which differs for 42% of provisions."""
        source_df = spark.createDataFrame(
            [
                # Adur replaced its URL — the 2021 row was end-dated, 2025 is live
                (
                    "https://adur.example/old.csv",
                    PQ_ADU,
                    PQ_DATASET,
                    "2021-03-04T09:00:00Z",
                ),
                (
                    "https://adur.example/new.csv",
                    PQ_ADU,
                    PQ_DATASET,
                    "2025-01-09T09:00:00Z",
                ),
                # one source row feeding two datasets via ';'-split pipelines
                (
                    "https://mhclg.example/a.csv",
                    PQ_MHCLG,
                    f"{PQ_DATASET};article-4-direction",
                    "2022-07-01T13:13:02Z",
                ),
                # no endpoint configured — not a provision at all
                ("", PQ_NEW, PQ_DATASET, "2020-01-01T00:00:00Z"),
                # endpoint but no entry-date — nothing to report
                ("https://new.example/a.csv", PQ_NEW, PQ_DATASET, None),
            ],
            ["endpoint", "organisation", "pipelines", "entry_date"],
        )

        rows = {
            (r["dataset"], r["organisation"]): r["start_date"]
            for r in _provision_start_dates(source_df).collect()
        }

        # the earlier, end-dated row wins
        assert rows[(PQ_DATASET, PQ_ADU)] == date(2021, 3, 4)

        # a ';'-joined pipelines value produces one row per dataset
        assert rows[(PQ_DATASET, PQ_MHCLG)] == date(2022, 7, 1)
        assert rows[("article-4-direction", PQ_MHCLG)] == date(2022, 7, 1)

        # a blank endpoint is not a provision, and a missing entry-date has no date
        assert (PQ_DATASET, PQ_NEW) not in rows
        assert len(rows) == 3

    def test_start_date_is_null_where_there_is_no_source_row(self, spark):
        """The date comes from source.csv, so only an organisation that supplied
        through an endpoint of its own has one. Lewes owns entities seeded on its
        behalf and Declared DC has supplied nothing, so neither is the provider and
        neither has a date to report — a blank here means "not the provider", not
        "has not started"."""
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        rows = {r["organisation"]: r.asDict() for r in pq.collect()}

        assert rows[PQ_ADU]["start_date"] == date(2021, 3, 4)
        assert rows[PQ_MHCLG]["start_date"] == date(2022, 7, 1)
        assert rows[PQ_NEW]["start_date"] == date(2024, 11, 12)

        assert rows[PQ_LEW]["start_date"] is None
        assert rows[PQ_DEC]["start_date"] is None

        # a left join onto the key set must not add or duplicate provisions
        assert pq.count() == 5

    def test_start_date_survives_scoring(self, spark):
        """_assign_quality re-selects every output column by name, so a new column is
        silently dropped unless it is named there too. That failure is quiet in the
        CSV and fatal at the Postgres write, whose column list comes from
        PROVISION_QUALITY_PG_TYPES."""
        rows = _scored_rows(spark)

        assert rows[PQ_ADU]["start_date"] == date(2021, 3, 4)
        assert rows[PQ_LEW]["start_date"] is None

    def test_dataset_quality_rollup(self, spark):
        ds = {
            r["dataset"]: r.asDict()
            for r in _build_dataset_quality(
                _scored(spark), AUTHORITATIVE_PRIORITY
            ).collect()
        }

        row = ds[PQ_DATASET]
        assert row["authoritative_organisations"] == 1  # Adur
        assert row["some_organisations"] == 4  # Lewes, MHCLG, New LA, Declared DC
        # Every row is scored now, so nothing is filtered out of the totals: the
        # denominator became "expected to provide" rather than "known to us".
        assert row["total_organisations"] == 5
        assert row["total_entities"] == 4  # owned counts only, no double count

    def test_rollup_counts_the_whole_authoritative_band(self, spark):
        """usable and trustworthy are authoritative provisions that happen to be
        cleaner, so the count must not move when Adur picks up an error task and
        drops from 'trustworthy' to 'authoritative'. An equality test on
        "authoritative" would have counted only the version with errors."""
        clean = _build_dataset_quality(
            _scored(spark), AUTHORITATIVE_PRIORITY
        ).collect()[0]
        with_error = _build_dataset_quality(
            _scored(spark, [(PQ_DATASET, PQ_ADU, 1, 1)]), AUTHORITATIVE_PRIORITY
        ).collect()[0]

        assert clean["authoritative_organisations"] == 1
        assert with_error["authoritative_organisations"] == 1

    def test_organisation_quality_rollup(self, spark):
        orgs = {
            r["organisation"]: r.asDict()
            for r in _build_organisation_quality(
                _scored(spark), AUTHORITATIVE_PRIORITY
            ).collect()
        }

        assert orgs[PQ_ADU]["authoritative_datasets"] == 1
        assert orgs[PQ_ADU]["total_entities_owned"] == 2

        assert orgs[PQ_MHCLG]["some_datasets"] == 1
        assert orgs[PQ_MHCLG]["authoritative_datasets"] == 0
        assert orgs[PQ_MHCLG]["total_entities_owned"] == 0  # seeder owns nothing

        # New LA used to be excluded for having a null quality. It now scores
        # 'none' and is counted — an organisation expected to provide and not
        # providing is exactly what this table should show.
        assert orgs[PQ_NEW]["some_datasets"] == 1
        assert orgs[PQ_NEW]["authoritative_datasets"] == 0

    def test_load_entity_quality_reads_flattened_csvs(self, spark, tmp_path):
        """load_entity_quality() reads the flattened per-dataset entity CSVs,
        tags dataset from the filename, aliases organisation-entity, and skips
        files missing the required columns. Also guards against the function
        being dropped (it once vanished in a refactor, breaking execute())."""
        entity_dir = tmp_path / "entity"
        write_csv(
            str(entity_dir / f"{PQ_DATASET}.csv"),
            ["entity", "organisation-entity", "quality"],
            [
                {
                    "entity": "1",
                    "organisation-entity": "100",
                    "quality": "authoritative",
                },
                {"entity": "2", "organisation-entity": "200", "quality": "some"},
            ],
        )
        # A file missing organisation-entity/quality must be skipped, not fail.
        write_csv(
            str(entity_dir / "no-quality.csv"),
            ["entity", "name"],
            [{"entity": "9", "name": "irrelevant"}],
        )
        df = load_entity_quality(spark, str(entity_dir))

        assert set(df.columns) == {
            "entity",
            "organisation_entity",
            "quality",
            "dataset",
        }
        rows = {r["entity"]: r.asDict() for r in df.collect()}
        assert set(rows) == {"1", "2"}  # no-quality.csv skipped
        assert rows["1"]["organisation_entity"] == "100"
        assert rows["1"]["quality"] == "authoritative"
        assert rows["1"]["dataset"] == PQ_DATASET

    def test_drop_blank_organisations(self, spark):
        # A blank or null organisation can't key the table (organisation is a
        # NOT NULL PK), so execute() drops these rows before writing.
        df = spark.createDataFrame(
            [
                (PQ_DATASET, PQ_ADU),
                (PQ_DATASET, ""),
                (PQ_DATASET, None),
            ],
            ["dataset", "organisation"],
        )
        result = {r["organisation"] for r in _drop_blank_organisations(df).collect()}
        assert result == {PQ_ADU}


class TestTaskState:
    """What reaches the score at all. The rung comes from `task_count > 0`, not
    from severity, so anything that survives this function moves a provision."""

    def test_notice_tasks_do_not_reach_the_score(self, spark):
        """★ The guard that lets the task table carry `notice` rows safely. ★

        Nothing consumes a notice task, but `_assign_quality` reads
        `task_count > 0` to decide the rung — so if a notice survived here, a
        provision whose ONLY tasks were notices would silently drop off the top
        of its band. Emitting no row at all leaves the left join null, which is
        the same as having no tasks.
        """
        state = _task_state_for(spark, [_task_row(severity="notice")])

        assert state.count() == 0

    def test_notice_does_not_inflate_task_count_alongside_a_real_task(self, spark):
        state = _task_state_for(
            spark,
            [
                _task_row(severity="error"),
                _task_row(severity="notice"),
                _task_row(severity="notice"),
            ],
        ).collect()

        assert len(state) == 1
        assert state[0]["task_count"] == 1, "only the error counts"
        assert state[0]["has_error_task"] == 1

    def test_warning_counts_but_is_not_an_error(self, spark):
        state = _task_state_for(spark, [_task_row(severity="warning")]).collect()

        assert len(state) == 1
        assert state[0]["task_count"] == 1
        assert state[0]["has_error_task"] == 0

    def test_critical_and_error_both_set_has_error_task(self, spark):
        for severity in ("critical", "error"):
            state = _task_state_for(spark, [_task_row(severity=severity)]).collect()
            assert state[0]["has_error_task"] == 1, severity

    def test_info_and_debug_do_not_reach_the_score(self, spark):
        state = _task_state_for(
            spark,
            [_task_row(severity="info"), _task_row(severity="debug")],
        )

        assert state.count() == 0

    def test_unrecognised_severity_does_not_reach_the_score(self, spark):
        """A severity absent from severity.csv has no priority, so it cannot be
        ranked — and something unrankable must not silently move a provision."""
        state = _task_state_for(spark, [_task_row(severity="not-a-severity")])

        assert state.count() == 0

    def test_authoritativeness_and_internal_are_still_excluded(self, spark):
        """Regression guard on the two exclusions that predate the severity
        filter — both must survive it."""
        state = _task_state_for(
            spark,
            [
                _task_row(dimension="authoritativeness"),
                _task_row(responsibility="internal"),
            ],
        )

        assert state.count() == 0

    def test_blank_organisation_is_still_excluded(self, spark):
        state = _task_state_for(
            spark, [(PQ_DATASET, "", "error", "external", "validity")]
        )

        assert state.count() == 0


class TestAssignQuality:
    """The seven-level ladder: authoritativeness picks the band, task state picks
    the rung inside it. Adur is authoritative (band base 4), Lewes and MHCLG are
    'some' (band base 1), New LA and Declared DC have no data at all."""

    def test_no_tasks_reaches_the_top_of_each_band(self, spark):
        rows = _scored_rows(spark)

        assert rows[PQ_ADU]["quality"] == "trustworthy"
        assert rows[PQ_ADU]["quality_score"] == 6.0
        assert rows[PQ_LEW]["quality"] == "verifiable"
        assert rows[PQ_LEW]["quality_score"] == 3.0

    def test_a_warning_task_drops_one_rung(self, spark):
        rows = _scored_rows(spark, [(PQ_DATASET, PQ_ADU, 0, 3)])

        assert rows[PQ_ADU]["quality"] == "usable"
        assert rows[PQ_ADU]["quality_score"] == 5.0

    def test_an_error_task_drops_to_the_bottom_of_the_band(self, spark):
        rows = _scored_rows(spark, [(PQ_DATASET, PQ_ADU, 1, 2)])

        assert rows[PQ_ADU]["quality"] == "authoritative"
        assert rows[PQ_ADU]["quality_score"] == 4.0

    def test_the_non_authoritative_band_moves_the_same_way(self, spark):
        """The offset is the same in both bands — which is the whole reason
        `indicative` and `verifiable` are reachable at all."""
        rows = _scored_rows(
            spark, [(PQ_DATASET, PQ_LEW, 1, 1), (PQ_DATASET, PQ_MHCLG, 0, 1)]
        )

        assert rows[PQ_LEW]["quality"] == "some"
        assert rows[PQ_MHCLG]["quality"] == "indicative"

    def test_no_data_stays_at_the_bottom_whatever_the_tasks(self, spark):
        """An organisation that has supplied nothing cannot be lifted by the task
        state, in either direction — no tasks must not read as 'clean'."""
        with_error = _scored_rows(spark, [(PQ_DATASET, PQ_NEW, 1, 5)])
        without = _scored_rows(spark)

        assert with_error[PQ_NEW]["quality"] == "none"
        assert with_error[PQ_NEW]["quality_score"] == 0.0
        assert without[PQ_NEW]["quality"] == "none"
        assert without[PQ_NEW]["quality_score"] == 0.0

    def test_declared_only_provision_scores_no_data(self, spark):
        """The point of unioning provision.csv: a provision the specification says
        should exist, which has supplied nothing, gets a row and scores 'none'.
        Without the union it has no row at all, so the table cannot tell
        "provided nothing" from "does not exist"."""
        rows = _scored_rows(spark)

        assert PQ_DEC in rows
        assert rows[PQ_DEC]["quality"] == "none"
        assert rows[PQ_DEC]["quality_score"] == 0.0
        assert rows[PQ_DEC]["organisation_name"] == "Declared DC"

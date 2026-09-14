"""
Integration tests for jobs.pipeline.authority — the shared authoritativeness
classification used by both TaskPipeline and ProvisionQualityPipeline.

Uses a real Spark session. See tests/integration/pipeline/test_provision_quality.py
for load_entity_quality's own tests (reading the flattened per-dataset entity
CSVs), unchanged by its move into this module.
"""

from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from jobs.pipeline.authority import (
    join_entity_quality_to_org,
    live_datasets,
    non_authoritative_providers,
    owner_side_classification,
)

# Explicit schemas rather than createDataFrame(rows, [col, ...]) — several
# tests below exercise an empty-rows case, and schema inference from an
# empty list raises CANNOT_INFER_EMPTY_SCHEMA.
ENTITY_QUALITY_SCHEMA = StructType(
    [
        StructField("entity", StringType(), True),
        StructField("organisation_entity", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("dataset", StringType(), True),
    ]
)
ORG_SCHEMA = StructType(
    [
        StructField("organisation", StringType(), True),
        StructField("organisation_entity", StringType(), True),
        StructField("org_active", BooleanType(), True),
    ]
)
ENTITY_ORG_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), True),
        StructField("organisation", StringType(), True),
    ]
)
LIVE_DATASETS_SCHEMA = StructType([StructField("dataset", StringType(), True)])
DATASET_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("end_date", StringType(), True),
    ]
)


def _entity_quality_df(spark, rows):
    return spark.createDataFrame(rows, schema=ENTITY_QUALITY_SCHEMA)


def _org_df(spark, rows):
    return spark.createDataFrame(rows, schema=ORG_SCHEMA)


def _entity_org_df(spark, rows):
    return spark.createDataFrame(rows, schema=ENTITY_ORG_SCHEMA)


def _live_datasets_df(spark, datasets):
    return spark.createDataFrame([(d,) for d in datasets], schema=LIVE_DATASETS_SCHEMA)


def _dataset_df(spark, rows):
    return spark.createDataFrame(rows, schema=DATASET_SCHEMA)


class TestOwnerSideClassification:

    def test_authoritative_when_any_owned_entity_is_authoritative(self, spark):
        eq = join_entity_quality_to_org(
            _entity_quality_df(
                spark,
                [
                    ("1", "100", "some", "conservation-area"),
                    ("2", "100", "authoritative", "conservation-area"),
                ],
            ),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
        )
        row = owner_side_classification(eq).collect()[0]
        assert row["owner_quality"] == "authoritative"
        assert row["owned_entity_count"] == 2

    def test_some_when_no_owned_entity_is_authoritative(self, spark):
        eq = join_entity_quality_to_org(
            _entity_quality_df(spark, [("1", "100", "some", "conservation-area")]),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
        )
        row = owner_side_classification(eq).collect()[0]
        assert row["owner_quality"] == "some"

    def test_absent_org_produces_no_row(self, spark):
        """An org owning nothing never appears here — this is a groupBy, not
        a left join against every org. non_authoritative_providers handles
        that case via its own left join against designation."""
        eq = join_entity_quality_to_org(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
        )
        assert owner_side_classification(eq).count() == 0


class TestNonAuthoritativeProviders:

    def test_designated_provider_owning_nothing_gets_quality_none(self, spark):
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(spark, [("conservation-area", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        row = result.collect()[0]
        assert row["quality"] == "none"
        assert row["owned_entity_count"] == 0

    def test_designated_provider_owning_only_some_stays_in_population(self, spark):
        result = non_authoritative_providers(
            _entity_quality_df(spark, [("1", "100", "some", "conservation-area")]),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(spark, [("conservation-area", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        row = result.collect()[0]
        assert row["quality"] == "some"
        assert row["owned_entity_count"] == 1

    def test_authoritative_provider_is_excluded(self, spark):
        """The whole point: an org that already owns authoritative data must
        never get this task."""
        result = non_authoritative_providers(
            _entity_quality_df(
                spark, [("1", "100", "authoritative", "conservation-area")]
            ),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(spark, [("conservation-area", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        assert result.count() == 0

    def test_non_designated_owner_is_excluded(self, spark):
        """Owns nothing, and isn't designated for this dataset either — not
        this org's problem, no task."""
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(spark, []),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        assert result.count() == 0

    def test_inactive_org_excluded_when_restricted(self, spark):
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", False)]),
            _entity_org_df(spark, [("conservation-area", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        assert result.count() == 0

    def test_inactive_org_included_when_not_restricted(self, spark):
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", False)]),
            _entity_org_df(spark, [("conservation-area", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=False,
        )
        assert result.count() == 1

    def test_designated_provider_for_a_non_live_dataset_gets_no_task(self, spark):
        """Designation config outlives the dataset. A provider still designated
        for a dataset the platform no longer builds must produce no task —
        without this filter, retired datasets alone produced ~580 tasks in
        production that nobody could act on."""
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(spark, [("retired-dataset", "local-authority:BRO")]),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        assert result.count() == 0

    def test_live_and_non_live_designations_are_separated(self, spark):
        """The filter must drop only the non-live dataset, not the whole
        population — guards against an over-broad join killing real tasks."""
        result = non_authoritative_providers(
            _entity_quality_df(spark, []),
            _org_df(spark, [("local-authority:BRO", "100", True)]),
            _entity_org_df(
                spark,
                [
                    ("conservation-area", "local-authority:BRO"),
                    ("retired-dataset", "local-authority:BRO"),
                ],
            ),
            _live_datasets_df(spark, ["conservation-area"]),
            restrict_to_active_orgs=True,
        )
        rows = result.collect()
        assert [r["dataset"] for r in rows] == ["conservation-area"]

    def test_agrees_with_owner_side_classification_on_the_same_input(self, spark):
        """Consistency guard: an org owner_side_classification calls
        authoritative must never also appear in non_authoritative_providers'
        output for the same (dataset, organisation) — makes the "one
        definition" property explicit rather than only implicit in the code
        calling the same function internally."""
        entity_quality = _entity_quality_df(
            spark,
            [
                ("1", "100", "authoritative", "conservation-area"),
                ("2", "200", "some", "conservation-area"),
            ],
        )
        org_df = _org_df(
            spark,
            [
                ("local-authority:BRO", "100", True),
                ("local-authority:LBH", "200", True),
            ],
        )
        entity_org_df = _entity_org_df(
            spark,
            [
                ("conservation-area", "local-authority:BRO"),
                ("conservation-area", "local-authority:LBH"),
            ],
        )

        eq = join_entity_quality_to_org(entity_quality, org_df)
        owner = {
            r["organisation"]: r["owner_quality"]
            for r in owner_side_classification(eq).collect()
        }
        non_authoritative = {
            r["organisation"]
            for r in non_authoritative_providers(
                entity_quality,
                org_df,
                entity_org_df,
                _live_datasets_df(spark, ["conservation-area"]),
                restrict_to_active_orgs=True,
            ).collect()
        }

        assert owner["local-authority:BRO"] == "authoritative"
        assert "local-authority:BRO" not in non_authoritative
        assert owner["local-authority:LBH"] == "some"
        assert "local-authority:LBH" in non_authoritative


class TestLiveDatasets:
    """Both excluded cases here are real: development-plan-document is a
    production dataset end-dated in February, and local-plan-housing has a
    blank environment. Between them they accounted for 578 of the bogus tasks
    this filter removes."""

    def test_end_dated_dataset_is_excluded(self, spark):
        dataset_df = _dataset_df(
            spark,
            [
                ("conservation-area", "production", ""),
                ("development-plan-document", "production", "2026-02-03"),
            ],
        )
        result = {
            r["dataset"] for r in live_datasets(dataset_df, "production").collect()
        }
        assert result == {"conservation-area"}

    def test_blank_environment_is_built_nowhere(self, spark):
        dataset_df = _dataset_df(
            spark,
            [
                ("conservation-area", "production", ""),
                ("local-plan-housing", "", ""),
            ],
        )
        for env in ("production", "staging", "development"):
            result = {r["dataset"] for r in live_datasets(dataset_df, env).collect()}
            assert result == {"conservation-area"}, f"failed for env={env}"

    def test_environment_tiering(self, spark):
        """production everywhere, staging in staging+development,
        development only in development."""
        dataset_df = _dataset_df(
            spark,
            [
                ("prod-ds", "production", ""),
                ("staging-ds", "staging", ""),
                ("dev-ds", "development", ""),
            ],
        )

        def live_in(env):
            return {r["dataset"] for r in live_datasets(dataset_df, env).collect()}

        assert live_in("production") == {"prod-ds"}
        assert live_in("staging") == {"prod-ds", "staging-ds"}
        assert live_in("development") == {"prod-ds", "staging-ds", "dev-ds"}

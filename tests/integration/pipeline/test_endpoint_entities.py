"""Tests for jobs.pipeline.endpoint_entities — which active endpoints give their
organisation no entities (config#3002)."""

import pytest

from jobs.config.endpoint_entities_task import (
    NO_ENTITIES,
    NO_RESOURCE,
    OTHER_ORGANISATION,
    load_endpoint_entities_task_rule,
)
from jobs.pipeline.endpoint_entities import (
    UNCONFIRMED,
    endpoint_datasets,
    endpoints_without_entities,
)

ORGS = [
    ("local-authority:AAA", "1", True),
    ("local-authority:BBB", "2", True),
    ("local-authority:OLD", "3", False),
    ("government-organisation:D1342", "4", True),
]


def _source_df(spark, rows):
    return spark.createDataFrame(
        rows, "endpoint string, pipelines string, organisation string"
    )


def _run(
    spark,
    sources,
    current_resources=(),
    facts=(),
    logged_zero=(),
    owners=(),
    live=("dataset-a", "dataset-b"),
    restrict_to_active_orgs=True,
):
    """sources: (endpoint, pipelines, organisation); current_resources:
    (endpoint, resource); facts: (dataset, resource, entity); logged_zero:
    (dataset, resource) whose dataset-resource log says 0 entities; owners:
    (dataset, entity, organisation_entity)."""
    result = endpoints_without_entities(
        endpoint_datasets(_source_df(spark, list(sources))),
        spark.createDataFrame(
            list(current_resources), "endpoint string, resource string"
        ),
        spark.createDataFrame(
            list(facts), "dataset string, resource string, entity string"
        ),
        spark.createDataFrame(list(logged_zero), "dataset string, resource string"),
        spark.createDataFrame(
            [(d, e, o, "some") for d, e, o in owners],
            "dataset string, entity string, organisation_entity string, "
            "quality string",
        ),
        spark.createDataFrame(
            ORGS, "organisation string, organisation_entity string, org_active boolean"
        ),
        spark.createDataFrame([(d,) for d in live], "dataset string"),
        load_endpoint_entities_task_rule(),
        restrict_to_active_orgs,
    )
    return {
        (r["endpoint"], r["dataset"], r["organisation"]): r for r in result.collect()
    }


class TestEndpointDatasets:
    def test_one_row_per_dataset_an_endpoint_serves(self, spark):
        rows = endpoint_datasets(
            _source_df(spark, [("e1", "dataset-a;dataset-b", "local-authority:AAA")])
        ).collect()
        assert sorted(r["dataset"] for r in rows) == ["dataset-a", "dataset-b"]

    def test_one_row_per_organisation_sharing_an_endpoint(self, spark):
        rows = endpoint_datasets(
            _source_df(
                spark,
                [
                    ("e1", "dataset-a", "local-authority:AAA"),
                    ("e1", "dataset-a", "local-authority:BBB"),
                ],
            )
        ).collect()
        assert sorted(r["organisation"] for r in rows) == [
            "local-authority:AAA",
            "local-authority:BBB",
        ]

    @pytest.mark.parametrize("pipelines", ["", None])
    def test_source_row_with_no_dataset_is_dropped(self, spark, pipelines):
        rows = endpoint_datasets(
            _source_df(spark, [("e1", pipelines, "local-authority:AAA")])
        ).collect()
        assert rows == []


class TestEndpointsWithoutEntities:
    def test_endpoint_that_owns_an_entity_is_not_raised(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            facts=[("dataset-a", "r1", "100"), ("dataset-a", "r1", "101")],
            owners=[("dataset-a", "100", "1"), ("dataset-a", "101", "2")],
        )
        assert result == {}

    def test_entities_owned_by_another_organisation_are_raised(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            facts=[("dataset-a", "r1", "100"), ("dataset-a", "r1", "101")],
            owners=[("dataset-a", "100", "2"), ("dataset-a", "101", "2")],
        )
        row = result[("e1", "dataset-a", "local-authority:AAA")]
        assert row["reason"] == OTHER_ORGANISATION
        assert row["resource"] == "r1"
        assert row["entity_count"] == 2
        assert row["credited_to"] == ["local-authority:BBB"]

    def test_resource_logged_with_no_entities_is_raised(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            logged_zero=[("dataset-a", "r1")],
        )
        row = result[("e1", "dataset-a", "local-authority:AAA")]
        assert row["reason"] == NO_ENTITIES
        assert row["entity_count"] == 0
        assert row["credited_to"] == []

    def test_endpoint_never_collected_is_raised(self, spark):
        result = _run(spark, sources=[("e1", "dataset-a", "local-authority:AAA")])
        row = result[("e1", "dataset-a", "local-authority:AAA")]
        assert row["reason"] == NO_RESOURCE
        assert row["resource"] is None

    def test_only_the_current_resource_counts(self, spark):
        """Entities from an older resource of the endpoint do not count."""
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r2")],
            facts=[("dataset-a", "r1", "100")],
            logged_zero=[("dataset-a", "r2")],
            owners=[("dataset-a", "100", "1")],
        )
        assert result[("e1", "dataset-a", "local-authority:AAA")]["reason"] == (
            NO_ENTITIES
        )

    def test_each_dataset_of_an_endpoint_is_checked_on_its_own(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a;dataset-b", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            facts=[("dataset-a", "r1", "100")],
            logged_zero=[("dataset-b", "r1")],
            owners=[("dataset-a", "100", "1")],
        )
        assert list(result) == [("e1", "dataset-b", "local-authority:AAA")]

    def test_shared_resource_raises_only_for_the_organisation_without_entities(
        self, spark
    ):
        result = _run(
            spark,
            sources=[
                ("e1", "dataset-a", "local-authority:AAA"),
                ("e2", "dataset-a", "local-authority:BBB"),
            ],
            current_resources=[("e1", "r1"), ("e2", "r1")],
            facts=[("dataset-a", "r1", "100")],
            owners=[("dataset-a", "100", "1")],
        )
        assert list(result) == [("e2", "dataset-a", "local-authority:BBB")]
        assert result[("e2", "dataset-a", "local-authority:BBB")]["credited_to"] == [
            "local-authority:AAA"
        ]

    def test_one_endpoint_of_several_with_nothing_is_raised(self, spark):
        """The per-endpoint case provision_quality cannot see: the organisation
        owns entities through e1, but e2 gives it none."""
        result = _run(
            spark,
            sources=[
                ("e1", "dataset-a", "local-authority:AAA"),
                ("e2", "dataset-a", "local-authority:AAA"),
            ],
            current_resources=[("e1", "r1"), ("e2", "r2")],
            facts=[("dataset-a", "r1", "100")],
            logged_zero=[("dataset-a", "r2")],
            owners=[("dataset-a", "100", "1")],
        )
        assert list(result) == [("e2", "dataset-a", "local-authority:AAA")]

    def test_dataset_not_live_is_not_raised(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-c", "local-authority:AAA")],
        )
        assert result == {}

    def test_exempt_organisation_is_not_raised(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "government-organisation:D1342")],
        )
        assert result == {}

    def test_inactive_organisation_is_not_raised(self, spark):
        result = _run(spark, sources=[("e1", "dataset-a", "local-authority:OLD")])
        assert result == {}

    def test_inactive_organisation_is_raised_when_not_restricted(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:OLD")],
            restrict_to_active_orgs=False,
        )
        assert list(result) == [("e1", "dataset-a", "local-authority:OLD")]

    def test_resource_not_yet_processed_is_unconfirmed(self, spark):
        """No facts and no log saying 0: the resource may just not have been
        processed yet, so it is not called empty (the dev false positives)."""
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
        )
        assert result[("e1", "dataset-a", "local-authority:AAA")]["reason"] == (
            UNCONFIRMED
        )

    def test_zero_logged_for_another_dataset_does_not_count(self, spark):
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            logged_zero=[("dataset-b", "r1")],
        )
        assert result[("e1", "dataset-a", "local-authority:AAA")]["reason"] == (
            UNCONFIRMED
        )

    def test_entities_with_no_known_owner_are_unconfirmed(self, spark):
        """Facts whose entities have no owner in the entity files cannot be
        said to belong to another organisation."""
        result = _run(
            spark,
            sources=[("e1", "dataset-a", "local-authority:AAA")],
            current_resources=[("e1", "r1")],
            facts=[("dataset-a", "r1", "100")],
        )
        row = result[("e1", "dataset-a", "local-authority:AAA")]
        assert row["reason"] == UNCONFIRMED
        assert row["credited_to"] == []

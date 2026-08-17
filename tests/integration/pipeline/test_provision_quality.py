"""
Integration tests for ProvisionQualityPipeline and its module-level helpers.

Uses a real Spark session and local filesystem for reads/writes.
"""

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.provision_quality import (
    ProvisionQualityPipeline,
    _build_dataset_quality,
    _build_organisation_quality,
    _build_provision_quality,
    _drop_blank_organisations,
)

from ._test_helpers import write_csv

PQ_DATASET = "conservation-area"
PQ_ADU = "local-authority:ADU"
PQ_LEW = "local-authority:LEW"
PQ_MHCLG = "government-organisation:MHCLG"
PQ_NEW = "local-authority:NEW"


def _provision_quality_inputs(spark):
    """Conservation-area style scenario covering the provider/owner mismatches:
    - Adur  : active endpoint + owns authoritative entities            -> authoritative
    - Lewes : no endpoint, owns entities seeded on its behalf ('some') -> some
    - MHCLG : national seeder, has endpoint, owns nothing, seeded Lewes -> some
    - New LA: active endpoint but nothing arriving (kept + flagged, null)
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
    return providers_df, org_df, entity_org_df, lookup_df, entity_quality_df


class TestProvisionQuality:

    def test_flags_and_quality(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        rows = {r["organisation"]: r.asDict() for r in pq.collect()}

        assert set(rows) == {PQ_ADU, PQ_LEW, PQ_MHCLG, PQ_NEW}

        adu = rows[PQ_ADU]
        assert adu["has_active_endpoint"] is True
        assert adu["has_active_resource"] is True
        assert adu["owns_entities"] is True
        assert adu["is_designated_provider"] is True
        assert adu["quality"] == "authoritative"
        assert adu["entity_count"] == 2

        lew = rows[PQ_LEW]
        assert lew["has_active_endpoint"] is False  # owns but never submitted
        assert lew["has_active_resource"] is False  # no endpoint, so nothing arriving
        assert lew["owns_entities"] is True
        assert lew["is_designated_provider"] is True
        assert lew["quality"] == "some"
        assert lew["entity_count"] == 2

        mhclg = rows[PQ_MHCLG]
        assert mhclg["has_active_endpoint"] is True
        assert mhclg["has_active_resource"] is True
        assert mhclg["owns_entities"] is False  # provider that owns nothing
        assert mhclg["is_designated_provider"] is False
        assert mhclg["quality"] == "some"  # via seeder detection
        assert mhclg["entity_count"] == 2  # seeded count

        new = rows[PQ_NEW]
        assert new["has_active_endpoint"] is True
        # endpoint is configured but its resource has stopped arriving
        assert new["has_active_resource"] is False
        assert new["owns_entities"] is False
        assert new["quality"] is None  # endpoint but no data — kept + flagged
        assert new["entity_count"] == 0

    def test_dataset_quality_rollup(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        ds = {r["dataset"]: r.asDict() for r in _build_dataset_quality(pq).collect()}

        row = ds[PQ_DATASET]
        assert row["authoritative_organisations"] == 1  # Adur
        assert row["some_organisations"] == 2  # Lewes, MHCLG
        assert row["total_organisations"] == 3  # New LA excluded (quality null)
        assert row["total_entities"] == 4  # owned counts only, no double count

    def test_organisation_quality_rollup(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        orgs = {
            r["organisation"]: r.asDict()
            for r in _build_organisation_quality(pq).collect()
        }

        assert orgs[PQ_ADU]["authoritative_datasets"] == 1
        assert orgs[PQ_ADU]["total_entities_owned"] == 2

        assert orgs[PQ_MHCLG]["some_datasets"] == 1
        assert orgs[PQ_MHCLG]["authoritative_datasets"] == 0
        assert orgs[PQ_MHCLG]["total_entities_owned"] == 0  # seeder owns nothing

        assert PQ_NEW not in orgs  # quality null -> excluded from rollup

    def test_load_entity_quality_reads_flattened_csvs(self, spark, tmp_path):
        """load_entity_quality() reads the flattened per-dataset entity CSVs,
        tags dataset from the filename, aliases organisation-entity, and skips
        files missing the required columns. Also guards against the method
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

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{tmp_path}/",
            parquet_datasets_path=str(tmp_path / "parquet-output/"),
        )
        df = ProvisionQualityPipeline(config).load_entity_quality(
            spark, str(entity_dir)
        )

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

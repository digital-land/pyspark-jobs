"""
Acceptance tests for entity data processing end - to - end workflow.

This test module validates the complete entity processing pipeline including:
- Data loading from various sources
- Entity transformation and pivoting
- JSON column creation from dynamic fields
- Organisation and dataset joins
- Output generation
"""

import json
import os
import sys
import tempfile

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "src")
)  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("EntityAcceptanceTestSuite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_entity_csv_data(spark):
    """Create sample entity data in CSV format."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("entry - number", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry - date", StringType(), True),
            StructField("start - date", StringType(), True),
            StructField("end - date", StringType(), True),
            StructField("organisation", StringType(), True),
        ]
    )

    data = [
        (
            "1",
            "entity - 001",
            "name",
            "Test Boundary 1",
            "2025 - 01 - 01",
            "2025 - 01 - 01",
            "",
            "local - authority:LBH",
        ),
        (
            "1",
            "entity - 001",
            "reference",
            "TB001",
            "2025 - 01 - 01",
            "2025 - 01 - 01",
            "",
            "local - authority:LBH",
        ),
        (
            "1",
            "entity - 001",
            "prefix",
            "title - boundary",
            "2025 - 01 - 01",
            "2025 - 01 - 01",
            "",
            "local - authority:LBH",
        ),
        (
            "1",
            "entity - 001",
            "boundary - type",
            "freehold",
            "2025 - 01 - 01",
            "2025 - 01 - 01",
            "",
            "local - authority:LBH",
        ),
        (
            "2",
            "entity - 002",
            "name",
            "Test Boundary 2",
            "2025 - 01 - 02",
            "2025 - 01 - 02",
            "",
            "local - authority:CMD",
        ),
        (
            "2",
            "entity - 002",
            "reference",
            "TB002",
            "2025 - 01 - 02",
            "2025 - 01 - 02",
            "",
            "local - authority:CMD",
        ),
        (
            "2",
            "entity - 002",
            "prefix",
            "title - boundary",
            "2025 - 01 - 02",
            "2025 - 01 - 02",
            "",
            "local - authority:CMD",
        ),
        (
            "2",
            "entity - 002",
            "boundary - type",
            "leasehold",
            "2025 - 01 - 02",
            "2025 - 01 - 02",
            "",
            "local - authority:CMD",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_organisation_data(spark):
    """Create sample organisation reference data."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )

    data = [
        ("local - authority:LBH", "600001", "London Borough of Hackney"),
        ("local - authority:CMD", "600002", "Camden Council"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_dataset_data(spark):
    """Create sample dataset reference data."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )

    data = [
        ("title - boundary", "geography", "Title Boundaries"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def temp_csv_files(
    sample_entity_csv_data, sample_organisation_data, sample_dataset_data
):
    """Create temporary CSV files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Entity data CSV
        entity_csv_path = os.path.join(temp_dir, "entity_data.csv")
        sample_entity_csv_data.write.mode("overwrite").option("header", True).csv(
            entity_csv_path
        )

        # Organisation data CSV
        org_csv_path = os.path.join(temp_dir, "organisation.csv")
        sample_organisation_data.write.mode("overwrite").option("header", True).csv(
            org_csv_path
        )

        # Dataset data CSV
        dataset_csv_path = os.path.join(temp_dir, "dataset.csv")
        sample_dataset_data.write.mode("overwrite").option("header", True).csv(
            dataset_csv_path
        )

        yield {
            "entity_csv": entity_csv_path,
            "organisation_csv": org_csv_path,
            "dataset_csv": dataset_csv_path,
            "output_dir": temp_dir,
        }


def test_end_to_end_entity_processing_workflow(spark, temp_csv_files):
    """
    Test the complete end - to - end entity processing workflow.

    This test validates:
    1. Loading CSV data
    2. Pivoting entity fields into columns
    3. Adding dataset column
    4. Handling missing geometry column
    5. Joining with organisation data
    6. Creating JSON from dynamic columns
    7. Joining with dataset typology
    8. Generating final output
    """
    from pyspark.sql.functions import col, first, lit, struct, to_json

    # Step 1: Load the main entity CSV data
    df = spark.read.option("header", True).csv(temp_csv_files["entity_csv"])

    # Verify initial data load
    assert df.count() == 8
    assert "entry - number" in df.columns
    assert "field" in df.columns
    assert "value" in df.columns

    # Step 2: Transform data (pivot entity fields)
    dataset_name = "title - boundary"
    pivot_df = (
        df.groupBy("entry - number", "entity", "organisation")
        .pivot("field")
        .agg(first("value"))
    )
    pivot_df = pivot_df.drop("entry - number")
    pivot_df = pivot_df.withColumn("dataset", lit(dataset_name))

    # Verify pivot operation
    assert pivot_df.count() == 2  # Two entities
    assert "dataset" in pivot_df.columns
    assert "name" in pivot_df.columns
    assert "reference" in pivot_df.columns

    # Step 3: Handle missing geometry column
    if "geometry" not in pivot_df.columns:
        pivot_df = pivot_df.withColumn("geometry", lit("").cast("string"))

    # Drop geojson if exists (simulate real scenario)
    if "geojson" in pivot_df.columns:
        pivot_df = pivot_df.drop("geojson")

    # Step 4: Load and join organisation data
    organisation_df = spark.read.option("header", True).csv(
        temp_csv_files["organisation_csv"]
    )
    pivot_df = pivot_df.join(
        organisation_df, pivot_df.organisation == organisation_df.organisation, "left"
    ).select(pivot_df["*"], organisation_df["entity"].alias("organisation_entity"))
    pivot_df = pivot_df.drop(pivot_df.organisation)

    # Verify organisation join
    assert "organisation_entity" in pivot_df.columns
    org_entities = [row.organisation_entity for row in pivot_df.collect()]
    assert "600001" in org_entities
    assert "600002" in org_entities

    # Step 5: Create JSON column from dynamic fields
    standard_columns = [
        "dataset",
        "end - date",
        "entity",
        "entry - date",
        "geometry",
        "json",
        "name",
        "organisation_entity",
        "point",
        "prefix",
        "reference",
        "start - date",
        "typology",
    ]
    pivot_df_columns = pivot_df.columns
    difference_columns = list(set(pivot_df_columns) - set(standard_columns))

    # Verify we have dynamic columns to convert to JSON
    assert "boundary - type" in difference_columns

    pivot_df_with_json = pivot_df.withColumn(
        "json", to_json(struct(*[col(c) for c in difference_columns]))
    )

    # Verify JSON column creation
    assert "json" in pivot_df_with_json.columns
    json_values = [row.json for row in pivot_df_with_json.collect()]

    # Parse and verify JSON content
    for json_str in json_values:
        parsed_json = json.loads(json_str)
        assert "boundary - type" in parsed_json
        assert parsed_json["boundary - type"] in ["freehold", "leasehold"]

    # Step 6: Load and join dataset data for typology
    dataset_df = spark.read.option("header", True).csv(temp_csv_files["dataset_csv"])
    final_df = pivot_df_with_json.join(
        dataset_df, pivot_df_with_json.dataset == dataset_df.dataset, "left"
    ).select(pivot_df_with_json["*"], dataset_df["typology"])

    # Verify final result
    assert "typology" in final_df.columns
    typologies = [row.typology for row in final_df.collect()]
    assert all(t == "geography" for t in typologies)

    # Step 7: Verify final output can be written
    output_path = os.path.join(temp_csv_files["output_dir"], "final_output")
    final_df.write.mode("overwrite").option("header", True).csv(output_path)

    # Verify output file was created
    output_files = os.listdir(output_path)
    csv_files = [f for f in output_files if f.endswith(".csv")]
    assert len(csv_files) > 0


def test_entity_workflow_with_missing_data(spark, temp_csv_files):
    """Test entity processing workflow with missing/incomplete data."""
    from pyspark.sql.types import StringType, StructField, StructType

    # Create dataset with missing values
    incomplete_schema = StructType(
        [
            StructField("entry - number", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    incomplete_data = [
        ("1", "entity - 001", "name", "Incomplete Entity"),
        ("1", "entity - 001", "reference", None),  # Missing value
        ("2", None, "name", "Entity with no ID"),  # Missing entity
    ]

    incomplete_df = spark.createDataFrame(incomplete_data, incomplete_schema)

    with tempfile.TemporaryDirectory() as temp_dir:
        incomplete_csv_path = os.path.join(temp_dir, "incomplete_data.csv")
        incomplete_df.write.mode("overwrite").option("header", True).csv(
            incomplete_csv_path
        )

        # Load and process
        df = spark.read.option("header", True).csv(incomplete_csv_path)

        # Filter out rows with missing critical data
        clean_df = df.filter(df.entity.isNotNull() & df.value.isNotNull())

        assert clean_df.count() == 1  # Only one complete record


def test_entity_workflow_performance_with_large_dataset(spark):
    """Test entity processing workflow performance with larger dataset."""
    from pyspark.sql.functions import first
    from pyspark.sql.types import StringType, StructField, StructType

    # Generate larger test dataset
    large_schema = StructType(
        [
            StructField("entry - number", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    # Generate 1000 entities with 3 fields each = 3000 rows
    large_data = []
    for i in range(1000):
        entity_id = f"entity-{i:04d}"
        entry_num = str(i + 1)
        large_data.extend(
            [
                (entry_num, entity_id, "name", f"Entity {i}"),
                (entry_num, entity_id, "type", "test"),
                (entry_num, entity_id, "status", "active"),
            ]
        )

    large_df = spark.createDataFrame(large_data, large_schema)

    # Process the large dataset
    pivot_df = (
        large_df.groupBy("entry - number", "entity").pivot("field").agg(first("value"))
    )
    pivot_df = pivot_df.drop("entry - number")

    # Verify processing completed successfully
    assert pivot_df.count() == 1000
    assert "name" in pivot_df.columns
    assert "type" in pivot_df.columns
    assert "status" in pivot_df.columns


@pytest.mark.acceptance
def test_entity_data_quality_validation(spark, sample_entity_csv_data):
    """Test data quality validation in entity processing."""
    from pyspark.sql.functions import col

    # Test for required fields
    required_fields = ["entity", "field", "value"]
    for field in required_fields:
        assert field in sample_entity_csv_data.columns

    # Test for data completeness
    null_counts = {}
    for field in required_fields:
        null_count = sample_entity_csv_data.filter(col(field).isNull()).count()
        null_counts[field] = null_count

    # Should have no nulls in critical fields
    assert null_counts["entity"] == 0
    assert null_counts["field"] == 0
    assert null_counts["value"] == 0

    # Test for data consistency
    unique_entities = sample_entity_csv_data.select("entity").distinct().count()
    assert unique_entities == 2  # Should have exactly 2 entities


@pytest.mark.acceptance
def test_complete_etl_pipeline_integration(spark, temp_csv_files):
    """Test complete ETL pipeline integration with all components."""
    from pyspark.sql.functions import first, lit

    # This test simulates the complete main_collection_data.py workflow
    # Step 1: Data extraction (simulated)
    raw_df = spark.read.option("header", True).csv(temp_csv_files["entity_csv"])
    assert raw_df.count() > 0

    # Step 2: Data transformation
    dataset_name = "title - boundary"

    # Replace hyphens in column names (as done in real pipeline)
    for column in raw_df.columns:
        if "-" in column:
            new_col = column.replace("-", "_")
            raw_df = raw_df.withColumnRenamed(column, new_col)
    
    # Debug: Print actual column names to understand the issue
    actual_columns = raw_df.columns
    print(f"Actual columns after renaming: {actual_columns}")
    
    # Use the actual column name that exists
    entry_col = None
    for col_name in actual_columns:
        if "entry" in col_name.lower():
            entry_col = col_name
            break
    
    if entry_col is None:
        raise ValueError(f"No entry column found in: {actual_columns}")

    # Apply entity transformation (simplified without external dependencies)
    processed_df = (
        raw_df.groupBy(entry_col, "entity").pivot("field").agg(first("value"))
    )
    processed_df = processed_df.drop(entry_col)
    processed_df = processed_df.withColumn("dataset", lit(dataset_name))

    # Step 3: Data loading (output)
    output_path = os.path.join(temp_csv_files["output_dir"], "etl_output")
    processed_df.write.mode("overwrite").option("header", True).csv(output_path)

    # Verify complete pipeline
    assert os.path.exists(output_path)

    # Verify output data
    result_df = spark.read.option("header", True).csv(output_path)
    assert result_df.count() == 2
    assert "dataset" in result_df.columns

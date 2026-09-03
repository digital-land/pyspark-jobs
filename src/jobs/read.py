import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, input_file_name, lit, when
from pyspark.sql.types import StringType, StructField, StructType

from jobs.utils.df_utils import normalise_column_names

logger = logging.getLogger(__name__)


def read_old_resources(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the old-resource CSV file into a DataFrame.

    Expected columns: resource, status (and any others present in the file).

    Args:
        spark: Active Spark session
        path: Path to the old-resource.csv file (local or S3)

    Returns:
        DataFrame with old resource records
    """
    logger.info(f"read_old_resources: Reading old resources from {path}")
    df = spark.read.option("header", "true").csv(path)
    return normalise_column_names(df)


def read_resources(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the collection's resource CSV into a DataFrame.

    Provides the `start-date` and `end-date` the entity ranking needs to prefer
    live resources over superseded ones, matching `dataset_parquet.py`'s
    LEFT JOIN onto resource.csv.

    Note this is a different file from old-resource.csv: resource.csv lives at
    {collection}-collection/collection/, old-resource.csv at
    config/collection/{collection}/.

    Args:
        spark: Active Spark session
        path: Path to the resource.csv file (local or S3)

    Returns:
        DataFrame with resource records, columns normalised to underscores
    """
    logger.info(f"read_resources: Reading resources from {path}")
    df = spark.read.option("header", "true").csv(path)
    return normalise_column_names(df)


_ISSUE_RAW_SCHEMA = StructType(
    [StructField(f"_c{i}", StringType(), True) for i in range(9)]
)


def read_csvs_by_name(
    spark: SparkSession, files: list[str], columns: list[str]
) -> DataFrame:
    """
    Read many CSVs per-file and unionByName on the named columns.

    A single spark.read.csv([...]) maps columns by POSITION and uses one file's
    header for all files, so a file whose column SET differs — e.g. prod
    tree-preservation-order's source.csv is missing the leading `source` column
    — gets shifted (its `pipelines` value lands in the `organisation` slot,
    producing phantom provider rows). Reading each file against its OWN header
    and selecting by name removes that risk.

    Costs one Spark job per file, so keep this for small file counts — tens,
    not thousands. For issue CSVs use read_issue_csvs.

    Args:
        spark: Active Spark session
        files: Paths to read
        columns: Columns to select from each file; a file missing any of them
            is skipped with a warning

    Returns:
        DataFrame with exactly `columns`

    Raises:
        ValueError: If no file had all of `columns`
    """
    frames = []
    for f in files:
        df = normalise_column_names(spark.read.option("header", "true").csv(f))
        if not all(c in df.columns for c in columns):
            logger.warning(f"read_csvs_by_name: {f} missing {columns} — skipping")
            continue
        frames.append(df.select(*columns))
    if not frames:
        raise ValueError(f"No usable CSVs with columns {columns}")
    return reduce(lambda a, b: a.unionByName(b), frames)


def read_issue_csvs(spark: SparkSession, files: list[str]) -> DataFrame:
    """
    Read issue CSVs, which exist in three historical layouts.

    7 cols  dataset,resource,line-number,entry-number,field,issue-type,value
    8 cols  ...,field,issue-type,value,message
    9 cols  ...,field,entity,issue-type,value,message

    An `entity` column was added at position 6, shifting issue-type from index
    5 to 6. spark.read.option("header","true").csv(files) infers the schema from
    ONE file and applies it positionally to the rest, ignoring their headers —
    so whichever layout is read first silently corrupts the others: issue_type
    receives the entity number, the issue-type join returns a null severity, and
    every row is dropped by the severity filter with no error and exit code 0.

    Reading headerless against nine string columns and re-aligning each row
    using its own file's header parses all three layouts in a single job,
    unlike read_csvs_by_name which costs a job per file.

    Args:
        spark: Active Spark session
        files: Issue CSV paths

    Returns:
        DataFrame with dataset, resource, line_number, entry_number, field,
        entity, issue_type, value, message — entity and message are null for
        the layouts that lack them
    """
    raw = (
        spark.read.schema(_ISSUE_RAW_SCHEMA)
        .option("header", "false")
        .csv(files)
        .withColumn("_file", input_file_name())
    )

    # each file's own header row, so its rows can be aligned independently
    headers = raw.filter(col("_c0") == "dataset").select(
        col("_file"), col("_c6").alias("_h6")
    )
    rows = raw.filter(col("_c0") != "dataset").join(headers, on="_file", how="left")

    nine_column = col("_h6") == "issue-type"

    return rows.select(
        col("_c0").alias("dataset"),
        col("_c1").alias("resource"),
        col("_c2").alias("line_number"),
        col("_c3").alias("entry_number"),
        col("_c4").alias("field"),
        when(nine_column, col("_c5")).otherwise(lit(None)).alias("entity"),
        when(nine_column, col("_c6")).otherwise(col("_c5")).alias("issue_type"),
        when(nine_column, col("_c7")).otherwise(col("_c6")).alias("value"),
        when(nine_column, col("_c8")).otherwise(col("_c7")).alias("message"),
    )

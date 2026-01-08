import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col,
    desc,
    expr,
    first,
    lit,
    row_number,
    struct,
    to_date,
    to_json,
    when,
)
from pyspark.sql.window import Window

from jobs.utils.df_utils import show_df
from jobs.utils.logger_config import get_logger
from jobs.utils.s3_dataset_typology import get_dataset_typology

# centroid computation is handled via Sedona when available; fall back to
# the Python UDF in jobs.utils.point_utils at runtime if needed (lazy import).

logger = get_logger(__name__)


# -------------------- Transformation Processing --------------------
def transform_data_fact(df):
    try:
        logger.info("transform_data_fact:Transforming data for Fact table")
        # Define the window specification
        window_spec = (
            Window.partitionBy("fact")
            .orderBy("priority", "entry_date", "entry_number")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        logger.info(
            f"transform_data_fact:Window specification defined for partitioning by 'fact' and ordering {window_spec})"
        )
        # Add row number
        df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
        logger.info(
            f"transform_data_fact:Row number added to DataFrame {df_with_rownum.columns}"
        )

        # Filter to keep only the top row per partition
        transf_df = df_with_rownum.filter(df_with_rownum["row_num"] == 1).drop(
            "row_num"
        )
        transf_df = transf_df.select(
            "fact",
            "end_date",
            "entity",
            "field",
            "entry_date",
            "priority",
            "reference_entity",
            "start_date",
            "value",
        )
        logger.info(
            f"transform_data_fact:Final DataFrame after filtering: {transf_df.columns}"
        )
        transf_df = transf_df.select(
            "end_date",
            "entity",
            "fact",
            "field",
            "entry_date",
            "priority",
            "reference_entity",
            "start_date",
            "value",
        )
        return transf_df
    except Exception as e:
        logger.error(f"transform_data_fact:Error occurred - {e}")
        raise


def transform_data_fact_res(df):
    try:
        logger.info("transform_data_fact_res:Transforming data for Fact Resource table")
        transf_df = df.select(
            "end_date",
            "fact",
            "entry_date",
            "entry_number",
            "priority",
            "resource",
            "start_date",
        )
        logger.info(
            f"transform_data_fact_res:Final DataFrame after filtering: {transf_df.columns}"
        )

        transf_df = transf_df.select(
            "end_date",
            "fact",
            "entry_date",
            "entry_number",
            "priority",
            "resource",
            "start_date",
        )

        return transf_df
    except Exception as e:
        logger.error(f"transform_data_fact_res:Error occurred - {e}")
        raise


def transform_data_issue(df):
    try:
        logger.info("transform_data_issue:Transforming data for Issue table")
        transf_df = (
            df.withColumn("start_date", lit("").cast("string"))
            .withColumn("entry_date", lit("").cast("string"))
            .withColumn("end_date", lit("").cast("string"))
        )
        logger.info(
            f"transform_data_fact_res:Final DataFrame after filtering: {transf_df.columns}"
        )
        transf_df = transf_df.select(
            "end_date",
            "entity",
            "entry_date",
            "entry_number",
            "field",
            "issue_type",
            "line_number",
            "dataset",
            "resource",
            "start_date",
            "value",
            "message",
        )
        return transf_df
    except Exception as e:
        logger.error(f"transform_data_issue:Error occurred - {e}")
        raise


def transform_data_entity(df, data_set, spark, env):
    try:
        logger.info("transform_data_entity:Transforming data for Entity table")
        show_df(df, 20, env)
        # 1) Select the top record per (entity, field) using priority, entry_date, entry_number
        # Fallback if 'priority' is missing: use entry_date, entry_number
        if "priority" in df.columns:
            ordering_cols = [desc("priority"), desc("entry_date"), desc("entry_number")]
        else:
            ordering_cols = [desc("entry_date"), desc("entry_number")]

        w = Window.partitionBy("entity", "field").orderBy(*ordering_cols)
        df_ranked = (
            df.withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        # 2) Pivot to get one row per entity
        pivot_df = df_ranked.groupBy("entity").pivot("field").agg(first("value"))
        show_df(pivot_df, 5, env)

        logger.info(
            "transform_data_entity:Adding Typology data as the column missing after flattening"
        )
        # filtered_df = pivot_df.filter(col("field") == "typology").select("field", "value")

        # Add a new column "typology" to the pivoted DataFrame by applying the get_dataset_typology function
        typology_value = get_dataset_typology(data_set)
        logger.info(
            f"transform_data_entity: Fetched typology value from dataset specification for dataset: {data_set} is {typology_value}"
        )

        pivot_df = pivot_df.withColumn("typology", F.lit(typology_value))
        show_df(pivot_df, 5, env)

        # 3) Normalise column names (kebab-case -> snake_case)
        logger.info(
            f"transform_data_entity: Normalising column names from kebab-case to snake_case"
        )
        for column in pivot_df.columns:
            if "-" in column:
                pivot_df = pivot_df.withColumnRenamed(column, column.replace("-", "_"))

        # 4) Set dataset and drop legacy geojson if present
        logger.info(
            f"transform_data_entity: Setting dataset column to {data_set} and dropping geojson column if exists"
        )
        pivot_df = pivot_df.withColumn("dataset", lit(data_set))
        if "geojson" in pivot_df.columns:
            pivot_df = pivot_df.drop("geojson")

        # 5) Organisation join to fetch organisation_entity
        logger.info(
            f"transform_data_entity: Joining organisation to get organisation_entity"
        )
        organisation_df = spark.read.option("header", "true").csv(
            f"s3://{env}-collection-data/organisation/dataset/organisation.csv"
        )
        pivot_df = (
            pivot_df.join(
                organisation_df,
                pivot_df.organisation == organisation_df.organisation,
                "left",
            )
            .select(
                pivot_df["*"], organisation_df["entity"].alias("organisation_entity")
            )
            .drop("organisation")
        )

        # 6) Join typology from dataset specification
        # (Step removed: typology already retrieved earlier)

        # 7) Build json from any non-standard columns
        standard_columns = {
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        }
        if "geometry" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("geometry", lit(None).cast("string"))
        # Ensure expected columns exist before projection
        if "end_date" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("end_date", lit(None).cast("date"))
        if "start_date" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("start_date", lit(None).cast("date"))
        if "name" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("name", lit("").cast("string"))
        if "point" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("point", lit(None).cast("string"))
        diff_columns = [c for c in pivot_df.columns if c not in standard_columns]
        if diff_columns:
            pivot_df = pivot_df.withColumn(
                "json", to_json(struct(*[col(c) for c in diff_columns]))
            )
        else:
            pivot_df = pivot_df.withColumn("json", lit("{}"))

        # 8) Normalise date columns
        for date_col in ["end_date", "entry_date", "start_date"]:
            if date_col in pivot_df.columns:
                pivot_df = pivot_df.withColumn(
                    date_col,
                    when(col(date_col) == "", None)
                    .when(col(date_col).isNull(), None)
                    .otherwise(to_date(col(date_col), "yyyy-MM-dd")),
                )

        # 9) Normalise geometry columns
        for geom_col in ["geometry", "point"]:
            if geom_col in pivot_df.columns:
                pivot_df = pivot_df.withColumn(
                    geom_col,
                    when(col(geom_col) == "", None)
                    .when(col(geom_col).isNull(), None)
                    .when(col(geom_col).startswith("POINT"), col(geom_col))
                    .when(col(geom_col).startswith("POLYGON"), col(geom_col))
                    .when(col(geom_col).startswith("MULTIPOLYGON"), col(geom_col))
                    .otherwise(None),
                )

        # 10) Final projection and safety dedupe
        out = pivot_df.select(
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ).dropDuplicates(["entity"])

        logger.info(
            "transform_data_entity:Transform data for Entity table after pivoting and normalization"
        )
        show_df(out, 5, env)

        return out
    except Exception as e:
        logger.error(f"transform_data_entity:Error occurred - {e}")
        raise

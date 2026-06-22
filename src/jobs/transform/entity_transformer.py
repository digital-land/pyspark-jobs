import logging
from datetime import datetime

from pyspark.sql.functions import (
    col,
    desc,
    first,
    lit,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import (
    row_number,
    struct,
    to_date,
    to_json,
    when,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from jobs.config.schema import get_schema
from jobs.utils.df_utils import show_df
from jobs.utils.geometry_utils import calculate_centroid
from jobs.utils.s3_dataset_typology import get_dataset_typology

logger = logging.getLogger(__name__)

_STANDARD_COLUMNS = {
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
    "quality",
    "reference",
    "start_date",
    "typology",
}


def _deduplicate_eav(df):
    if "priority" in df.columns:
        ordering_cols = [desc("entry_date"), desc("priority"), desc("entry_number")]
    else:
        ordering_cols = [desc("entry_date"), desc("entry_number")]

    w = Window.partitionBy("entity", "field").orderBy(*ordering_cols)

    return (
        df.withColumn("row_num", row_number().over(w))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def _pivot_to_entity(df, env):
    agg_dict = {"value": first("value")}
    if "priority" in df.columns:
        agg_dict["priority"] = spark_max("priority")

    pivot_df = df.groupBy("entity").pivot("field").agg(agg_dict["value"])

    if "priority" in df.columns:
        priority_df = df.groupBy("entity").agg(spark_max("priority").alias("priority"))
        pivot_df = pivot_df.join(priority_df, "entity", "left")

    show_df(pivot_df, 5, env)
    return pivot_df


def _add_quality_column(pivot_df):
    if "priority" not in pivot_df.columns:
        return pivot_df.withColumn("quality", lit(""))

    return pivot_df.withColumn(
        "quality",
        when(col("priority") == 1, lit("same"))
        .when(col("priority") == 2, lit("authoritative"))
        .otherwise(lit("")),
    ).drop("priority")


def _add_typology(df, dataset, env):
    typology_value = get_dataset_typology(dataset)
    logger.info(f"_add_typology: Fetched typology value: {typology_value}")
    df = df.withColumn("typology", lit(typology_value))
    show_df(df, 5, env)
    return df


def _normalise_column_names(df):
    for column in df.columns:
        if "-" in column:
            df = df.withColumnRenamed(column, column.replace("-", "_"))
    return df


def _set_dataset(df, dataset):
    df = df.withColumn("dataset", lit(dataset))
    if "geojson" in df.columns:
        df = df.drop("geojson")
    return df


def _join_organisation(df, organisation_df):
    return (
        df.join(
            organisation_df, df.organisation == organisation_df.organisation, "left"
        )
        .select(df["*"], organisation_df["entity"].alias("organisation_entity"))
        .drop("organisation")
    )


def _build_json_column(df):
    if "geometry" not in df.columns:
        df = df.withColumn("geometry", lit(None).cast("string"))
    if "end_date" not in df.columns:
        df = df.withColumn("end_date", lit(None).cast("date"))
    if "start_date" not in df.columns:
        df = df.withColumn("start_date", lit(None).cast("date"))
    if "name" not in df.columns:
        df = df.withColumn("name", lit("").cast("string"))
    if "point" not in df.columns:
        df = df.withColumn("point", lit(None).cast("string"))

    diff_columns = [c for c in df.columns if c not in _STANDARD_COLUMNS]
    if diff_columns:
        df = df.withColumn("json", to_json(struct(*[col(c) for c in diff_columns])))
    else:
        df = df.withColumn("json", lit("{}"))
    return df


def _normalise_dates(df):
    for date_col in ["end_date", "entry_date", "start_date"]:
        if date_col in df.columns:
            df = df.withColumn(
                date_col,
                when(col(date_col) == "", None)
                .when(col(date_col).isNull(), None)
                .otherwise(to_date(col(date_col), "yyyy-MM-dd")),
            )
    return df


def _normalise_geometry(df):
    for geom_col in ["geometry", "point"]:
        if geom_col in df.columns:
            df = df.withColumn(
                geom_col,
                when(col(geom_col) == "", None)
                .when(col(geom_col).isNull(), None)
                .when(col(geom_col).startswith("POINT"), col(geom_col))
                .when(col(geom_col).startswith("POLYGON"), col(geom_col))
                .when(col(geom_col).startswith("MULTIPOLYGON"), col(geom_col))
                .otherwise(None),
            )
    return df


def _final_projection(df):
    return get_schema("entity").enforce(df).dropDuplicates(["entity"])


def transform_entity(df, dataset, organisation_df, env=None):
    logger.info("transform_entity: Transforming data for Entity table")
    show_df(df, 20, env)

    logger.info("transform_entity: Step 1 — deduplicate EAV records")
    df = _deduplicate_eav(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 2 — pivot to wide format")
    df = _pivot_to_entity(df, env)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 3 — add quality column")
    df = _add_quality_column(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 4 — add typology")
    df = _add_typology(df, dataset, env)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 5 — normalise column names")
    df = _normalise_column_names(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 6 — set dataset column")
    df = _set_dataset(df, dataset)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 7 — join organisation")
    df = _join_organisation(df, organisation_df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 8 — build JSON column")
    df = _build_json_column(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 9 — normalise dates")
    df = _normalise_dates(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 10 — normalise geometry")
    df = _normalise_geometry(df)
    show_df(df, 5, env)

    logger.info("transform_entity: Step 11 — final projection")
    df = _final_projection(df)
    show_df(df, 5, env)

    df = df.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    df = calculate_centroid(df)
    logger.info("transform_entity: Complete")
    return df

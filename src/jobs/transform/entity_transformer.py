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
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import datetime

from jobs.utils.df_utils import show_df
from jobs.utils.geometry_utils import calculate_centroid
from jobs.utils.logger_config import get_logger, log_execution_time
from jobs.utils.s3_dataset_typology import get_dataset_typology

logger = get_logger(__name__)


class EntityTransformer:
    """
    Transform transformed data into entity records.

    transformed data follows the EAV (Entity-Attribute-Value) model, where each row represents a single attribute of an entity.
    EAV Model:
    - entity: The main object or record (e.g., a property or organisation)
    - field: The attribute name (e.g., entry-date, geometry, organisation)
    - value: The actual data for that attribute
    - priority: Numeric ranking determining which record to keep (higher = higher priority)

    Business Rule:
    Each entity can have multiple field-value pairs, but only one pair per field.
    When duplicates exist for the same entity-field combination, keep the record
    with the highest priority number.
    """

    STANDARD_COLUMNS = {
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

    @log_execution_time
    def transform(self, df, dataset, organisation_df, env=None):
        """Transform transformed data to entity format."""
        logger.info("EntityTransformer: Transforming data for Entity table")
        show_df(df, 20, env)

        # Step 1: Deduplicate EAV records - keep highest priority per (entity, field)
        logger.info(
            "_deduplicate_eav: Step 1: Deduplicate EAV records - keep highest priority per (entity, field)"
        )
        df_ranked = self._deduplicate_eav(df)
        show_df(df_ranked, 5, env)

        # Step 2: Pivot from EAV format to wide format (one row per entity)
        logger.info(
            "_pivot_to_entity: Step 2: Pivot from EAV format to wide format (one row per entity)"
        )
        pivot_df = self._pivot_to_entity(df_ranked, env)
        show_df(pivot_df, 5, env)

        # Step 3: Add quality column based on max priority (1=same, 2=authoritative)
        logger.info(
            "_add_quality_column: Step 3: Add quality column based on max priority (1=same, 2=authoritative)"
        )
        pivot_df = self._add_quality_column(pivot_df)
        show_df(pivot_df, 5, env)

        # Step 4: Add typology from dataset specification
        logger.info("_add_typology: Step 4: Add typology from dataset specification")
        pivot_df = self._add_typology(pivot_df, dataset, env)
        show_df(pivot_df, 5, env)

        # Step 5: Normalise column names (kebab-case to snake_case)
        logger.info(
            "_normalise_column_names: Step 5: Normalise column names (kebab-case to snake_case)"
        )
        pivot_df = self._normalise_column_names(pivot_df)
        show_df(pivot_df, 5, env)

        # Step 6: Set dataset column and cleanup
        logger.info("_set_dataset: Step 6: Set dataset column and cleanup")
        pivot_df = self._set_dataset(pivot_df, dataset)
        show_df(pivot_df, 5, env)

        # Step 7: Join organisation data to get organisation_entity
        logger.info(
            "_join_organisation: Step 7: Join organisation data to get organisation_entity"
        )
        pivot_df = self._join_organisation(pivot_df, organisation_df)
        show_df(pivot_df, 5, env)

        # Step 8: Build JSON column from non-standard columns
        logger.info(
            "_build_json_column: Step 8: Build JSON column from non-standard columns"
        )
        pivot_df = self._build_json_column(pivot_df)
        show_df(pivot_df, 5, env)

        # Step 9: Normalise date columns to proper date type
        logger.info(
            "_normalise_dates: Step 9: Normalise date columns to proper date type"
        )
        pivot_df = self._normalise_dates(pivot_df)
        show_df(pivot_df, 5, env)

        # Step 10: Normalise geometry columns (validate WKT format)
        logger.info(
            "_normalise_geometry: Step 10: Normalise geometry columns (validate WKT format)"
        )
        pivot_df = self._normalise_geometry(pivot_df)
        show_df(pivot_df, 5, env)

        # Step 11: Final projection and deduplication
        logger.info("_final_projection: Step 11: Final projection and deduplication")
        pivot_df = self._final_projection(pivot_df)
        show_df(pivot_df, 5, env)

        logger.info("EntityTransformer: Transformation complete")
        show_df(pivot_df, 5, env)

        # add processing timestamp
        pivot_df = pivot_df.withColumn(
            "processed_timestamp",
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
        )

        pivot_df = calculate_centroid(pivot_df)
        return pivot_df

    def _deduplicate_eav(self, df):
        """
        EAV Deduplication: Select top record per (entity, field) by priority.

        For each unique (entity, field) combination, keep only the record with:
        1. Most recent entry_date (primary sort)
        2. Highest priority (secondary sort, if column exists)
        3. Highest entry_number (final tiebreaker)
        """
        # Determine ordering columns based on available fields
        if "priority" in df.columns:
            ordering_cols = [desc("entry_date"), desc("priority"), desc("entry_number")]
        else:
            ordering_cols = [desc("entry_date"), desc("entry_number")]

        # Create window partitioned by (entity, field) and ordered by entry_date/priority
        w = Window.partitionBy("entity", "field").orderBy(*ordering_cols)

        # Assign row numbers and keep only rank 1 (most recent entry_date, then highest priority)
        return (
            df.withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

    def _pivot_to_entity(self, df, env):
        """
        Pivot from EAV format to wide format, preserving max priority.

        Transforms from:
          entity | field    | value    | priority
          1001   | name     | "Prop A" | 2
          1001   | geometry | "POINT" | 1

        To:
          entity | name     | geometry | priority
          1001   | "Prop A" | "POINT" | 2
        """
        agg_dict = {"value": first("value")}
        if "priority" in df.columns:
            agg_dict["priority"] = spark_max("priority")

        pivot_df = df.groupBy("entity").pivot("field").agg(agg_dict["value"])

        if "priority" in df.columns:
            priority_df = df.groupBy("entity").agg(
                spark_max("priority").alias("priority")
            )
            pivot_df = pivot_df.join(priority_df, "entity", "left")

        show_df(pivot_df, 5, env)
        return pivot_df

    def _add_quality_column(self, pivot_df):
        """
        Add quality column based on priority, then drop priority.

        Business Rule:
        - priority = 1 → quality = "same"
        - priority = 2 → quality = "authoritative"
        - other values → quality = "" (blank)
        """
        if "priority" not in pivot_df.columns:
            return pivot_df.withColumn("quality", lit(""))

        return pivot_df.withColumn(
            "quality",
            when(col("priority") == 1, lit("same"))
            .when(col("priority") == 2, lit("authoritative"))
            .otherwise(lit("")),
        ).drop("priority")

    def _add_typology(self, df, data_set, env):
        """
        Add typology column from dataset specification.

        Fetches the typology value from the dataset specification
        and adds it as a constant column to all rows.
        """
        typology_value = get_dataset_typology(data_set)
        logger.info(f"EntityTransformer: Fetched typology value: {typology_value}")
        df = df.withColumn("typology", lit(typology_value))
        show_df(df, 5, env)
        return df

    def _normalise_column_names(self, df):
        """
        Normalise column names from kebab-case to snake_case.

        Example: "entry-date" → "entry_date"
        """
        for column in df.columns:
            if "-" in column:
                df = df.withColumnRenamed(column, column.replace("-", "_"))
        return df

    def _set_dataset(self, df, data_set):
        """
        Set dataset column and drop legacy geojson.

        Adds dataset name as a column and removes geojson if present
        (geojson is handled separately in output formatting).
        """
        df = df.withColumn("dataset", lit(data_set))
        if "geojson" in df.columns:
            df = df.drop("geojson")
        return df

    def _join_organisation(self, df, organisation_df):
        """
        Join organisation to fetch organisation_entity.

        Replaces organisation code with organisation_entity ID
        by joining with the organisation reference dataset.
        """
        # Left join to get organisation_entity, then drop original organisation column
        return (
            df.join(
                organisation_df, df.organisation == organisation_df.organisation, "left"
            )
            .select(df["*"], organisation_df["entity"].alias("organisation_entity"))
            .drop("organisation")
        )

    def _build_json_column(self, df):
        """
        Build json from any non-standard columns.

        Ensures all standard columns exist (with defaults if missing),
        then packages any extra columns into a JSON column.
        """
        # Ensure standard columns exist with appropriate defaults
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

        # Find columns not in standard set and package them as JSON
        diff_columns = [c for c in df.columns if c not in self.STANDARD_COLUMNS]
        if diff_columns:
            df = df.withColumn("json", to_json(struct(*[col(c) for c in diff_columns])))
        else:
            df = df.withColumn("json", lit("{}"))
        return df

    def _normalise_dates(self, df):
        """
        Normalise date columns to proper date type.

        Converts string dates to date type, handling empty strings and nulls.
        Expected format: yyyy-MM-dd
        """
        for date_col in ["end_date", "entry_date", "start_date"]:
            if date_col in df.columns:
                df = df.withColumn(
                    date_col,
                    when(col(date_col) == "", None)
                    .when(col(date_col).isNull(), None)
                    .otherwise(to_date(col(date_col), "yyyy-MM-dd")),
                )
        return df

    def _normalise_geometry(self, df):
        """
        Normalise geometry columns to valid WKT format.

        Validates that geometry values are proper WKT strings
        (POINT, POLYGON, or MULTIPOLYGON). Invalid values set to null.
        """
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

    def _final_projection(self, df):
        """
        Final projection and safety deduplication.

        Selects only the standard entity columns in the correct order
        and ensures no duplicate entities in the output.
        """
        return df.select(
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
        ).dropDuplicates(["entity"])

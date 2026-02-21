"""DataFrame utility functions for conditional operations based on environment."""

from pyspark.sql import DataFrame


def normalise_column_names(df: DataFrame) -> DataFrame:
    """Replace hyphens with underscores in all DataFrame column names.

    Input CSVs use kebab-case (entry-number, start-date, …). Transformers
    expect snake_case. Call this once after reading a CSV before passing the
    DataFrame to any transformer.
    """
    for col_name in df.columns:
        if "-" in col_name:
            df = df.withColumnRenamed(col_name, col_name.replace("-", "_"))
    return df


# added function to show dataframe rows based on environment
def show_df(df, n=5, env=None):
    """
    Show DataFrame rows only in development and staging environments.

    Args:
        df: PySpark DataFrame to display
        n: Number of rows to show (default: 5)
        env: Environment name (development, staging, production)
    """
    if env and env.lower() in ["development", "staging"]:
        df.show(n)


# added function to count dataframe rows based on environment
def count_df(df, env=None):
    """
    Count DataFrame rows only in development and staging environments.

    Args:
        df: PySpark DataFrame to count
        env: Environment name (development, staging, production)
    """
    if env and env.lower() in ["development", "staging"]:
        return df.count()
    return None

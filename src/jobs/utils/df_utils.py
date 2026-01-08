"""DataFrame utility functions for conditional operations based on environment."""


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

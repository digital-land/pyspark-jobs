"""DataFrame utility functions for conditional operations based on environment."""

def show_df(df, n=5, env=None):
    """
    Show DataFrame rows only in development and staging environments.
    
    Args:
        df: PySpark DataFrame to display
        n: Number of rows to show (default: 5)
        env: Environment name (development, staging, production)
    """
    if env and env.lower() in ['development', 'staging']:
        df.show(n)

"""
Example usage of the AWS Secrets Manager utility in main_collection_data.py

This example shows how to integrate the secrets manager with your existing code.
"""

from utils.aws_secrets_manager import (
    SecretsManagerError,
    get_database_credentials,
    get_secret,
    get_secret_json,
    get_secret_with_fallback,
)

logger = logging.getLogger(__name__)


def example_database_connection():
    """
    Example: How to retrieve database credentials and use them SECURELY.

    SECURITY NOTICE: This example shows the WRONG way and RIGHT way to handle credentials.
    """
    try:
        # Method 1: Using environment variable to specify secret name
        db_creds = get_database_credentials(
            secret_name=os.getenv("POSTGRES_SECRET_NAME", "myapp/database/postgres"),
            region_name=os.getenv("AWS_REGION", "us - east - 1"),
        )

        # Use the credentials
        username = db_creds["username"]
        password = db_creds["password"]
        host = db_creds["host"]
        port = db_creds["port"]
        database = db_creds.get("database", "postgres")

        # Create connection string (example for PostgreSQL)
        # NOTE: In production, pass these credentials directly to your database client
        # DO NOT return, log, or expose connection strings as they contain sensitive information
        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        logger.info("Database credentials retrieved successfully")

        # For security demonstration purposes only - in real code, use credentials directly
        # with your database client and don't return or expose any connection details
        return {"status": "success", "message": "Database credentials ready for use"}

    except SecretsManagerError as e:
        logger.error(f"Failed to retrieve database credentials: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in database connection setup: {e}")
        raise


def example_secure_database_usage():
    """
    Example: SECURE way to use database credentials without exposing sensitive data.
    """
    try:
        # Retrieve credentials securely
        db_creds = get_database_credentials(
            secret_name=os.getenv("POSTGRES_SECRET_NAME", "myapp/database/postgres"),
            region_name=os.getenv("AWS_REGION", "us - east - 1"),
        )

        # RIGHT WAY: Use credentials directly with your database client
        # Example with psycopg2 (PostgreSQL) - credentials never exposed
        import psycopg2

        conn = psycopg2.connect(
            host=db_creds["host"],
            port=db_creds["port"],
            database=db_creds.get("database", "postgres"),
            user=db_creds["username"],
            password=db_creds["password"],
        )

        logger.info("Database connection established securely")

        # Use the connection for your database operations
        # ...

        conn.close()
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Secure database connection failed: {str(e)}")
        raise


def example_api_key_retrieval():
    """
    Example: How to retrieve a simple API key.
    """
    try:
        # Method 2: Direct secret retrieval
        api_key = get_secret(
            secret_name="myapp/external - apis/data - source - key",
            region_name=os.getenv("AWS_REGION"),
        )

        logger.info("API key retrieved successfully")
        return api_key

    except SecretsManagerError as e:
        logger.error(f"Failed to retrieve API key: {e}")
        raise


def example_with_fallback():
    """
    Example: How to use secrets with environment variable fallback for development.
    """
    try:
        # Method 3: With fallback to environment variable (useful for local development)
        s3_access_key = get_secret_with_fallback(
            secret_name="myapp/aws/s3 - access - key",
            env_var_name="S3_ACCESS_KEY",  # Fallback for local development
            region_name=os.getenv("AWS_REGION"),
        )

        logger.info("S3 access key retrieved")
        return s3_access_key

    except SecretsManagerError as e:
        logger.error(f"Failed to retrieve S3 access key: {e}")
        raise


def example_json_secret():
    """
    Example: How to retrieve a complex JSON secret.
    """
    try:
        # Method 4: JSON secret retrieval
        app_config = get_secret_json(
            secret_name="myapp/config/application - settings",
            region_name=os.getenv("AWS_REGION"),
        )

        # Use the configuration
        debug_mode = app_config.get("debug", False)
        timeout = app_config.get("timeout", 30)
        endpoints = app_config.get("endpoints", {})

        logger.info(
            f"Application config loaded - Debug: {debug_mode}, Timeout: {timeout}"
        )
        return app_config

    except SecretsManagerError as e:
        logger.error(f"Failed to retrieve application config: {e}")
        raise


# Example integration with main_collection_data.py
def enhanced_main_with_secrets(args):
    """
    Example of how to modify your main_collection_data.py main() function
    to use secrets manager.
    """
    logger.info("Starting ETL process with secrets from AWS Secrets Manager")

    try:
        # Retrieve database credentials if needed
        if os.getenv("USE_DATABASE", "false").lower() == "true":
            db_creds = get_database_credentials(
                secret_name=os.getenv("POSTGRES_SECRET_NAME"),
                region_name=os.getenv("AWS_REGION"),
            )
            logger.info("Database credentials retrieved successfully")
            # Use db_creds for your database connections

        # Retrieve any API keys needed for external services
        if os.getenv("USE_EXTERNAL_API", "false").lower() == "true":
            api_key = get_secret(
                secret_name=os.getenv("API_SECRET_NAME"),
                region_name=os.getenv("AWS_REGION"),
            )
            logger.info("External API key retrieved successfully")
            # Use api_key for external API calls

        # Continue with your existing main() logic here...
        # Your existing ETL process from main_collection_data.py

        logger.info("ETL process completed successfully")

    except SecretsManagerError as e:
        logger.error(f"Secrets retrieval failed: {e}")
        raise
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Example usage
    try:
        print(
            "=== Database Credentials Example (Educational - shows what NOT to do) ==="
        )
        example_database_connection()

        print("\n=== SECURE Database Usage Example ===")
        example_secure_database_usage()

        print("\n=== API Key Example ===")
        example_api_key_retrieval()

        print("\n=== Fallback Example ===")
        example_with_fallback()

        print("\n=== JSON Secret Example ===")
        example_json_secret()

    except Exception as e:
        print(f"Example failed: {e}")

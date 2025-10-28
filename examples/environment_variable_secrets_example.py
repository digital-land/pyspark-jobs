#!/usr/bin/env python3
"""
Example script showing how to use environment variables as fallback for AWS Secrets Manager.

This is particularly useful for testing and development environments where you might
want to avoid AWS Secrets Manager calls or when troubleshooting EMR Serverless issues.

Usage:
    # Set the environment variable with the secret JSON
    export SECRET_DEV_PYSPARK_POSTGRES='{"username":"test_user","password":"test_pass","dbName":"test_db","host":"localhost","port":"5432"}'
    
    # Then run your PySpark job - it will use the environment variable instead of AWS Secrets Manager
    python examples/environment_variable_secrets_example.py
"""

import os
import json
import sys

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from jobs.utils.aws_secrets_manager import get_secret_emr_compatible
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)

def main():
    """Demonstrate environment variable fallback for secrets."""
    
    # Example: Set up environment variable for PostgreSQL secret
    # This would normally be set externally, but we're doing it here for demonstration
    postgres_secret = {
        "username": "demo_user",
        "password": "demo_password", 
        "dbName": "demo_database",
        "host": "demo-host.amazonaws.com",
        "port": "5432"
    }
    
    # Convert secret name to environment variable name (/development-pd-batch/postgres-secret -> SECRET_DEV_PYSPARK_POSTGRES)
    env_var_name = "SECRET_DEV_PYSPARK_POSTGRES"
    os.environ[env_var_name] = json.dumps(postgres_secret)
    
    logger.info(f"Set environment variable {env_var_name} for testing")
    
    try:
        # Try to get the secret - this should use the environment variable fallback
        secret_json = get_secret_emr_compatible("/development-pd-batch/postgres-secret")
        
        # Parse and validate the secret
        secret_data = json.loads(secret_json)
        logger.info("Successfully retrieved secret using environment variable fallback")
        logger.info(f"Database: {secret_data.get('dbName')} at {secret_data.get('host')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}")
        return False
    
    finally:
        # Clean up the environment variable
        if env_var_name in os.environ:
            del os.environ[env_var_name]
            logger.info(f"Cleaned up environment variable {env_var_name}")

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Environment variable fallback test passed!")
        sys.exit(0)
    else:
        print("❌ Environment variable fallback test failed!")
        sys.exit(1)

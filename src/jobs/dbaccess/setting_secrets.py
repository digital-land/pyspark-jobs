
import os

import boto3
from botocore.exceptions import ClientError


def get_secret():
    """
    Retrieves a secret from AWS Secrets Manager using environment variables
    for configuration. Ensure that SECRET_NAME and AWS_REGION are set.
    """

    secret_name = os.getenv("POSTGRES_SECRET_NAME")
    region_name = os.getenv("AWS_REGION")

    if not secret_name or not region_name:
        raise ValueError("Environment variables POSTGRES_SECRET_NAME and AWS_REGION must be set.")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve secret: {e}")

    return get_secret_value_response.get('SecretString')

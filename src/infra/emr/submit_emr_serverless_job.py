import boto3
import json

# Initialize Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# Name of the secret
secret_name = 'airflow/emr_serverless'

# Retrieve the secret value
get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)

# Parse the secret string (assuming it's stored as a JSON string)
secret = json.loads(get_secret_value_response['SecretString'])

# Extract values from the secret
application_id = secret['application_id']
execution_role_arn = secret['execution_role_arn']
entry_point = secret['entry_point']
log_uri = secret['log_uri']

# Initialize EMR Serverless client
emr_client = boto3.client('emr-serverless')

# Submit the job
response = emr_client.start_job_run(
    applicationId=application_id,
    executionRoleArn=execution_role_arn,
    jobDriver={
        'sparkSubmit': {
            'entryPoint': entry_point,
            'sparkSubmitParameters': (
                "--conf spark.archives=ss3://development-collection-data/emr-data-processing/envs/emr_venv.tar.gz#environment "
                "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            )
        }
    },
    configurationOverrides={
        'monitoringConfiguration': {
            's3MonitoringConfiguration': {
                'logUri': log_uri
            }
        }
    }
)

print("Job submitted. JobRunId:", response['jobRunId'])

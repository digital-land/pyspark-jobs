# AWS Secrets Manager Troubleshooting Guide for EMR Serverless

This guide helps troubleshoot AWS Secrets Manager issues in EMR Serverless environments, particularly the `botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints` error.

## Common Issues and Solutions

### 1. DataNotFoundError - Unable to load data for endpoints

**Error Message:**
```
botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints
```

**Root Cause:**
This error occurs when botocore cannot find its internal data files (like endpoint definitions) in the EMR Serverless environment. This can happen due to:
- Incomplete botocore installation in the dependencies package
- Missing data directory in botocore package
- Version incompatibility between boto3 and botocore

**Solutions:**

#### Solution 1: Use the EMR-Compatible Function
The `get_secret_emr_compatible()` function includes multiple fallback strategies:

```python
from jobs.utils.aws_secrets_manager import get_secret_emr_compatible

# Use this instead of get_secret()
secret_value = get_secret_emr_compatible("/development-pd-batch/postgres-secret")
```

#### Solution 2: Environment Variable Fallback
For testing or when AWS Secrets Manager is unavailable:

```bash
# Set environment variable with the secret JSON
export SECRET_DEV_PYSPARK_POSTGRES='{"username":"user","password":"pass","dbName":"db","host":"host","port":"5432"}'
```

The naming convention converts the secret name:
- `/development-pd-batch/postgres-secret` → `SECRET_DEV_PYSPARK_POSTGRES`
- Replace `/` and `-` with `_`
- Convert to uppercase
- Add `SECRET_` prefix

#### Solution 3: Update Dependencies
Ensure you're using compatible versions:

```txt
# In requirements-emr.txt
boto3>=1.34.0,<2.0.0
botocore>=1.34.0,<2.0.0
```

#### Solution 4: Verify Build Process
Use the enhanced build script that verifies dependencies:

```bash
./bin/build_aws_package.sh --docker  # For Linux compatibility
```

The build script now checks for:
- Presence of critical dependencies (boto3, botocore, pg8000)
- botocore data directory
- Provides warnings if issues are detected

### 2. NoCredentialsError

**Error Message:**
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Solution:**
Ensure EMR Serverless has proper IAM roles configured with Secrets Manager permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "arn:aws:secretsmanager:region:account:secret:/development-pd-batch/postgres-secret*"
        }
    ]
}
```

### 3. Testing the Fix

#### Local Testing
```bash
# Run the example script to test environment variable fallback
python examples/environment_variable_secrets_example.py
```

#### EMR Testing
1. Build and deploy the updated package:
   ```bash
   ./bin/build_aws_package.sh --upload
   ```

2. Submit EMR job with the new package

3. Monitor logs for improved error messages and successful secret retrieval

## Implementation Details

### Fallback Strategy Order

The `get_secret_emr_compatible()` function tries these methods in order:

1. **Environment Variable**: Checks for `SECRET_{NORMALIZED_NAME}`
2. **Standard AWS SDK**: Uses regular boto3.client approach
3. **Session-based**: Creates explicit session with region
4. **Environment-configured**: Sets AWS_DEFAULT_REGION and retries
5. **Explicit Configuration**: Uses botocore.config with custom settings

### Error Handling Improvements

- More detailed error messages with exception type and details
- Logging at multiple levels (info, warning, error)
- Graceful degradation with multiple fallback attempts
- Security: Avoids logging sensitive credential information

## Prevention

### Build Process
- Use the enhanced build script with dependency verification
- Consider using Docker builds for Linux compatibility
- Regularly update boto3/botocore versions

### Monitoring
- Add alerting on Secrets Manager errors
- Monitor EMR job logs for early detection
- Set up CloudWatch alarms for failed secret retrievals

### Development
- Use environment variables for local development
- Test with the example scripts before deployment
- Validate secret JSON structure before deployment

## Getting Help

If these solutions don't resolve your issue:

1. Check EMR CloudWatch logs for additional error details
2. Verify the secret exists and is accessible from the EMR region
3. Test with a simple secret retrieval script first
4. Consider using AWS CLI from EMR to verify basic connectivity:
   ```bash
   aws secretsmanager get-secret-value --secret-id /development-pd-batch/postgres-secret
   ```

## Related Files

- `src/jobs/utils/aws_secrets_manager.py` - Secret retrieval with fallbacks
- `src/jobs/dbaccess/postgres_connectivity.py` - PostgreSQL connection logic
- `examples/environment_variable_secrets_example.py` - Environment variable fallback example
- `bin/build_aws_package.sh` - Build script with dependency verification

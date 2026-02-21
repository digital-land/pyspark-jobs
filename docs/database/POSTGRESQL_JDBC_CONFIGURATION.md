# PostgreSQL JDBC Driver Configuration for EMR Serverless 7.9.0

## Problem
When using PySpark with PostgreSQL via JDBC in EMR Serverless 7.9.0, you may encounter:
```
java.lang.ClassNotFoundException: org.postgresql.Driver
```

This error occurs because the PostgreSQL JDBC driver is not included in EMR Serverless by default. EMR 7.9.0 includes Spark 3.5.x and Java 17, requiring compatible JDBC driver versions.

## Solution

### 1. Current Implementation (Recommended for Development)
The current configuration uses the PostgreSQL JDBC driver from Maven Central:

```bash
--jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

This is implemented in:
- `emr_sl_transport_access_node_collection_test.py` (Airflow DAG)
- `build_aws_package.sh` (deployment manifest)

### 2. Production Alternative - S3 Hosted JAR
For production environments, consider hosting the JAR in your own S3 bucket for better reliability:

1. Download the PostgreSQL JDBC driver:
   ```bash
   wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
   ```

2. Upload to S3:
   ```bash
   aws s3 cp postgresql-42.7.4.jar s3://your-bucket/jars/
   ```

3. Update the `sparkSubmitParameters`:
   ```bash
   --jars s3://your-bucket/jars/postgresql-42.7.4.jar
   ```

### 3. Alternative Solutions - Self-Hosted JARs
For more control and production reliability, consider these approaches:

**Option A: S3-Hosted JAR (Recommended for Production)**
```bash
# Upload JAR to your S3 bucket
aws s3 cp postgresql-42.7.4.jar s3://your-bucket/jars/

# Update EMR configuration
--jars s3://your-bucket/jars/postgresql-42.7.4.jar
```

**Option B: Include in Build Package**
Bundle the JAR in your `build_output/jars/` directory via the build script.

## Configuration Details

### Spark Session Configuration
The Spark session in `src/jobs/job.py` is configured to support JDBC connections. The JDBC driver is made available through the `--jars` parameter rather than the Spark session configuration, which is the recommended approach for EMR Serverless.

### JDBC Connection Configuration
The PostgreSQL connection uses these properties:

```python
properties = {
    "user": conn_params["user"],
    "password": conn_params["password"],
    "driver": "org.postgresql.Driver"
}
```

## Testing
After implementing this fix:

1. Deploy the updated configuration
2. Run the EMR Serverless job
3. Verify that PostgreSQL writes complete successfully
4. Check logs for successful JDBC operations

## Version Compatibility
- PostgreSQL JDBC Driver: 42.7.4 (latest stable, optimized for Java 17)
- EMR 7.9.0: Spark 3.5.x, Java 17
- Compatible with PostgreSQL 9.x, 10.x, 11.x, 12.x, 13.x, 14.x, 15.x, 16.x, 17.x
- Fully compatible with Java 17 (EMR 7.9.0 requirement)

## Troubleshooting

### If the driver still can't be found:
1. Verify the JAR URL is accessible
2. Check EMR execution role permissions for external JAR access
3. Consider using S3-hosted JAR instead of Maven Central

### For connection timeouts:
1. Verify EMR Serverless is in the correct VPC
2. Check security group rules allow PostgreSQL connections (port 5432)
3. Verify RDS security groups allow connections from EMR subnet

## Related Files
- `src/jobs/job.py` - Spark session configuration
- `src/jobs/dbaccess/postgres_connectivity.py` - PostgreSQL connection logic
- `bin/build_aws_package.sh` - Deployment configuration

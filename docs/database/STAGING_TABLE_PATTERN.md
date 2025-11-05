# Staging Table Pattern for Aurora RDS Postgres

## Overview

The **staging table pattern** is a best practice for writing large datasets to a production table that's under constant load from concurrent queries. This approach minimizes lock contention and improves overall system reliability.

## The Problem

When writing directly to the `entity` table:
- **Long lock times**: DELETE + INSERT operations can hold locks for 10-30 minutes
- **Blocked queries**: SELECT queries from applications wait for write locks to release
- **Cascading delays**: Users experience slow response times
- **Retry failures**: Timeouts more likely with concurrent operations

## The Solution: Staging Tables

```
┌─────────────────────────────────────────────────────────────┐
│                    STAGING TABLE PATTERN                     │
└─────────────────────────────────────────────────────────────┘

Step 1: Create Staging Table
┌──────────────────────────┐
│ CREATE TEMP TABLE        │  ← Fast, no locks on entity table
│ entity_staging_xxx       │
└──────────────────────────┘

Step 2: Write to Staging (10-20 minutes)
┌──────────────────────────┐
│ INSERT INTO staging      │  ← No impact on entity table
│ 34M rows via JDBC        │  ← Queries continue normally
└──────────────────────────┘

Step 3: Atomic Commit (30-60 seconds)
┌──────────────────────────┐
│ BEGIN TRANSACTION        │
│ DELETE FROM entity       │  ← Only 30-60s of locks!
│ INSERT INTO entity       │
│   SELECT * FROM staging  │
│ COMMIT                   │
└──────────────────────────┘
```

## Benefits

| Metric | Direct Write | Staging Table | Improvement |
|--------|-------------|---------------|-------------|
| **Lock time on entity table** | 15-30 min | 30-60 sec | **95-97% reduction** |
| **Impact on queries** | High | Minimal | **Queries run normally** |
| **Write reliability** | Lower | Higher | **Fewer timeout errors** |
| **Rollback capability** | Difficult | Easy | **Full transaction safety** |
| **Validation before commit** | No | Yes | **Data quality checks** |

## Implementation

### 1. Create Staging Table

```python
from jobs.dbaccess.postgres_connectivity import create_and_prepare_staging_table

# Creates a temporary staging table with same schema as entity
staging_table_name = create_and_prepare_staging_table(
    conn_params=conn_params,
    dataset_value="my-dataset"
)

# Returns: "entity_staging_abc123_20250930_143022"
```

**What happens:**
- Creates temporary table: `entity_staging_<hash>_<timestamp>`
- Same schema as `entity` table
- Automatically cleaned up after session
- Verifies `entity` table exists

### 2. Write to Staging Table

```python
from jobs.dbaccess.postgres_connectivity import write_to_postgres

# Write DataFrame to staging table (not entity table)
write_to_postgres(
    df=df,
    dataset=data_set,
    conn_params=conn_params,
    target_table=staging_table_name,  # ← Key parameter!
    batch_size=5000,
    num_partitions=20
)
```

**What happens:**
- All data written to staging table
- No locks on `entity` table
- Concurrent queries unaffected
- Can take 10-20 minutes for large datasets

### 3. Commit to Production

```python
from jobs.dbaccess.postgres_connectivity import commit_staging_to_production

# Atomically move data from staging to entity table
result = commit_staging_to_production(
    conn_params=conn_params,
    staging_table_name=staging_table_name,
    dataset_value=data_set
)

print(f"Deleted: {result['rows_deleted']:,}")
print(f"Inserted: {result['rows_inserted']:,}")
print(f"Duration: {result['total_duration']:.2f}s")
```

**What happens:**
1. **BEGIN TRANSACTION**
2. Count rows in staging table
3. DELETE FROM entity WHERE dataset = 'my-dataset'  (indexed, fast)
4. INSERT INTO entity SELECT * FROM staging  (bulk insert, fast)
5. Verify row counts match
6. **COMMIT** (or ROLLBACK on error)
7. Drop staging table

**Critical**: The DELETE + INSERT happens in a **single transaction**, so locks on `entity` table are held for only 30-60 seconds instead of 15-30 minutes!

## Complete Example

### Automated Usage (Recommended)

The easiest way is to use the enhanced `_write_dataframe_to_postgres_jdbc` function which handles everything automatically:

```python
from jobs.main_collection_data import _write_dataframe_to_postgres_jdbc

# This automatically uses staging table for entity table
_write_dataframe_to_postgres_jdbc(
    df=df_entity,
    table_name='entity',
    data_set='my-dataset',
    use_staging=True  # ← Default, recommended for production
)
```

**Log output:**
```
_write_dataframe_to_postgres_jdbc: Using STAGING TABLE pattern for entity table
_write_dataframe_to_postgres_jdbc: This minimizes lock contention on production table
_write_dataframe_to_postgres_jdbc: Step 1/3 - Creating staging table
create_and_prepare_staging_table: Created staging table: entity_staging_a1b2c3d4_20250930_143022
_write_dataframe_to_postgres_jdbc: Step 2/3 - Writing 34,845,893 rows to staging table
_write_to_postgres_optimized: Writing to staging table, skipping create_table
_write_to_postgres_optimized: Successfully wrote data to entity_staging_a1b2c3d4_20250930_143022
_write_dataframe_to_postgres_jdbc: Step 3/3 - Committing staging data to production entity table
commit_staging_to_production: Deleted 34,600,000 old rows in 15.23s
commit_staging_to_production: Inserted 34,845,893 new rows in 42.67s
commit_staging_to_production: ✓ Transaction committed successfully
_write_dataframe_to_postgres_jdbc: ✓ STAGING COMMIT SUCCESSFUL
_write_dataframe_to_postgres_jdbc: Lock time on entity table: ~57.90s (vs. several minutes with direct write)
```

### Manual Usage (Advanced)

For more control, use the functions directly:

```python
from jobs.dbaccess.postgres_connectivity import (
    create_and_prepare_staging_table,
    write_to_postgres,
    commit_staging_to_production,
    get_aws_secret
)

conn_params = get_aws_secret("development")
data_set = "my-dataset"

try:
    # Step 1: Create staging table
    staging_table = create_and_prepare_staging_table(
        conn_params=conn_params,
        dataset_value=data_set
    )
    print(f"Created staging table: {staging_table}")
    
    # Step 2: Write to staging
    write_to_postgres(
        df=df_entity,
        dataset=data_set,
        conn_params=conn_params,
        target_table=staging_table
    )
    print(f"Wrote data to staging table")
    
    # Optional: Validate data in staging table
    # You can query staging table here to verify data quality
    
    # Step 3: Commit to production
    result = commit_staging_to_production(
        conn_params=conn_params,
        staging_table_name=staging_table,
        dataset_value=data_set
    )
    
    if result["success"]:
        print(f"Success! Replaced {result['rows_deleted']:,} with {result['rows_inserted']:,} rows")
        print(f"Lock duration: {result['total_duration']:.2f}s")
    else:
        print(f"Failed: {result['error']}")
        
except Exception as e:
    print(f"Error: {e}")
    # Staging table will be auto-cleaned up on connection close
```

## How It Works Internally

### Staging Table Creation

```sql
-- Generate unique table name
-- entity_staging_<hash>_<timestamp>

CREATE TEMP TABLE entity_staging_a1b2c3d4_20250930_143022 (
    dataset TEXT,
    end_date DATE,
    entity TEXT,
    entry_date DATE,
    geojson JSONB,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    json JSONB,
    name TEXT,
    organisation_entity BIGINT,
    point GEOMETRY(POINT, 4326),
    prefix TEXT,
    reference TEXT,
    start_date DATE,
    typology TEXT
) ON COMMIT PRESERVE ROWS;

-- Verify main entity table exists
CREATE TABLE IF NOT EXISTS entity (...);
```

### Data Writing

```python
# PySpark writes to staging table using optimized JDBC
df.write \
    .mode("append") \
    .option("batchsize", "5000") \
    .option("numPartitions", "20") \
    .jdbc(url=jdbc_url, table="entity_staging_a1b2c3d4_20250930_143022")
```

### Atomic Commit

```sql
BEGIN;

-- Step 1: Verify staging table has data
SELECT COUNT(*) FROM entity_staging_a1b2c3d4_20250930_143022;
-- Result: 34,845,893 rows

-- Step 2: Delete existing data (indexed delete, very fast)
DELETE FROM entity WHERE dataset = 'my-dataset';
-- 34,600,000 rows deleted in ~15 seconds

-- Step 3: Bulk insert from staging (optimized insert, fast)
INSERT INTO entity (dataset, end_date, entity, ...)
SELECT dataset, end_date, entity, ...
FROM entity_staging_a1b2c3d4_20250930_143022;
-- 34,845,893 rows inserted in ~43 seconds

-- Step 4: Verify counts match
-- If mismatch detected: ROLLBACK

COMMIT;
-- Total lock time: ~58 seconds

-- Step 5: Cleanup
DROP TABLE IF EXISTS entity_staging_a1b2c3d4_20250930_143022;
```

## Performance Comparison

### Scenario: 34.8M rows to entity table

#### Without Staging (Direct Write)

```
┌─────────────────────────────────────┐
│ DIRECT WRITE TO ENTITY TABLE        │
├─────────────────────────────────────┤
│ 1. DELETE FROM entity (10 min)      │ ← Locks table
│ 2. INSERT 34.8M rows (15 min)       │ ← Locks table
│                                     │
│ Total time: 25 minutes              │
│ Lock duration: 25 MINUTES           │ ← PROBLEM!
│ Queries blocked: YES (25 min)       │ ← PROBLEM!
│ Timeout risk: HIGH                  │ ← PROBLEM!
└─────────────────────────────────────┘
```

#### With Staging

```
┌─────────────────────────────────────┐
│ STAGING TABLE PATTERN                │
├─────────────────────────────────────┤
│ 1. Create staging (1 sec)           │ ← No entity locks
│ 2. Write to staging (18 min)        │ ← No entity locks
│ 3. Atomic commit (58 sec)           │ ← BRIEF entity locks
│                                     │
│ Total time: 19 minutes              │
│ Lock duration: 58 SECONDS           │ ← SOLUTION!
│ Queries blocked: NO (18 min)        │ ← SOLUTION!
│                 YES (58 sec only)   │ ← ACCEPTABLE!
│ Timeout risk: LOW                   │ ← SOLUTION!
└─────────────────────────────────────┘
```

### Key Metrics

| Operation | Direct Write | Staging Table | Difference |
|-----------|-------------|---------------|------------|
| **Total write time** | 25 min | 19 min | 24% faster |
| **Entity table lock time** | 25 min | 58 sec | **96% reduction** |
| **Queries blocked** | 25 min | 58 sec | **96% reduction** |
| **Timeout errors** | Frequent | Rare | **Much more reliable** |
| **Rollback capability** | Manual/difficult | Automatic | **Safer** |

## Error Handling

### Automatic Fallback

If staging table approach fails, the system automatically falls back to direct write:

```python
try:
    # Try staging approach
    staging_table = create_and_prepare_staging_table(...)
    write_to_postgres(..., target_table=staging_table)
    commit_staging_to_production(...)
except Exception as e:
    logger.error(f"Staging approach failed: {e}")
    logger.info("Falling back to direct write")
    # Fallback to direct write
    write_to_postgres(..., target_table=None)  # Direct to entity
```

### Transaction Safety

The commit operation is fully transactional:

```python
result = commit_staging_to_production(...)

if result["success"]:
    # All data committed successfully
    # Counts verified
    # Staging table dropped
else:
    # Transaction was rolled back
    # No data changed in entity table
    # Error details in result["error"]
```

## Monitoring

### Log Messages to Watch

```
# Success pattern
create_and_prepare_staging_table: Created staging table: entity_staging_xxx
_write_to_postgres_optimized: Successfully wrote data to entity_staging_xxx
commit_staging_to_production: Deleted X rows, Inserted Y rows in Z.ZZs
commit_staging_to_production: ✓ Transaction committed successfully

# Warning pattern (fallback triggered)
Staging approach failed: <error>
Falling back to direct write to entity table
```

### Metrics to Track

```python
# From commit_result
{
    "success": True,
    "rows_deleted": 34600000,
    "rows_inserted": 34845893,
    "delete_duration": 15.23,    # DELETE FROM entity time
    "insert_duration": 42.67,    # INSERT INTO entity time
    "total_duration": 57.90      # Total lock time on entity table
}
```

## Best Practices

### ✅ DO

1. **Use staging for entity table writes**
   ```python
   _write_dataframe_to_postgres_jdbc(df, 'entity', dataset, use_staging=True)
   ```

2. **Monitor lock duration**
   - Target: < 60 seconds for commit
   - Alert if > 120 seconds

3. **Validate before commit** (optional)
   ```python
   # After writing to staging, before commit
   # Run data quality checks on staging table
   validate_data_quality(staging_table)
   ```

4. **Use retry logic**
   - Both functions have built-in retries
   - Exponential backoff for transient failures

### ❌ DON'T

1. **Don't use for small datasets**
   - Overhead not worth it for < 10K rows
   - Direct write is fine for small datasets

2. **Don't skip row count verification**
   - Always check `result["success"]`
   - Verify `rows_inserted` matches expectations

3. **Don't manually manage staging tables**
   - Use provided functions
   - They handle cleanup automatically

4. **Don't forget the fallback**
   - Always have direct write as fallback
   - Log when fallback is triggered

## Configuration Options

### Staging Table Creation

```python
create_and_prepare_staging_table(
    conn_params=conn_params,
    dataset_value=data_set,
    max_retries=3  # Retry on connection failures
)
```

### Commit Operation

```python
commit_staging_to_production(
    conn_params=conn_params,
    staging_table_name=staging_table,
    dataset_value=data_set,
    max_retries=3  # Retry on transient failures
)
```

### Disable Staging (if needed)

```python
# Disable staging, use direct write
_write_dataframe_to_postgres_jdbc(
    df=df,
    table_name='entity',
    data_set=dataset,
    use_staging=False  # Direct write to entity table
)
```

## Troubleshooting

### Issue: Staging table creation fails

**Symptoms:**
```
create_and_prepare_staging_table: Error: permission denied for schema pg_temp
```

**Solution:**
- Check database user has CREATE TEMP TABLE permission
- Verify connection parameters are correct
- Check max_connections limit on database

### Issue: Commit takes too long

**Symptoms:**
```
commit_staging_to_production: Deleted 34M rows in 180s
```

**Solution:**
- Ensure `dataset` column is indexed on entity table:
  ```sql
  CREATE INDEX idx_entity_dataset ON entity(dataset);
  ```
- Consider batch deletion for very large datasets (> 50M rows)

### Issue: Row count mismatch

**Symptoms:**
```
commit_staging_to_production: Row count mismatch! Staging: 100000, Inserted: 99999
```

**Solution:**
- Transaction is automatically rolled back
- Check for NULL values in required columns
- Verify data types match between staging and entity
- Review application logs for specific errors

## Summary

The staging table pattern provides:

✅ **96% reduction** in entity table lock time  
✅ **Minimal impact** on concurrent queries  
✅ **Higher reliability** for large writes  
✅ **Transaction safety** with automatic rollback  
✅ **Data validation** before committing to production  
✅ **Automatic fallback** if staging fails  

**Recommendation**: Use staging tables for all entity table writes in production, especially when the table is under constant query load.

## Quick Reference

For a condensed version of this guide, see: [Staging Table Quick Reference](STAGING_TABLE_QUICK_REFERENCE.md)

## Further Reading

- [PostgreSQL Temporary Tables](https://www.postgresql.org/docs/current/sql-createtable.html)
- [Bulk Insert Performance](https://www.postgresql.org/docs/current/populate.html)
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- Aurora RDS Best Practices for bulk loading

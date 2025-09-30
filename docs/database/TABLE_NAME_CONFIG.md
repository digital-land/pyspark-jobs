# Entity Table Name Configuration

## 🎯 Quick Change Guide

To switch between `pyspark_entity` (temporary) and `entity` (production):

### Single File to Edit

**File:** `src/jobs/dbaccess/postgres_connectivity.py`

**Line:** 18

**Current (Temporary):**
```python
ENTITY_TABLE_NAME = "pyspark_entity"  # TODO: TEMPORARY - change to "entity" when ready
```

**To Revert to Production:**
```python
ENTITY_TABLE_NAME = "entity"  # Production table name
```

---

## ✅ What This Changes

Changing `ENTITY_TABLE_NAME` automatically updates:

1. **JDBC Writes** - All PySpark DataFrame writes to PostgreSQL
2. **Aurora S3 Imports** - CSV imports from S3 to Aurora
3. **Staging Tables** - Temporary staging table names (e.g., `entity_staging_...` or `pyspark_entity_staging_...`)
4. **Table Creation** - CREATE TABLE and CREATE INDEX statements
5. **Data Cleanup** - DELETE statements for dataset-specific cleanup

---

## 📋 Current Configuration

| Component | Current Value |
|-----------|---------------|
| **Entity Table Name** | `pyspark_entity` |
| **Staging Table Pattern** | `pyspark_entity_staging_{hash}_{timestamp}` |
| **Dataset Index Name** | `idx_pyspark_entity_dataset` |

---

## 🔍 Files Using This Configuration

- `src/jobs/dbaccess/postgres_connectivity.py` (defines the constant)
- `src/jobs/main_collection_data.py` (imports and uses it)
- All other modules use it indirectly through these two files

---

## ⚠️ Important Notes

1. **Single Source of Truth**: Only change the value in `postgres_connectivity.py`
2. **No Code Changes Needed**: All references use the `ENTITY_TABLE_NAME` constant
3. **Staging Tables**: Will automatically match the production table name
4. **Index Names**: Will automatically update to match table name

---

## 🔄 Change History

| Date | Table Name | Reason |
|------|------------|--------|
| 2025-09-30 | `pyspark_entity` | Temporary - dev issue raised by infra team |
| TBD | `entity` | Revert to production table |

---

## 🚀 After Changing

After updating `ENTITY_TABLE_NAME`, rebuild and deploy:

```bash
cd /Users/2193780/github_repo/pyspark-jobs
make build  # or your build command
make deploy # or your deployment command
```

No other code changes required! ✨

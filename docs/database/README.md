# Database Documentation

## Guides

- [Database Connectivity](./DATABASE_CONNECTIVITY.md) — Why we use pg8000 instead of psycopg2-binary for EMR compatibility
- [PostgreSQL JDBC Configuration](./POSTGRESQL_JDBC_CONFIGURATION.md) — Setting up JDBC drivers for EMR Serverless

## Database Architecture

```
PySpark Application
├── Python Layer (pg8000)     # DDL, transactions, atomic swap
│   └── Pure Python driver    # EMR compatible, no binaries
│
└── Java/Spark Layer (JDBC)   # Bulk DataFrame writes via staging tables
    └── PostgreSQL JDBC driver # Loaded via --jars parameter
```

## Write Pattern

All database writes use the staging table pattern in `postgres_writer_utils.py`:

1. CREATE a temporary staging table matching the target schema
2. JDBC bulk write the DataFrame into the staging table
3. Atomic swap: DELETE old dataset rows + INSERT from staging + DROP staging

This minimises lock contention on the target table.

---

[Back to docs](../README.md)

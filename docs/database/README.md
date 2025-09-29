# Database Documentation

This directory contains all documentation related to database connectivity, configuration, and performance optimization for PostgreSQL and Aurora PostgreSQL.

## 📚 Available Guides

### 🚀 **Performance & Optimization**
- **[CSV S3 Import Guide](./CSV_S3_IMPORT_GUIDE.md)** ⭐ **NEW**
  - Aurora S3 import with automatic CSV staging and cleanup
  - Up to 80% faster than JDBC for large datasets
  - Simple one-flag control with automatic fallback
  
- **[PostgreSQL Performance Optimization](./POSTGRESQL_PERFORMANCE_OPTIMIZATION.md)** 
  - Complete guide with 3-10x speedup techniques
  - Optimized JDBC, COPY protocol, async batching
  - Automatic performance recommendations
  
- **[Aurora PostgreSQL Optimization](./AURORA_POSTGRESQL_OPTIMIZATION.md)**
  - AWS Aurora-specific optimizations
  - Aurora vs standard PostgreSQL differences
  - S3 import capabilities and setup

### 🔧 **Connectivity & Configuration**
- **[Database Connectivity](./DATABASE_CONNECTIVITY.md)**
  - Why we use pg8000 instead of psycopg2-binary
  - EMR Serverless compatibility requirements
  - Cross-platform deployment considerations

- **[PostgreSQL JDBC Configuration](./POSTGRESQL_JDBC_CONFIGURATION.md)**
  - Setting up JDBC drivers for EMR Serverless 7.9.0
  - Maven Central vs S3-hosted JARs
  - Production deployment strategies

### 🔍 **Troubleshooting & Legacy**
- **[Fix psycopg2 Issues](./FIX_PSYCOPG2.md)**
  - Historical context and migration away from psycopg2-binary
  - Platform compatibility solutions (deprecated)
  - Why pg8000 is the current solution

## 🎯 Quick Navigation

| Need | Start Here |
|------|------------|
| **⚡ Faster Aurora Imports** | [CSV S3 Import Guide](./CSV_S3_IMPORT_GUIDE.md) |
| **Performance Issues** | [PostgreSQL Performance Optimization](./POSTGRESQL_PERFORMANCE_OPTIMIZATION.md) |
| **Aurora Setup** | [Aurora PostgreSQL Optimization](./AURORA_POSTGRESQL_OPTIMIZATION.md) |
| **Connection Problems** | [Database Connectivity](./DATABASE_CONNECTIVITY.md) |
| **JDBC Driver Issues** | [PostgreSQL JDBC Configuration](./POSTGRESQL_JDBC_CONFIGURATION.md) |
| **psycopg2 Errors** | [Fix psycopg2 Issues](./FIX_PSYCOPG2.md) |

## 🔧 Database Architecture Overview

```
PySpark Application
├── Python Layer (pg8000)     # Direct database operations
│   └── Pure Python driver    # EMR compatible, no binaries
│
└── Java/Spark Layer (JDBC)   # DataFrame operations  
    └── PostgreSQL JDBC driver # Loaded via --jars parameter
```

## 📊 Performance Summary

| Method | Use Case | Performance Gain | Setup Required |
|--------|----------|------------------|----------------|
| **🆕 CSV S3 Import** | Aurora + any dataset | 50-80% faster | Aurora IAM role |
| **Optimized JDBC** | Most datasets | 3-5x faster | None |
| **Aurora S3 Import** | Aurora + large datasets | 5-8x faster | IAM role setup |
| **Async Batches** | Memory-fit datasets | 4-8x faster | None |

## 🚨 Common Issues

1. **Connection Timeouts** → Check VPC/security group configuration
2. **JDBC Driver Not Found** → Verify --jars parameter in EMR configuration  
3. **psycopg2 Import Errors** → Use pg8000 instead (see [Database Connectivity](./DATABASE_CONNECTIVITY.md))
4. **Slow Performance** → Implement optimizations from [Performance Guide](./POSTGRESQL_PERFORMANCE_OPTIMIZATION.md)

---

[← Back to Main Documentation](../README.md)

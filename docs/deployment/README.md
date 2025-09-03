# Deployment Documentation

This directory contains all documentation related to building, packaging, and deploying PySpark jobs to AWS EMR Serverless.

## 📚 Available Guides

### 🏗️ **Build & Packaging**
- **[Build Guide](./BUILD_GUIDE.md)**
  - Complete guide for building and deploying to AWS EMR Serverless
  - Automated build script usage (`build_aws_package.sh`)
  - S3 upload and EMR configuration

- **[Build Artifacts Guide](./BUILD_ARTIFACTS_GUIDE.md)**
  - Understanding the dual-artifact approach
  - `dist/` vs `build_output/` directories
  - Development vs production workflows

### 📊 **Data Processing & Output**
- **[Parquet to SQLite Guide](./PARQUET_TO_SQLITE_GUIDE.md)**
  - Converting parquet files to SQLite databases
  - Separate dedicated script for clean architecture
  - Local analysis, backup, and portable data files

## 🎯 Quick Navigation

| Need | Start Here |
|------|------------|
| **First-time Deployment** | [Build Guide](./BUILD_GUIDE.md) |
| **Understanding Build Output** | [Build Artifacts Guide](./BUILD_ARTIFACTS_GUIDE.md) |
| **Creating SQLite Backups** | [Parquet to SQLite Guide](./PARQUET_TO_SQLITE_GUIDE.md) |

## 🚀 Quick Start Commands

### Build and Deploy
```bash
# Build only
./build_aws_package.sh

# Build and upload to S3
./build_aws_package.sh --upload

# Convert parquet to SQLite
./bin/convert_parquet_to_sqlite.sh \
  --input s3://bucket/parquet/ \
  --output ./output.sqlite
```

## 📦 Deployment Architecture

```
Build Process:
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Source Code │ => │ Python Package  │ => │ EMR Deployment  │
└─────────────┘    │ (wheel + deps)  │    │ (S3 artifacts)  │
                   └─────────────────┘    └─────────────────┘

Artifacts Created:
├── dist/                     # Development artifacts
│   └── pyspark_jobs-*.whl   # Python wheel package
│
└── build_output/            # Deployment artifacts
    ├── whl_pkg/             # Application wheel
    ├── dependencies/        # External dependencies
    ├── entry_script/        # Job entry points
    └── upload_to_s3.sh      # Upload script
```

## 📊 File Processing Flow

```
ETL Pipeline:
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Raw Data    │ => │ Main Pipeline   │ => │ Parquet Files   │
│ (CSV/JSON)  │    │ (Spark ETL)     │    │ (S3 Storage)    │
└─────────────┘    └─────────────────┘    └─────────────────┘
                                                     │
Optional Output:                                     │
┌─────────────┐    ┌─────────────────┐               │
│ SQLite Files│ <= │ SQLite Converter│ <─────────────┘
│ (Portable)  │    │ (Separate Tool) │
└─────────────┘    └─────────────────┘
```

## 🔧 Configuration Files

| File | Purpose | Location |
|------|---------|----------|
| `build_aws_package.sh` | Main build script | Project root |
| `setup.py` | Python packaging | Project root |
| `requirements.txt` | Production deps | Project root |
| `requirements-local.txt` | Development deps | Project root |
| `deployment_manifest.json` | EMR configuration | `build_output/` |

## 🚨 Common Deployment Issues

1. **Build Fails** 
   - Check Python 3.8+ and pip installation
   - Verify `setup.py` is valid
   - Ensure in project root directory

2. **Upload Fails**
   - Verify AWS CLI configuration
   - Check S3 bucket permissions
   - Confirm bucket exists and is accessible

3. **EMR Job Fails**
   - Check deployment manifest for correct S3 paths
   - Verify all dependencies are packaged
   - Review EMR Serverless logs

4. **SQLite Conversion Issues**
   - Check parquet file permissions
   - Verify Spark memory settings for large datasets
   - Use partitioned method for datasets > 500k rows

## 📈 Best Practices

### Development Workflow
1. **Test locally first** using `dist/` artifacts
2. **Use organized structure** from `build_output/`
3. **Verify checksums** if deployment issues occur
4. **Monitor build logs** for warnings or errors

### Production Deployment
1. **Use automated builds** via CI/CD
2. **Test in staging** environment first
3. **Monitor EMR execution** logs
4. **Backup artifacts** to version-controlled storage

---

[← Back to Main Documentation](../README.md)

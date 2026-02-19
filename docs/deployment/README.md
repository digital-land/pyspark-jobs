# Deployment Documentation

This directory contains all documentation related to building, packaging, and deploying PySpark jobs to AWS EMR Serverless.

## 📚 Available Guides

### Build & Packaging
- **[Build Guide](./BUILD_GUIDE.md)** — Building and deploying to AWS EMR Serverless
- **[Build Artifacts Guide](./BUILD_ARTIFACTS_GUIDE.md)** — Understanding `dist/` vs `build_output/` directories

## Quick Start

```bash
# Build only
./bin/build_aws_package.sh

# Build and upload to S3
./bin/build_aws_package.sh --upload
```

## Deployment Architecture

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

## Key Files

| File | Purpose |
|------|---------|
| `bin/build_aws_package.sh` | Main build script |
| `setup.py` | Python packaging |
| `requirements.txt` | Production dependencies |

---

[Back to docs](../README.md)

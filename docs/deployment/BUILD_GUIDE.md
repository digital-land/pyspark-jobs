# PySpark Jobs - AWS Deployment Build Guide

This guide explains how to build and deploy your PySpark jobs to AWS EMR Serverless.

## Quick Start

### Build Only
```bash
./bin/build_aws_package.sh
```

### Build and Upload to S3
```bash
./bin/build_aws_package.sh --upload
```

## What the Build Script Does

The `build_aws_package.sh` script automates the entire build process:

1. **Validates Environment** - Checks for Python 3, pip, and required files
2. **Cleans Previous Builds** - Removes old build artifacts and cache files
3. **Builds Wheel Package** - Creates your application wheel using `setup.py`
4. **Creates Dependencies Archive** - Packages external dependencies (pg8000, PyYAML, typing-extensions)
5. **Copies Entry Scripts** - Prepares your job entry points
6. **Generates Deployment Manifest** - Creates configuration for EMR Serverless
7. **Creates Upload Script** - Generates S3 upload commands
8. **Optionally Uploads to S3** - If `--upload` flag is provided

## Output Structure

After running the build script, you'll find these files in `build_output/`:

```
build_output/
├── whl_pkg/
│   └── pyspark_jobs-0.1.0-py3-none-any.whl
├── dependencies/
│   └── dependencies.zip
├── entry_script/
│   └── run_main.py
├── deployment_manifest.json
└── upload_to_s3.sh
```

## S3 Upload Destinations

The script uploads files to these S3 locations:

- **Wheel Package**: `s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/whl_pkg/`
- **Dependencies**: `s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/dependencies/`
- **Entry Scripts**: `s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/entry_script/`

## EMR Serverless Configuration

Use the configuration from `deployment_manifest.json` for your EMR Serverless job:

```json
{
  "entryPoint": "s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/entry_script/run_main.py",
  "sparkSubmitParameters": "--py-files s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl,s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/dependencies/dependencies.zip"
}
```

## Prerequisites

- Python 3.8+
- pip
- AWS CLI (for automatic upload)
- Proper AWS credentials configured (for upload)

## Troubleshooting

### Build Fails
- Ensure you're in the project root directory
- Check that `setup.py` exists and is valid
- Verify Python 3 and pip are installed

### Upload Fails
- Verify AWS CLI is installed and configured
- Check AWS credentials have S3 write permissions
- Confirm the S3 bucket exists and is accessible

### EMR Job Fails
- Check the deployment manifest for correct S3 paths
- Verify all dependencies are properly packaged
- Review EMR Serverless logs for specific error messages

## Manual Upload

If automatic upload fails, you can manually upload using:

```bash
cd build_output
./upload_to_s3.sh
```

Or upload files individually:

```bash
aws s3 cp build_output/whl_pkg/*.whl s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/whl_pkg/
aws s3 cp build_output/dependencies/dependencies.zip s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/dependencies/
aws s3 cp build_output/entry_script/run_main.py s3://development-emr-serverless-pyspark-jobs-codepackage/pkg/entry_script/
```

## Customization

To modify the build process:

1. **Change S3 bucket**: Edit the `S3_BUCKET` variable in `build_aws_package.sh`
2. **Add dependencies**: Update the pip install command in the `build_dependencies()` function
3. **Add entry scripts**: Modify the `copy_entry_scripts()` function
4. **Change Python version**: Update the `PYTHON_VERSION` variable

## Integration with CI/CD

The build script can be integrated into your CI/CD pipeline:

```bash
# In your CI/CD script
./bin/build_aws_package.sh --upload

# Or for just building (manual upload later)
./bin/build_aws_package.sh
```

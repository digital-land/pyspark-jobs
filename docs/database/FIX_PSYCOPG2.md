# Database Connectivity: Why We Don't Use psycopg2-binary

## Current Solution (Recommended)

**We have eliminated `psycopg2-binary` entirely** and use `pg8000>=1.30.0` for all environments.

See [DATABASE_CONNECTIVITY.md](./DATABASE_CONNECTIVITY.md) for complete details.

## Historical Context: The psycopg2-binary Problem

This document explains why we moved away from `psycopg2-binary` for EMR Serverless compatibility.

### The Problem

When using `psycopg2-binary` in EMR Serverless, you encounter this error:

```
ModuleNotFoundError: No module named 'psycopg2._psycopg'
```

### Root Cause

- `psycopg2-binary` contains platform-specific compiled C extensions
- Development builds on macOS download macOS wheels  
- EMR Serverless runs on Amazon Linux and expects Linux x86_64 binaries
- Binary incompatibility causes module loading failures

### Why This Was Problematic

1. **Platform Dependency**: Required different builds for different platforms
2. **Build Complexity**: Needed Docker or Linux environment for EMR builds
3. **Maintenance Overhead**: Had to manage platform-specific dependency resolution
4. **Deployment Issues**: Different behavior between local and production

## Legacy Solutions (No Longer Used)

### Historical Solution 1: Docker Build (Deprecated)

Previously, we used Docker to build Linux-compatible wheels:

```bash
# Build with Linux-compatible dependencies
./build_aws_package.sh --upload
```

The script now:
1. Downloads `psycopg2-binary` wheel specifically for Linux x86_64
2. Uses `--platform linux_x86_64` to ensure compatibility
3. Targets Python 3.9 (EMR Serverless default)

### Solution 2: Manual Linux-Compatible Build

If you need to manually ensure Linux compatibility:

```bash
# Create a temporary directory
mkdir temp_linux_deps
cd temp_linux_deps

# Download Linux-specific wheel
pip download --only-binary=psycopg2-binary \
    --platform linux_x86_64 \
    --python-version 39 \
    --abi cp39 \
    psycopg2-binary==2.9.7

# Create your dependencies archive with this wheel
# ... (rest of your build process)
```

### Solution 3: Using Docker for Cross-Platform Build

For guaranteed compatibility, use Docker with a Linux container:

```bash
# Build dependencies in a Linux container
docker run --rm -v $(pwd):/workspace python:3.9-slim bash -c "
    cd /workspace
    pip install --target ./linux_deps psycopg2-binary==2.9.7 PyYAML==6.0.1
    cd linux_deps && zip -r ../dependencies.zip .
"
```

### Solution 4: Alternative - Use psycopg2 (without binary)

If binary compatibility continues to be an issue, consider using `psycopg2` (source distribution) instead:

**Note:** This requires PostgreSQL development libraries on the build system.

```python
# In requirements.txt, replace:
# psycopg2-binary>=2.9.0

# With:
psycopg2>=2.9.0
```

## Verification

To verify your dependencies are Linux-compatible:

```bash
# Extract and inspect your dependencies.zip
unzip -l dependencies.zip | grep psycopg2
```

Look for files like:
- `psycopg2/_psycopg.cpython-39-x86_64-linux-gnu.so` (Linux)
- NOT `psycopg2/_psycopg.cpython-39-darwin.so` (macOS)

## EMR Serverless Configuration

Ensure your EMR Serverless job is configured correctly:

```json
{
    "sparkSubmitParameters": "--py-files s3://your-bucket/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl,s3://your-bucket/pkg/dependencies/dependencies.zip"
}
```

## Testing

Test your fix by:

1. Rebuilding with the updated script: `./build_aws_package.sh`
2. Uploading to S3: `cd build_output && ./upload_to_s3.sh`
3. Running a test EMR Serverless job
4. Checking that PostgreSQL connections work

## Additional Notes

- EMR Serverless uses Amazon Linux 2
- Python version is typically 3.9.x
- Always test in a staging environment first
- Consider using environment-specific requirements files if you support multiple platforms

## Troubleshooting

If you still encounter issues:

1. Check EMR Serverless logs for detailed error messages
2. Verify Python version compatibility
3. Ensure all dependencies are included in the zip file
4. Test with a minimal script that only imports psycopg2

```python
# Minimal test script
try:
    import psycopg2
    print("psycopg2 import successful!")
    print(f"psycopg2 version: {psycopg2.__version__}")
except ImportError as e:
    print(f"psycopg2 import failed: {e}")
```

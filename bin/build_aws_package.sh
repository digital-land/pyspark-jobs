#!/bin/bash

# Build AWS Package Script for PySpark Jobs
# This script creates all necessary files for AWS EMR Serverless deployment
# 
# Usage: ./bin/build_aws_package.sh [--upload]
#   --upload: Optional flag to automatically upload to S3

set -e  # Exit on any error

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/build_output"
S3_BUCKET="development-emr-serverless-pyspark-jobs-codepackage"
PYTHON_VERSION="python3"
PIP_CMD=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to get pip command
get_pip_command() {
    if command_exists pip; then
        echo "pip"
    elif command_exists pip3; then
        echo "pip3"
    else
        return 1
    fi
}

# Validate prerequisites
validate_environment() {
    print_status "Validating build environment..."
    
    if ! command_exists $PYTHON_VERSION; then
        print_error "Python 3 not found. Please install Python 3."
        exit 1
    fi
    
    PIP_CMD=$(get_pip_command)
    if [[ $? -ne 0 ]]; then
        print_error "pip not found. Please install pip."
        exit 1
    fi
    
    print_success "Using Python: $PYTHON_VERSION, pip: $PIP_CMD"
    
    # Check if we're in the right directory
    if [[ ! -f "$PROJECT_DIR/setup.py" ]]; then
        print_error "setup.py not found. Please run this script from the project root directory."
        exit 1
    fi
    
    print_success "Environment validation passed"
}

# Clean previous builds
clean_build() {
    print_status "Cleaning previous build artifacts..."
    
    rm -rf "$PROJECT_DIR/dist/"
    rm -rf "$PROJECT_DIR/build/"
    rm -rf "$PROJECT_DIR/temp_venv/"
    rm -rf "$PROJECT_DIR/build_venv/"
    rm -rf "$BUILD_DIR"
    
    # Clean Python cache
    find "$PROJECT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$PROJECT_DIR" -name "*.pyc" -delete 2>/dev/null || true
    
    print_success "Build artifacts cleaned"
}

# Create build directory structure
setup_build_dir() {
    print_status "Setting up build directory structure..."
    
    mkdir -p "$BUILD_DIR/whl_pkg"
    mkdir -p "$BUILD_DIR/dependencies"
    mkdir -p "$BUILD_DIR/entry_script"
    mkdir -p "$BUILD_DIR/jars"
    
    print_success "Build directory structure created at $BUILD_DIR"
}

# Build wheel package
build_wheel() {
    print_status "Building wheel package..."
    
    cd "$PROJECT_DIR"
    
    # Create temporary virtual environment for building
    $PYTHON_VERSION -m venv build_venv
    source build_venv/bin/activate
    
    # Install build dependencies
    pip install --quiet wheel setuptools
    
    # Build the wheel
    $PYTHON_VERSION setup.py bdist_wheel --quiet
    
    # Deactivate build environment
    deactivate
    
    # Copy wheel to build directory
    WHEEL_FILE=$(ls dist/*.whl | head -n 1)
    if [[ -f "$WHEEL_FILE" ]]; then
        cp "$WHEEL_FILE" "$BUILD_DIR/whl_pkg/"
        WHEEL_NAME=$(basename "$WHEEL_FILE")
        print_success "Wheel package built: $WHEEL_NAME"
    else
        print_error "Failed to build wheel package"
        exit 1
    fi
    
    # Clean up build environment (keep dist/ and build/ for development)
    rm -rf build_venv/
}

# Create Python virtual environment archive for --archives submission
build_dependencies() {
    print_status "Creating Python environment archive..."

    cd "$PROJECT_DIR"

    # Check if Docker is available for Linux-compatible builds
    if command_exists docker && [[ "$1" == "--docker" ]]; then
        build_dependencies_docker
        return
    fi

    # Build a full venv so compiled extensions (.so files) are on the real filesystem
    # when extracted by EMR Serverless. This is the AWS-recommended approach for packages
    # with native extensions (e.g. pydantic-core). Submitted via --archives, not --py-files.
    $PYTHON_VERSION -m venv environment
    source environment/bin/activate

    pip install --quiet --upgrade pip

    print_status "Installing EMR dependencies into venv..."
    pip install --quiet -r "$PROJECT_DIR/requirements.txt" || {
        print_error "Failed to install EMR dependencies."
        exit 1
    }

    # Install delta-spark Python bindings without pyspark (pre-installed on EMR).
    pip install --quiet --no-deps "delta-spark>=3.2.0,<4.0.0" || {
        print_error "Failed to install delta-spark Python bindings."
        exit 1
    }

    # venv-pack creates a relocatable venv archive — it patches absolute paths,
    # symlinks, and pyvenv.cfg so the venv works when extracted at any location
    # on EMR Serverless. Plain tar of a venv retains hardcoded build paths and breaks.
    pip install --quiet venv-pack

    deactivate

    # Verify dependencies and ensure AWS SDK packages are NOT included
    print_status "Verifying dependencies..."
    SITE_PACKAGES=$(ls -d "$PROJECT_DIR/environment/lib/python"*/site-packages)

    aws_packages=("boto3" "botocore")
    found_aws_packages=()
    for pkg in "${aws_packages[@]}"; do
        if [[ -d "$SITE_PACKAGES/$pkg" ]]; then
            found_aws_packages+=("$pkg")
        fi
    done
    if [[ ${#found_aws_packages[@]} -gt 0 ]]; then
        print_error "AWS SDK packages found in environment: ${found_aws_packages[*]}"
        print_error "These are pre-installed on EMR Serverless and must not be bundled."
        exit 1
    fi
    print_success "✅ AWS SDK packages correctly excluded"

    required_deps=("pg8000" "pydantic" "pydantic_core")
    missing_deps=()
    for dep in "${required_deps[@]}"; do
        if [[ ! -d "$SITE_PACKAGES/$dep" ]]; then
            missing_deps+=("$dep")
        fi
    done
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        print_error "Required dependencies missing from environment: ${missing_deps[*]}"
        exit 1
    fi
    print_success "✅ All required dependencies present (including pydantic_core)"

    "$PROJECT_DIR/environment/bin/venv-pack" -p "$PROJECT_DIR/environment" -o "$BUILD_DIR/dependencies/environment.tar.gz"

    # Cleanup
    cd "$PROJECT_DIR"
    rm -rf environment/

    print_success "Environment archive created: environment.tar.gz"
}

# Create environment archive using Docker for Linux compatibility
build_dependencies_docker() {
    print_status "Creating Python environment archive using Docker..."

    cat > temp_dockerfile << 'EOF'
FROM python:3.9-slim
RUN apt-get update && apt-get install -y tar && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY requirements.txt /build/
RUN python -m venv environment && \
    environment/bin/pip install --quiet --upgrade pip && \
    environment/bin/pip install --quiet -r requirements.txt && \
    environment/bin/pip install --quiet --no-deps "delta-spark>=3.2.0,<4.0.0" && \
    environment/bin/pip install --quiet venv-pack && \
    environment/bin/venv-pack -o environment.tar.gz
EOF

    docker build -f temp_dockerfile -t pyspark-deps-builder . || {
        print_error "Failed to build Docker image"
        rm -f temp_dockerfile
        exit 1
    }

    docker create --name temp-deps-container pyspark-deps-builder || {
        print_error "Failed to create temporary container"
        rm -f temp_dockerfile
        exit 1
    }

    docker cp temp-deps-container:/build/environment.tar.gz "$BUILD_DIR/dependencies/" || {
        print_error "Failed to copy environment archive from Docker container"
        docker rm temp-deps-container
        rm -f temp_dockerfile
        exit 1
    }

    docker rm temp-deps-container
    docker rmi pyspark-deps-builder >/dev/null 2>&1 || true
    rm -f temp_dockerfile

    print_success "Environment archive created using Docker: environment.tar.gz"
}

# Download JDBC drivers (optional)
download_jdbc_drivers() {
    print_status "Downloading JDBC drivers for S3 hosting (optional)..."
    
    # PostgreSQL JDBC driver for EMR 7.9.0
    POSTGRESQL_JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
    POSTGRESQL_JAR_NAME="postgresql-42.7.4.jar"
    
    if command_exists curl; then
        curl -L "$POSTGRESQL_JAR_URL" -o "$BUILD_DIR/jars/$POSTGRESQL_JAR_NAME" || {
            print_warning "Failed to download PostgreSQL JDBC driver. Continuing without it."
            print_warning "You can manually download and upload the JAR to S3 if needed."
            return
        }
        print_success "PostgreSQL JDBC driver downloaded: $POSTGRESQL_JAR_NAME"
    elif command_exists wget; then
        wget "$POSTGRESQL_JAR_URL" -O "$BUILD_DIR/jars/$POSTGRESQL_JAR_NAME" || {
            print_warning "Failed to download PostgreSQL JDBC driver. Continuing without it."
            print_warning "You can manually download and upload the JAR to S3 if needed."
            return
        }
        print_success "PostgreSQL JDBC driver downloaded: $POSTGRESQL_JAR_NAME"
    else
        print_warning "Neither curl nor wget available. Skipping JDBC driver download."
        print_warning "You can manually download and upload the JAR to S3 if needed."
        return
    fi
    
    print_status "📦 JDBC drivers ready for S3 upload in build_output/jars/"
}

# Copy entry scripts
copy_entry_scripts() {
    print_status "Copying entry scripts..."
    
    # Copy the main entry script
    if [[ -f "$PROJECT_DIR/entry_points/run_main.py" ]]; then
        cp "$PROJECT_DIR/entry_points/run_main.py" "$BUILD_DIR/entry_script/"
        print_success "Entry script copied: entry_points/run_main.py"
    else
        print_error "Entry script not found: entry_points/run_main.py"
        exit 1
    fi
    
    # Copy any additional scripts if needed
    # Add more entry scripts here if you have them
}

# Generate deployment manifest
generate_manifest() {
    print_status "Generating deployment manifest..."
    
    cat > "$BUILD_DIR/deployment_manifest.json" << EOF
{
    "project": "pyspark-jobs",
    "version": "0.1.0",
    "build_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "s3_bucket": "$S3_BUCKET",
    "files": {
        "wheel_package": {
            "local_path": "whl_pkg/$WHEEL_NAME",
            "s3_path": "s3://$S3_BUCKET/pkg/whl_pkg/$WHEEL_NAME",
            "description": "Main application wheel package"
        },
        "dependencies": {
            "local_path": "dependencies/dependencies.zip",
            "s3_path": "s3://$S3_BUCKET/pkg/dependencies/dependencies.zip",
            "description": "External Python dependencies archive"
        },
        "entry_script": {
            "local_path": "entry_script/run_main.py",
            "s3_path": "s3://$S3_BUCKET/pkg/entry_script/run_main.py",
            "description": "Job entry point script"
        }
    },
    "emr_serverless_config": {
        "entryPoint": "s3://$S3_BUCKET/pkg/entry_script/run_main.py",
        "sparkSubmitParameters": "--py-files s3://$S3_BUCKET/pkg/whl_pkg/$WHEEL_NAME,s3://$S3_BUCKET/pkg/dependencies/dependencies.zip --jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
        "note": "PostgreSQL JDBC driver is loaded from Maven Central. For production, consider hosting the JAR in your own S3 bucket for better reliability."
    }
}
EOF
    
    print_success "Deployment manifest generated: deployment_manifest.json"
}

# Generate upload script
generate_upload_script() {
    print_status "Generating S3 upload script..."
    
    cat > "$BUILD_DIR/upload_to_s3.sh" << 'EOF'
#!/bin/bash

# S3 Upload Script
# Generated by build_aws_package.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
S3_BUCKET="development-emr-serverless-pyspark-jobs-codepackage"

echo "Uploading files to S3..."

# Upload wheel package
echo "Uploading wheel package..."
aws s3 cp "$SCRIPT_DIR/whl_pkg/"*.whl "s3://$S3_BUCKET/pkg/whl_pkg/"
aws s3 cp "$SCRIPT_DIR/whl_pkg/"*.whl "s3://$S3_BUCKET/emr-data-processing/src0/whl_pkg/"

# Upload dependencies
echo "Uploading dependencies..."
aws s3 cp "$SCRIPT_DIR/dependencies/dependencies.zip" "s3://$S3_BUCKET/pkg/dependencies/"
aws s3 cp "$SCRIPT_DIR/dependencies/dependencies.zip" "s3://$S3_BUCKET/emr-data-processing/src0/dependencies/dependencies.zip"

# Upload entry script
echo "Uploading entry script..."
aws s3 cp "$SCRIPT_DIR/entry_script/run_main.py" "s3://$S3_BUCKET/pkg/entry_script/"
aws s3 cp "$SCRIPT_DIR/entry_script/run_main.py" "s3://$S3_BUCKET/emr-data-processing/src0/entry_script/"

# Upload JDBC drivers (if they exist)
if [ -d "$SCRIPT_DIR/jars" ] && [ "$(ls -A $SCRIPT_DIR/jars)" ]; then
    echo "Uploading JDBC drivers..."
    aws s3 cp "$SCRIPT_DIR/jars/" "s3://$S3_BUCKET/pkg/jars/" --recursive
    aws s3 cp "$SCRIPT_DIR/jars/" "s3://$S3_BUCKET/emr-data-processing/src0/jars/" --recursive
else
    echo "No JDBC drivers to upload (jars directory empty or doesn't exist)"
fi

echo "Upload completed successfully!"
echo ""
echo "Files uploaded to:"
echo "  - s3://$S3_BUCKET/pkg/whl_pkg/"
echo "  - s3://$S3_BUCKET/pkg/dependencies/"
echo "  - s3://$S3_BUCKET/pkg/entry_script/"
if [ -d "$SCRIPT_DIR/jars" ] && [ "$(ls -A $SCRIPT_DIR/jars)" ]; then
    echo "  - s3://$S3_BUCKET/pkg/jars/"
fi
EOF
    
    chmod +x "$BUILD_DIR/upload_to_s3.sh"
    print_success "S3 upload script generated: upload_to_s3.sh"
}

# Upload to S3 if requested
upload_to_s3() {
    if [[ "$1" == "--upload" ]]; then
        print_status "Uploading to S3..."
        
        if ! command_exists aws; then
            print_error "AWS CLI not found. Please install AWS CLI to upload files."
            print_warning "You can manually upload files using the generated upload_to_s3.sh script"
            return
        fi
        
        cd "$BUILD_DIR"
        ./upload_to_s3.sh
        print_success "Files uploaded to S3"
    fi
}

# Display build summary
show_summary() {
    print_status "Build Summary:"
    echo ""
    echo "Build completed successfully! 📦"
    echo ""
    echo "Generated files in $BUILD_DIR:"
    echo "  📁 whl_pkg/"
    echo "     └── $WHEEL_NAME"
    echo "  📁 dependencies/"
    echo "     └── dependencies.zip"
    echo "  📁 entry_script/"
    echo "     └── run_main.py"
    if [ -d "$BUILD_DIR/jars" ] && [ "$(ls -A $BUILD_DIR/jars)" ]; then
        echo "  📁 jars/"
        echo "     └── postgresql-42.7.4.jar"
    fi
    echo "  📄 deployment_manifest.json"
    echo "  📄 upload_to_s3.sh"
    echo ""
    echo "Next steps:"
    if [[ "$1" != "--upload" ]]; then
        echo "  1. Run: cd $BUILD_DIR && ./upload_to_s3.sh"
        echo "     (or run this script with --upload flag)"
    fi
    echo "  2. Configure your EMR Serverless job using the paths in deployment_manifest.json"
    echo "  3. Test your job deployment"
    echo ""
    print_success "Build process completed! 🚀"
}

# Main execution
main() {
    echo "🔨 PySpark Jobs AWS Package Builder"
    echo "=================================="
    echo ""
    
    validate_environment
    clean_build
    setup_build_dir
    build_wheel
    build_dependencies "$1"
    download_jdbc_drivers
    copy_entry_scripts
    generate_manifest
    generate_upload_script
    upload_to_s3 "$1"
    show_summary "$1"
}

# Run main function with all arguments
main "$@"

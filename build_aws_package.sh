#!/bin/bash

# Build AWS Package Script for PySpark Jobs
# This script creates all necessary files for AWS EMR Serverless deployment
# 
# Usage: ./build_aws_package.sh [--upload]
#   --upload: Optional flag to automatically upload to S3

set -e  # Exit on any error

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$PROJECT_DIR/build_output"
S3_BUCKET="development-pyspark-jobs-codepackage"
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
    
    # Clean up build environment
    rm -rf build_venv/
}

# Create dependencies archive
build_dependencies() {
    print_status "Creating dependencies archive..."
    
    cd "$PROJECT_DIR"
    
    # Create temporary virtual environment
    $PYTHON_VERSION -m venv temp_venv
    source temp_venv/bin/activate
    
    # Upgrade pip to avoid warnings
    pip install --quiet --upgrade pip
    
    # Install only the external dependencies (not pre-installed in EMR Serverless)
    print_status "Installing external dependencies..."
    
    # Install psycopg2-binary first with binary preference
    pip install --quiet --only-binary=psycopg2-binary psycopg2-binary==2.9.7 || {
        print_warning "Failed to install psycopg2-binary==2.9.7, trying latest version..."
        pip install --quiet --only-binary=psycopg2-binary psycopg2-binary || {
            print_error "Failed to install psycopg2-binary. Please install PostgreSQL development libraries."
            exit 1
        }
    }
    
    # Install other dependencies
    pip install --quiet PyYAML==6.0.1 typing-extensions==4.8.0
    
    # Create dependencies archive
    cd temp_venv/lib/python*/site-packages/
    zip -r "$BUILD_DIR/dependencies/dependencies.zip" .
    
    # Deactivate and cleanup
    cd "$PROJECT_DIR"
    deactivate
    rm -rf temp_venv/
    
    print_success "Dependencies archive created: dependencies.zip"
}

# Copy entry scripts
copy_entry_scripts() {
    print_status "Copying entry scripts..."
    
    # Copy the main entry script
    if [[ -f "$PROJECT_DIR/src/jobs/run_main.py" ]]; then
        cp "$PROJECT_DIR/src/jobs/run_main.py" "$BUILD_DIR/entry_script/"
        print_success "Entry script copied: run_main.py"
    else
        print_error "Entry script not found: src/jobs/run_main.py"
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
        "sparkSubmitParameters": "--py-files s3://$S3_BUCKET/pkg/whl_pkg/$WHEEL_NAME,s3://$S3_BUCKET/pkg/dependencies/dependencies.zip"
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
S3_BUCKET="development-pyspark-jobs-codepackage"

echo "Uploading files to S3..."

# Upload wheel package
echo "Uploading wheel package..."
aws s3 cp "$SCRIPT_DIR/whl_pkg/"*.whl "s3://$S3_BUCKET/pkg/whl_pkg/"

# Upload dependencies
echo "Uploading dependencies..."
aws s3 cp "$SCRIPT_DIR/dependencies/dependencies.zip" "s3://$S3_BUCKET/pkg/dependencies/"

# Upload entry script
echo "Uploading entry script..."
aws s3 cp "$SCRIPT_DIR/entry_script/run_main.py" "s3://$S3_BUCKET/pkg/entry_script/"

echo "Upload completed successfully!"
echo ""
echo "Files uploaded to:"
echo "  - s3://$S3_BUCKET/pkg/whl_pkg/"
echo "  - s3://$S3_BUCKET/pkg/dependencies/"
echo "  - s3://$S3_BUCKET/pkg/entry_script/"
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
    build_dependencies
    copy_entry_scripts
    generate_manifest
    generate_upload_script
    upload_to_s3 "$1"
    show_summary "$1"
}

# Run main function with all arguments
main "$@"

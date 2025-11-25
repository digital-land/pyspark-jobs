#!/bin/bash

# Virtual Environment Setup Script for PySpark Jobs
# This script creates and configures a Python virtual environment with all dependencies

set -e  # Exit on any error

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_NAME=".venv"
VENV_DIR="$PROJECT_DIR/$VENV_NAME"
PYTHON_VERSION="python3"
MIN_PYTHON_VERSION="3.8"

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

# Function to check Python version
check_python_version() {
    local python_cmd=$1
    if ! command_exists "$python_cmd"; then
        return 1
    fi
    
    local version=$($python_cmd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    local major=$(echo "$version" | cut -d. -f1)
    local minor=$(echo "$version" | cut -d. -f2)
    local min_major=$(echo "$MIN_PYTHON_VERSION" | cut -d. -f1)
    local min_minor=$(echo "$MIN_PYTHON_VERSION" | cut -d. -f2)
    
    if [ "$major" -gt "$min_major" ] || ([ "$major" -eq "$min_major" ] && [ "$minor" -ge "$min_minor" ]); then
        return 0
    else
        return 1
    fi
}

# Function to find suitable Python version
find_python() {
    local python_candidates=("python3" "python3.11" "python3.10" "python3.9" "python3.8" "python")
    
    for cmd in "${python_candidates[@]}"; do
        if check_python_version "$cmd"; then
            echo "$cmd"
            return 0
        fi
    done
    
    return 1
}

# Function to create virtual environment
create_venv() {
    print_status "Creating virtual environment at $VENV_DIR..."
    
    if [ -d "$VENV_DIR" ]; then
        print_warning "Virtual environment already exists at $VENV_DIR"
        read -p "Do you want to recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Removing existing virtual environment..."
            rm -rf "$VENV_DIR"
        else
            print_status "Using existing virtual environment"
            return 0
        fi
    fi
    
    "$PYTHON_VERSION" -m venv "$VENV_DIR"
    print_success "Virtual environment created successfully"
}

# Function to activate virtual environment
activate_venv() {
    print_status "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"
    
    # Verify activation
    if [[ "$VIRTUAL_ENV" != "$VENV_DIR" ]]; then
        print_error "Failed to activate virtual environment"
        exit 1
    fi
    
    print_success "Virtual environment activated: $VIRTUAL_ENV"
}

# Function to upgrade pip
upgrade_pip() {
    print_status "Upgrading pip to latest version..."
    pip install --upgrade pip setuptools wheel
    print_success "Pip upgraded successfully"
}

# Function to install dependencies
install_dependencies() {
    local env_type=$1
    
    case $env_type in
        "local"|"testing")
            print_status "Installing local testing dependencies..."
            pip install -r "$PROJECT_DIR/requirements-local.txt"
            ;;

        *)
            print_error "Unknown environment type: $env_type"
            print_status "Available options: local"
            exit 1
            ;;
    esac
    
    print_success "Dependencies installed successfully"
}

# Function to install package in development mode
install_package() {
    print_status "Installing pyspark-jobs package in development mode..."
    pip install -e .
    print_success "Package installed in development mode"
}

# Function to setup pre-commit hooks
setup_pre_commit() {
    if command_exists pre-commit; then
        print_status "Setting up pre-commit hooks..."
        pre-commit install
        print_success "Pre-commit hooks installed"
    else
        print_warning "pre-commit not available, skipping hook setup"
    fi
}

# Function to create .env file template
create_env_template() {
    local env_file="$PROJECT_DIR/.env"
    if [ ! -f "$env_file" ]; then
        print_status "Creating .env template file..."
        cat > "$env_file" << EOF
# PySpark Jobs Environment Configuration
# Copy this file and customize for your environment

# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=digital_land
DB_USER=postgres
DB_PASSWORD=password

# EMR Configuration
EMR_APPLICATION_ID=your-emr-application-id
EMR_EXECUTION_ROLE_ARN=your-execution-role-arn
EMR_JOB_ROLE_ARN=your-job-role-arn

# S3 Configuration
S3_BUCKET_NAME=your-s3-bucket
S3_CODE_PREFIX=code/
S3_DATA_PREFIX=data/

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json

# Development Configuration
ENVIRONMENT=development
DEBUG=true
EOF
        print_success ".env template created at $env_file"
        print_warning "Please customize the .env file with your specific configuration"
    else
        print_status ".env file already exists, skipping template creation"
    fi
}

# Function to display usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -t, --type TYPE     Environment type (production, development, emr)"
    echo "  -p, --python CMD    Python command to use (default: auto-detect)"
    echo "  -n, --name NAME     Virtual environment name (default: pyspark-jobs-venv)"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Create development environment"
    echo "  $0 --type production         # Create production environment"
    echo "  $0 --type emr                # Create EMR-compatible environment"
    echo "  $0 --python python3.9        # Use specific Python version"
}

# Function to display completion message
show_completion() {
    print_success "Virtual environment setup completed!"
    echo ""
    echo "To activate the environment in the future, run:"
    echo "  source $VENV_DIR/bin/activate"
    echo ""
    echo "To deactivate the environment, run:"
    echo "  deactivate"
    echo ""
    echo "Available commands after activation:"
    echo "  pytest                       # Run tests"
    echo "  black .                      # Format code"
    echo "  flake8 .                     # Lint code"
    echo "  mypy src/                    # Type checking"
    echo "  jupyter lab                  # Start Jupyter Lab"
    echo ""
    echo "Environment details:"
    echo "  Python: $(python --version)"
    echo "  Pip: $(pip --version)"
    echo "  Virtual env: $VIRTUAL_ENV"
}

# Main function
main() {
    local env_type="development"
    local custom_python=""
    local custom_name=""
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -t|--type)
                env_type="$2"
                shift 2
                ;;
            -p|--python)
                custom_python="$2"
                shift 2
                ;;
            -n|--name)
                custom_name="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Update configuration based on arguments
    if [ -n "$custom_name" ]; then
        VENV_NAME="$custom_name"
        VENV_DIR="$PROJECT_DIR/$VENV_NAME"
    fi
    
    print_status "Starting virtual environment setup for PySpark Jobs"
    print_status "Project directory: $PROJECT_DIR"
    print_status "Environment type: $env_type"
    
    # Find suitable Python version
    if [ -n "$custom_python" ]; then
        if check_python_version "$custom_python"; then
            PYTHON_VERSION="$custom_python"
        else
            print_error "Specified Python version $custom_python is not suitable (requires >= $MIN_PYTHON_VERSION)"
            exit 1
        fi
    else
        PYTHON_VERSION=$(find_python)
        if [ $? -ne 0 ]; then
            print_error "No suitable Python version found (requires >= $MIN_PYTHON_VERSION)"
            print_error "Please install Python $MIN_PYTHON_VERSION or higher"
            exit 1
        fi
    fi
    
    print_status "Using Python: $PYTHON_VERSION ($(${PYTHON_VERSION} --version))"
    
    # Setup steps
    create_venv
    activate_venv
    upgrade_pip
    install_dependencies "$env_type"
    install_package
    
    # Additional setup for development environment
    if [ "$env_type" = "development" ] || [ "$env_type" = "dev" ]; then
        setup_pre_commit
        create_env_template
    fi
    
    show_completion
}

# Run main function with all arguments
main "$@"

#!/bin/bash
# Parquet to SQLite Converter Script
# 
# This script provides a convenient wrapper for converting parquet files to SQLite databases.
# It handles common use cases and provides helpful defaults.

set -e  # Exit on any error

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
INPUT_PATH=""
OUTPUT_PATH=""
TABLE_NAME="data"
METHOD="optimized"
MAX_RECORDS=500000
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Show usage information
show_usage() {
    cat << EOF
Parquet to SQLite Converter

USAGE:
    $0 -i INPUT_PATH -o OUTPUT_PATH [OPTIONS]

REQUIRED ARGUMENTS:
    -i, --input INPUT_PATH      Input path to parquet files (local or S3)
    -o, --output OUTPUT_PATH    Output path for SQLite file(s) (local or S3)

OPTIONAL ARGUMENTS:
    -t, --table-name NAME       Name of table in SQLite database (default: data)
    -m, --method METHOD         Conversion method: optimized|single_file|partitioned (default: optimized)
    -r, --max-records NUM       Maximum records per SQLite file for partitioned method (default: 500000)
    -v, --verbose               Enable verbose logging
    -h, --help                  Show this help message

EXAMPLES:
    # Convert S3 parquet to local SQLite
    $0 -i s3://my-bucket/parquet/ -o ./data.sqlite

    # Create multiple SQLite files for large dataset
    $0 -i s3://my-bucket/parquet/ -o ./sqlite-output/ -m partitioned

    # Custom table name with verbose output
    $0 -i s3://my-bucket/parquet/ -o ./entity.sqlite -t entity -v

    # Upload SQLite to S3
    $0 -i s3://my-bucket/parquet/ -o s3://my-bucket/sqlite-backup/
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_PATH="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        -t|--table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        -m|--method)
            METHOD="$2"
            shift 2
            ;;
        -r|--max-records)
            MAX_RECORDS="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
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

# Validate required arguments
if [[ -z "$INPUT_PATH" ]]; then
    print_error "Input path is required (-i/--input)"
    show_usage
    exit 1
fi

if [[ -z "$OUTPUT_PATH" ]]; then
    print_error "Output path is required (-o/--output)"
    show_usage
    exit 1
fi

# Validate method
if [[ ! "$METHOD" =~ ^(optimized|single_file|partitioned)$ ]]; then
    print_error "Invalid method: $METHOD. Must be one of: optimized, single_file, partitioned"
    exit 1
fi

# Validate max records
if ! [[ "$MAX_RECORDS" =~ ^[0-9]+$ ]] || [[ "$MAX_RECORDS" -lt 1000 ]]; then
    print_error "Invalid max-records: $MAX_RECORDS. Must be a number >= 1000"
    exit 1
fi

# Build command arguments
PYTHON_CMD="python3"
SCRIPT_PATH="$PROJECT_ROOT/src/jobs/parquet_to_sqlite.py"
ARGS=(
    "--input" "$INPUT_PATH"
    "--output" "$OUTPUT_PATH"
    "--table-name" "$TABLE_NAME"
    "--method" "$METHOD"
    "--max-records" "$MAX_RECORDS"
)

if [[ "$VERBOSE" == "true" ]]; then
    ARGS+=("--verbose")
fi

# Print configuration
print_info "Parquet to SQLite Conversion"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📁 Input:        $INPUT_PATH"
echo "📁 Output:       $OUTPUT_PATH"
echo "🗄️  Table Name:   $TABLE_NAME"
echo "⚙️  Method:       $METHOD"
echo "📊 Max Records:  $MAX_RECORDS"
echo "🔍 Verbose:      $VERBOSE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check if Python script exists
if [[ ! -f "$SCRIPT_PATH" ]]; then
    print_error "Python script not found: $SCRIPT_PATH"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | cut -d' ' -f2)
print_info "Using Python $PYTHON_VERSION"

# Run the conversion
print_info "Starting parquet to SQLite conversion..."
echo

# Execute the Python script
if "$PYTHON_CMD" "$SCRIPT_PATH" "${ARGS[@]}"; then
    echo
    print_success "Parquet to SQLite conversion completed successfully!"
    print_info "Output location: $OUTPUT_PATH"
else
    exit_code=$?
    echo
    print_error "Parquet to SQLite conversion failed (exit code: $exit_code)"
    
    # Provide helpful troubleshooting tips
    echo
    print_warning "Troubleshooting tips:"
    echo "  • Check that input parquet files exist and are accessible"
    echo "  • Verify AWS credentials if using S3 paths"
    echo "  • For large datasets, try --method partitioned"
    echo "  • Use --verbose for detailed error information"
    echo "  • Check available disk space for local outputs"
    
    exit $exit_code
fi

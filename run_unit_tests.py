# !/usr/bin/env python3
"""
Comprehensive unit test runner for PySpark Jobs project.

This script provides a unified interface for running unit tests with proper
mocking, coverage reporting, and error handling.
"""

import subprocess


def setup_environment():
    """Set up the test environment with proper Python path."""
    project_root = Path(__file__).parent
    src_path = project_root / "src"

    # Add src to Python path
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    # Set environment variables for testing
    os.environ.update(
        {
            "PYTHONPATH": str(src_path),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
        }
    )


def run_command(cmd, cwd=None):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd, shell=True, cwd=cwd, capture_output=True, text=True, check=False
        )
        return result
    except Exception as e:
        print(f"Error running command '{cmd}': {e}")
        return None


def run_unit_tests(args):
    """Run unit tests with specified options."""
    setup_environment()

    # Base pytest command
    pytest_cmd = ["python", "-m", "pytest"]

    # Add test path
    if args.module:
        if args.module == "main":
            pytest_cmd.append("tests/unit/test_main_collection_data.py")
        elif args.module == "transform":
            pytest_cmd.append("tests/unit/test_transform_collection_data.py")
        elif args.module == "csv":
            pytest_cmd.append("tests/unit/test_csv_s3_writer.py")
        elif args.module == "utils":
            pytest_cmd.append("tests/unit/utils/")
        elif args.module == "dbaccess":
            pytest_cmd.append("tests/unit/dbaccess/")
        else:
            pytest_cmd.append(f"tests/unit/{args.module}")
    else:
        pytest_cmd.append("tests/unit/")

    # Add coverage options
    if args.coverage:
        pytest_cmd.extend(
            [
                "--cov=src",
                "--cov - report=term - missing",
            ]
        )

        if args.html_report:
            pytest_cmd.append("--cov - report=html:tests/unit/test_coverage/htmlcov")

        if args.xml_report:
            pytest_cmd.append(
                "--cov - report=xml:tests/unit/test_coverage/coverage.xml"
            )

        if args.fail_under:
            pytest_cmd.append(f"--cov - fail - under={args.fail_under}")

    # Add verbosity
    if args.verbose:
        pytest_cmd.append("-v")

    # Add specific test markers
    if args.markers:
        pytest_cmd.extend(["-m", args.markers])

    # Add parallel execution
    if args.parallel:
        pytest_cmd.extend(["-n", "auto"])

    # Add other options
    if args.tb_style:
        pytest_cmd.extend(["--tb", args.tb_style])

    if args.durations:
        pytest_cmd.extend(["--durations", str(args.durations)])

    # Run the command
    cmd_str = " ".join(pytest_cmd)
    print(f"Running: {cmd_str}")
    print("-" * 80)

    result = run_command(cmd_str)

    if result:
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        # Generate coverage report summary
        if args.coverage and result.returncode == 0:
            generate_coverage_summary()

        return result.returncode
    else:
        return 1


def generate_coverage_summary():
    """Generate a coverage summary report."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = f"tests/unit/test_coverage/coverage_summary_{timestamp}.txt"

    # Create coverage summary
    cmd = "python -m coverage report --show - missing"
    result = run_command(cmd)

    if result and result.returncode == 0:
        os.makedirs("tests/unit/test_coverage", exist_ok=True)
        with open(summary_file, "w") as f:
            f.write(f"Coverage Report Generated: {datetime.now()}\n")
            f.write("=" * 60 + "\n")
            f.write(result.stdout)

        print(f"\nCoverage summary saved to: {summary_file}")


def install_dependencies():
    """Install test dependencies."""
    print("Installing test dependencies...")

    # Check if we're in a virtual environment
    if not hasattr(sys, "real_prefix") and not (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    ):
        print("Warning: Not in a virtual environment. Consider activating test - venv.")

    dependencies = [
        "pytest>=7.0.0",
        "pytest - cov>=4.0.0",
        "pytest - mock>=3.10.0",
        "pytest - xdist>=3.0.0",  # for parallel execution
        "coverage>=7.0.0",
    ]

    for dep in dependencies:
        cmd = f"pip install {dep}"
        print(f"Installing {dep}...")
        result = run_command(cmd)
        if result and result.returncode != 0:
            print(f"Failed to install {dep}: {result.stderr}")
            return False

    print("Dependencies installed successfully!")
    return True


def check_test_environment():
    """Check if the test environment is properly set up."""
    issues = []

    # Check Python version
    if sys.version_info < (3, 8):
        issues.append("Python 3.8+ required")

    # Check required packages
    required_packages = ["pytest", "pyspark", "boto3"]
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            issues.append(f"Missing package: {package}")

    # Check test files exist
    test_files = [
        "tests/conftest.py",
        "tests/unit/conftest.py",
        "tests/unit/test_main_collection_data.py",
        "pytest.ini",
    ]

    for test_file in test_files:
        if not Path(test_file).exists():
            issues.append(f"Missing test file: {test_file}")

    if issues:
        print("Test environment issues found:")
        for issue in issues:
            print(f"  - {issue}")
        return False

    print("Test environment check passed!")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run unit tests for PySpark Jobs project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_unit_tests.py                          # Run all unit tests
  python run_unit_tests.py --coverage               # Run with coverage
  python run_unit_tests.py --module main            # Run main module tests
  python run_unit_tests.py --module utils --verbose # Run utils tests verbosely
  python run_unit_tests.py --coverage --html - report # Generate HTML coverage report
  python run_unit_tests.py --install - deps           # Install test dependencies
  python run_unit_tests.py --check - env              # Check test environment
        """,
    )

    parser.add_argument(
        "--module",
        "-m",
        choices=["main", "transform", "csv", "utils", "dbaccess"],
        help="Run tests for specific module",
    )

    parser.add_argument(
        "--coverage",
        "-c",
        action="store_true",
        help="Run tests with coverage reporting",
    )

    parser.add_argument(
        "--html - report", action="store_true", help="Generate HTML coverage report"
    )

    parser.add_argument(
        "--xml - report", action="store_true", help="Generate XML coverage report"
    )

    parser.add_argument(
        "--fail - under",
        type=int,
        default=80,
        help="Fail if coverage is under this percentage (default: 80)",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    parser.add_argument(
        "--markers",
        help="Run tests with specific markers (e.g., 'unit', 'integration')",
    )

    parser.add_argument(
        "--parallel", "-p", action="store_true", help="Run tests in parallel"
    )

    parser.add_argument(
        "--tb - style",
        choices=["short", "long", "auto", "line", "native", "no"],
        default="short",
        help="Traceback style",
    )

    parser.add_argument(
        "--durations", type=int, default=10, help="Show N slowest test durations"
    )

    parser.add_argument(
        "--install - deps", action="store_true", help="Install test dependencies"
    )

    parser.add_argument(
        "--check - env", action="store_true", help="Check test environment setup"
    )

    args = parser.parse_args()

    # Handle special commands
    if args.install_deps:
        return 0 if install_dependencies() else 1

    if args.check_env:
        return 0 if check_test_environment() else 1

    # Run tests
    return run_unit_tests(args)


if __name__ == "__main__":
    sys.exit(main())

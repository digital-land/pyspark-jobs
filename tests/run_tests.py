# !/usr/bin/env python3
"""
Test runner script for PySpark Jobs project.

This script provides a convenient way to run tests with different configurations
and generate coverage reports.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --coverage         # Run tests with coverage report
    python run_tests.py --parallel         # Run tests in parallel
    python run_tests.py --verbose          # Run tests with verbose output
"""

import argparse
import os
import subprocess
import sys


def run_command(cmd, description=""):
    """Run a command and handle errors."""
    print(f"\n{'=' * 60}")
    if description:
        print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'=' * 60}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error: Command failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"Error: Command not found: {cmd[0]}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test runner for PySpark Jobs project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py                    # Run all tests
  python run_tests.py --unit             # Run only unit tests
  python run_tests.py --integration      # Run only integration tests
  python run_tests.py --coverage         # Run with coverage report
  python run_tests.py --parallel         # Run tests in parallel
  python run_tests.py --verbose          # Verbose output
  python run_tests.py --quick            # Quick run (unit tests only, no coverage)
        """,
    )

    # Test selection options
    parser.add_argument("--unit", "-u", action="store_true", help="Run only unit tests")

    parser.add_argument(
        "--integration", "-i", action="store_true", help="Run only integration tests"
    )

    parser.add_argument(
        "--acceptance", "-a", action="store_true", help="Run only acceptance tests"
    )

    # Execution options
    parser.add_argument(
        "--coverage", "-c", action="store_true", help="Generate coverage report"
    )

    parser.add_argument(
        "--parallel", "-p", action="store_true", help="Run tests in parallel"
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    parser.add_argument(
        "--quick",
        "-q",
        action="store_true",
        help="Quick run (unit tests only, no coverage)",
    )

    parser.add_argument(
        "--html-report", action="store_true", help="Generate HTML coverage report"
    )

    parser.add_argument(
        "--fail-fast", "-x", action="store_true", help="Stop on first failure"
    )

    parser.add_argument(
        "--test-path",
        default="tests/",
        help="Path to test directory (default: tests/)",
    )

    args = parser.parse_args()

    # Build pytest command
    cmd = ["python", "-m", "pytest"]

    # Add test path
    if os.path.exists(args.test_path):
        cmd.append(args.test_path)
    else:
        print(f"Warning: Test path '{args.test_path}' does not exist")
        cmd.append("tests/")

    # Test selection
    if args.quick:
        cmd.extend(["-m", "unit"])
        print("Quick mode: Running unit tests only")
    elif args.unit:
        cmd.extend(["-m", "unit"])
    elif args.integration:
        cmd.extend(["-m", "integration"])
    elif args.acceptance:
        cmd.extend(["-m", "acceptance"])

    # Coverage options
    if args.coverage or (
        not args.quick
        and not args.unit
        and not args.integration
        and not args.acceptance
    ):
        cmd.extend(["--cov=src", "--cov-report=term-missing"])
        if args.html_report:
            cmd.append("--cov-report=html")

    # Execution options
    if args.parallel:
        cmd.extend(["-n", "auto"])

    if args.verbose:
        cmd.append("-v")

    if args.fail_fast:
        cmd.append("-x")

    # Additional pytest options
    cmd.extend(["--tb=short", "--strict-markers", "--strict-config"])

    # Check if we're in the right directory
    if not os.path.exists("src") or not os.path.exists("tests"):
        print("Error: Please run this script from the project root directory")
        print("Expected to find 'src' and 'tests' directories")
        return 1

    # Run the tests
    success = run_command(cmd, "Running pytest")

    if success:
        print(f"\n{'=' * 60}")
        print("✅ All tests passed!")

        if args.coverage or (
            not args.quick
            and not args.unit
            and not args.integration
            and not args.acceptance
        ):
            print("\n📊 Coverage report generated:")
            print("  - Terminal: See output above")
            if args.html_report:
                print("  - HTML: Open htmlcov/index.html in your browser")
            else:
                print("  - HTML: Run with --html-report to generate HTML report")

        print(f"{'=' * 60}")
        return 0
    else:
        print(f"\n{'=' * 60}")
        print("❌ Tests failed!")
        print(f"{'=' * 60}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

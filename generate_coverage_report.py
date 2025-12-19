#!/usr/bin/env python3
"""
Generate comprehensive coverage report for PySpark Jobs project.

This script runs all unit tests, generates coverage reports, and creates
a detailed summary of test coverage across all modules.
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET


def run_command(cmd, cwd=None):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            cwd=cwd, 
            capture_output=True, 
            text=True,
            check=False
        )
        return result
    except Exception as e:
        print(f"Error running command '{cmd}': {e}")
        return None


def setup_environment():
    """Set up the test environment."""
    project_root = Path(__file__).parent
    src_path = project_root / "src"
    
    # Add src to Python path
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Set environment variables
    os.environ.update({
        'PYTHONPATH': str(src_path),
        'SPARK_LOCAL_IP': '127.0.0.1',
        'PYSPARK_PYTHON': sys.executable,
        'PYSPARK_DRIVER_PYTHON': sys.executable,
    })


def run_tests_with_coverage():
    """Run all unit tests with coverage reporting."""
    print("Running unit tests with coverage...")
    print("=" * 60)
    
    # Create coverage directories
    os.makedirs("tests/unit/test_coverage", exist_ok=True)
    os.makedirs("tests/unit/test_coverage/htmlcov", exist_ok=True)
    
    # Run tests with coverage
    cmd = [
        "python", "-m", "pytest",
        "tests/unit/",
        "--cov=src",
        "--cov-report=html:tests/unit/test_coverage/htmlcov",
        "--cov-report=xml:tests/unit/test_coverage/coverage.xml",
        "--cov-report=term-missing",
        "--cov-report=json:tests/unit/test_coverage/coverage.json",
        "-v"
    ]
    
    result = run_command(" ".join(cmd))
    
    if result:
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    
    return False


def parse_coverage_xml():
    """Parse coverage XML report to extract detailed metrics."""
    xml_file = "tests/unit/test_coverage/coverage.xml"
    
    if not os.path.exists(xml_file):
        return None
    
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        coverage_data = {
            'overall': {},
            'packages': {},
            'classes': {}
        }
        
        # Overall coverage
        coverage_data['overall'] = {
            'line_rate': float(root.get('line-rate', 0)) * 100,
            'branch_rate': float(root.get('branch-rate', 0)) * 100,
            'lines_covered': int(root.get('lines-covered', 0)),
            'lines_valid': int(root.get('lines-valid', 0)),
            'branches_covered': int(root.get('branches-covered', 0)),
            'branches_valid': int(root.get('branches-valid', 0))
        }
        
        # Package-level coverage
        for package in root.findall('.//package'):
            package_name = package.get('name')
            coverage_data['packages'][package_name] = {
                'line_rate': float(package.get('line-rate', 0)) * 100,
                'branch_rate': float(package.get('branch-rate', 0)) * 100,
                'classes': {}
            }
            
            # Class-level coverage
            for class_elem in package.findall('.//class'):
                class_name = class_elem.get('name')
                filename = class_elem.get('filename')
                
                coverage_data['classes'][class_name] = {
                    'filename': filename,
                    'line_rate': float(class_elem.get('line-rate', 0)) * 100,
                    'branch_rate': float(class_elem.get('branch-rate', 0)) * 100
                }
        
        return coverage_data
        
    except Exception as e:
        print(f"Error parsing coverage XML: {e}")
        return None


def parse_coverage_json():
    """Parse coverage JSON report for additional details."""
    json_file = "tests/unit/test_coverage/coverage.json"
    
    if not os.path.exists(json_file):
        return None
    
    try:
        with open(json_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error parsing coverage JSON: {e}")
        return None


def generate_module_coverage_report(coverage_data):
    """Generate detailed module coverage report."""
    if not coverage_data:
        return "Coverage data not available"
    
    report = []
    report.append("# Module Coverage Report")
    report.append("=" * 50)
    report.append("")
    
    # Overall summary
    overall = coverage_data.get('overall', {})
    report.append(f"**Overall Coverage**: {overall.get('line_rate', 0):.1f}%")
    report.append(f"**Lines Covered**: {overall.get('lines_covered', 0)}/{overall.get('lines_valid', 0)}")
    report.append("")
    
    # Module breakdown
    report.append("## Module Breakdown")
    report.append("")
    report.append("| Module | Coverage | Lines | Status |")
    report.append("|--------|----------|-------|--------|")
    
    # Sort modules by coverage percentage
    modules = []
    for class_name, class_data in coverage_data.get('classes', {}).items():
        filename = class_data.get('filename', '')
        if filename.startswith('src/'):
            module_name = filename.replace('src/', '').replace('.py', '')
            coverage_pct = class_data.get('line_rate', 0)
            
            status = "✅" if coverage_pct >= 80 else "⚠️" if coverage_pct >= 60 else "❌"
            
            modules.append({
                'name': module_name,
                'coverage': coverage_pct,
                'filename': filename,
                'status': status
            })
    
    # Sort by coverage percentage (descending)
    modules.sort(key=lambda x: x['coverage'], reverse=True)
    
    for module in modules:
        report.append(f"| {module['name']} | {module['coverage']:.1f}% | - | {module['status']} |")
    
    report.append("")
    
    # Coverage targets
    report.append("## Coverage Targets")
    report.append("")
    report.append("- **Minimum Target**: 80%")
    report.append("- **Good Coverage**: 85%+")
    report.append("- **Excellent Coverage**: 90%+")
    report.append("")
    
    # Recommendations
    low_coverage_modules = [m for m in modules if m['coverage'] < 80]
    if low_coverage_modules:
        report.append("## Modules Needing Attention")
        report.append("")
        for module in low_coverage_modules:
            report.append(f"- **{module['name']}**: {module['coverage']:.1f}% coverage")
        report.append("")
    
    return "\n".join(report)


def generate_test_summary():
    """Generate test execution summary."""
    # Get test count by running pytest with collect-only
    result = run_command("python -m pytest tests/unit/ --collect-only -q")
    
    test_count = 0
    if result and result.returncode == 0:
        lines = result.stdout.split('\n')
        for line in lines:
            if 'test session starts' in line:
                continue
            if 'collected' in line and 'items' in line:
                try:
                    test_count = int(line.split()[0])
                except:
                    pass
    
    return {
        'total_tests': test_count,
        'test_files': len(list(Path('tests/unit').rglob('test_*.py'))),
        'timestamp': datetime.now().isoformat()
    }


def create_coverage_badge(coverage_percentage):
    """Create a simple coverage badge (text-based)."""
    if coverage_percentage >= 90:
        color = "brightgreen"
        status = "excellent"
    elif coverage_percentage >= 80:
        color = "green"
        status = "good"
    elif coverage_percentage >= 60:
        color = "yellow"
        status = "fair"
    else:
        color = "red"
        status = "poor"
    
    badge = f"![Coverage](https://img.shields.io/badge/coverage-{coverage_percentage:.0f}%25-{color})"
    return badge, status


def generate_comprehensive_report():
    """Generate comprehensive coverage report."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Parse coverage data
    coverage_xml = parse_coverage_xml()
    coverage_json = parse_coverage_json()
    test_summary = generate_test_summary()
    
    # Generate report
    report = []
    report.append("# PySpark Jobs - Unit Test Coverage Report")
    report.append("")
    report.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    if coverage_xml:
        overall_coverage = coverage_xml.get('overall', {}).get('line_rate', 0)
        badge, status = create_coverage_badge(overall_coverage)
        
        report.append(f"{badge}")
        report.append("")
        report.append(f"**Overall Coverage**: {overall_coverage:.1f}% ({status})")
        report.append(f"**Total Tests**: {test_summary.get('total_tests', 'N/A')}")
        report.append(f"**Test Files**: {test_summary.get('test_files', 'N/A')}")
        report.append("")
        
        # Add module coverage details
        module_report = generate_module_coverage_report(coverage_xml)
        report.append(module_report)
    else:
        report.append("⚠️ **Coverage data not available**")
        report.append("")
        report.append("Please run tests with coverage to generate this report:")
        report.append("```bash")
        report.append("python run_unit_tests.py --coverage --html-report")
        report.append("```")
    
    # Add testing instructions
    report.append("")
    report.append("## How to Run Tests")
    report.append("")
    report.append("```bash")
    report.append("# Run all tests with coverage")
    report.append("python run_unit_tests.py --coverage --html-report")
    report.append("")
    report.append("# Run specific module tests")
    report.append("python run_unit_tests.py --module utils --coverage")
    report.append("")
    report.append("# View HTML coverage report")
    report.append("open tests/unit/test_coverage/htmlcov/index.html")
    report.append("```")
    report.append("")
    
    # Add links
    report.append("## Coverage Reports")
    report.append("")
    report.append("- [HTML Coverage Report](htmlcov/index.html)")
    report.append("- [XML Coverage Report](coverage.xml)")
    report.append("- [JSON Coverage Report](coverage.json)")
    report.append("")
    
    # Save report
    report_content = "\n".join(report)
    report_file = f"tests/unit/test_coverage/COVERAGE_REPORT_{timestamp}.md"
    
    with open(report_file, 'w') as f:
        f.write(report_content)
    
    # Also save as latest
    latest_file = "tests/unit/test_coverage/COVERAGE_REPORT_LATEST.md"
    with open(latest_file, 'w') as f:
        f.write(report_content)
    
    return report_file, report_content


def main():
    """Main entry point."""
    print("PySpark Jobs - Coverage Report Generator")
    print("=" * 50)
    
    setup_environment()
    
    # Run tests with coverage
    success = run_tests_with_coverage()
    
    if not success:
        print("\n⚠️ Some tests failed, but generating coverage report anyway...")
    
    # Generate comprehensive report
    report_file, report_content = generate_comprehensive_report()
    
    print(f"\n✅ Coverage report generated: {report_file}")
    print(f"📊 View HTML report: tests/unit/test_coverage/htmlcov/index.html")
    
    # Print summary to console
    print("\n" + "=" * 50)
    print("COVERAGE SUMMARY")
    print("=" * 50)
    
    # Extract key metrics from report
    lines = report_content.split('\n')
    for line in lines:
        if line.startswith('**Overall Coverage**'):
            print(line.replace('**', ''))
        elif line.startswith('**Total Tests**'):
            print(line.replace('**', ''))
        elif line.startswith('**Test Files**'):
            print(line.replace('**', ''))
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
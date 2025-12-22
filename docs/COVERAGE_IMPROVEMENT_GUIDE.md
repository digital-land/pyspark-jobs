# Guide: Increasing Test Coverage from 39% to 80%

## Current Coverage Status
- **Overall Coverage**: 39% (2734 statements, 1662 missed)
- **Target Coverage**: 80%
- **Statements to Cover**: ~1120 additional statements

## Priority Modules (Ordered by Impact)

### 1. postgres_writer_utils.py (6% → 80%)
**Impact**: +97 statements covered
**Files Created**: `tests/unit/utils/test_postgres_writer_utils_extended.py`

**What to Test**:
- Connection creation with various credential scenarios
- Query execution (success, failure, timeout)
- DataFrame to table operations (create, insert, update, delete)
- Table existence checks
- Schema retrieval
- Transaction handling
- Error handling for all operations

### 2. s3_writer_utils.py (10% → 75%)
**Impact**: +296 statements covered

**What to Test**:
- S3 path validation
- File upload/download operations
- Multipart upload handling
- Batch operations
- Error handling for S3 operations
- Retry logic
- Permission errors

**Create**: `tests/unit/utils/test_s3_writer_utils_extended.py`

### 3. parquet_to_sqlite.py (10% → 70%)
**Impact**: +144 statements covered

**What to Test**:
- Parquet file reading
- SQLite database creation
- Data type conversions
- Batch insert operations
- Index creation
- Error handling
- Command-line interface

**Create**: `tests/unit/test_parquet_to_sqlite_extended.py`

### 4. transform_collection_data.py (45% → 85%)
**Impact**: +39 statements covered
**Files Created**: `tests/unit/test_transform_collection_data_extended.py`

**What to Test**:
- All transformation functions with edge cases
- Exception handling paths
- Different data types
- Missing columns scenarios
- Empty DataFrames
- Large datasets

### 5. csv_s3_writer.py (53% → 80%)
**Impact**: +102 statements covered

**What to Test**:
- DataFrame preparation for CSV
- S3 write operations
- Cleanup operations
- Aurora import methods
- Configuration handling
- Error scenarios

**Create**: `tests/unit/test_csv_s3_writer_extended.py`

## Implementation Strategy

### Step 1: Run Coverage Analysis
```bash
# Generate detailed coverage report
make test-coverage

# View HTML report to identify uncovered lines
open htmlcov/index.html
```

### Step 2: Create Extended Test Files
For each low-coverage module, create an `_extended.py` test file:

```python
# Example structure
class TestModuleExtended:
    """Extended tests to increase coverage."""
    
    def test_success_path(self):
        """Test normal operation."""
        pass
    
    def test_error_handling(self):
        """Test exception handling."""
        pass
    
    def test_edge_cases(self):
        """Test boundary conditions."""
        pass
    
    def test_different_inputs(self):
        """Test various input scenarios."""
        pass
```

### Step 3: Focus on Uncovered Lines
Use the HTML coverage report to identify:
- **Red lines**: Not executed
- **Yellow lines**: Partially executed (branches)
- **Green lines**: Fully covered

### Step 4: Test Patterns

#### Pattern 1: Exception Handling
```python
def test_function_with_exception(self):
    mock_obj = Mock()
    mock_obj.method.side_effect = Exception("Error")
    
    with pytest.raises(CustomError):
        function_under_test(mock_obj)
```

#### Pattern 2: Branch Coverage
```python
def test_function_with_condition_true(self):
    result = function(condition=True)
    assert result == expected_true_value

def test_function_with_condition_false(self):
    result = function(condition=False)
    assert result == expected_false_value
```

#### Pattern 3: Edge Cases
```python
def test_function_with_empty_input(self):
    result = function([])
    assert result == expected_empty_result

def test_function_with_large_input(self):
    result = function(large_dataset)
    assert result is not None
```

### Step 5: Run Tests Incrementally
```bash
# Test specific module
pytest tests/unit/utils/test_postgres_writer_utils_extended.py -v --cov=src/jobs/utils/postgres_writer_utils

# Check coverage improvement
make test-coverage
```

## Quick Wins (Immediate Impact)

### 1. Add Missing Exception Tests
Many functions have try-except blocks that aren't tested:
```python
def test_function_exception_path(self):
    with patch('module.dependency', side_effect=Exception("Error")):
        with pytest.raises(ModuleError):
            function_under_test()
```

### 2. Test All Code Branches
Look for `if/else` statements and test both paths:
```python
# If function has: if condition: ... else: ...
def test_condition_true(self):
    result = function(make_condition_true())
    assert result == expected_for_true

def test_condition_false(self):
    result = function(make_condition_false())
    assert result == expected_for_false
```

### 3. Test Error Messages
```python
def test_error_message_content(self):
    with pytest.raises(CustomError, match="specific error message"):
        function_that_raises_error()
```

## Estimated Timeline

| Phase | Target Coverage | Time Estimate | Files to Create |
|-------|----------------|---------------|-----------------|
| Phase 1 | 50-55% | 2-3 hours | 2 extended test files |
| Phase 2 | 60-70% | 4-6 hours | 3 extended test files |
| Phase 3 | 75-80% | 3-4 hours | 2 extended test files + refinement |

**Total**: 9-13 hours of focused testing work

## Monitoring Progress

### Daily Coverage Check
```bash
# Run full test suite with coverage
make test-coverage

# Check specific module
pytest tests/unit/utils/test_module.py --cov=src/jobs/utils/module --cov-report=term-missing
```

### Coverage Goals by Module
```
postgres_writer_utils.py:    6% → 80% (+74%)
s3_writer_utils.py:          10% → 75% (+65%)
parquet_to_sqlite.py:        10% → 70% (+60%)
transform_collection_data.py: 45% → 85% (+40%)
csv_s3_writer.py:            53% → 80% (+27%)
postgres_connectivity.py:    39% → 60% (+21%)
```

## Best Practices

1. **Mock External Dependencies**: Use `@patch` for AWS, database, and file system operations
2. **Test One Thing**: Each test should verify one specific behavior
3. **Use Descriptive Names**: Test names should describe what they test
4. **Test Negative Cases**: Don't just test success paths
5. **Keep Tests Fast**: Mock slow operations (network, disk I/O)
6. **Avoid Test Interdependence**: Each test should run independently

## Common Pitfalls to Avoid

1. **Over-mocking**: Don't mock the code you're testing
2. **Testing Implementation**: Test behavior, not implementation details
3. **Ignoring Edge Cases**: Empty inputs, None values, large datasets
4. **Skipping Error Paths**: Exception handling is critical
5. **Not Testing Cleanup**: Test that resources are properly released

## Next Steps

1. ✅ Create extended test files (2 files created)
2. ⏳ Run tests and verify coverage increase
3. ⏳ Create remaining extended test files
4. ⏳ Refine tests based on coverage report
5. ⏳ Document any intentionally uncovered code
6. ⏳ Set up coverage thresholds in CI/CD

## Maintenance

Once 80% coverage is achieved:
- Set minimum coverage threshold in `pytest.ini`
- Add coverage check to CI/CD pipeline
- Require tests for all new code
- Review coverage reports regularly

```ini
# pytest.ini
[pytest]
addopts = --cov=src --cov-fail-under=80
```

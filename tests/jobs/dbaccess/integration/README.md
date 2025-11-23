# Database Integration Tests

## Overview
These tests require a running PostgreSQL database on localhost.

## test_local_postgres.py

### Requirements
- PostgreSQL running on `localhost:5432`
- Database: `postgres`
- User: `postgres`
- Password: `postgres`

### Behavior
- Tests are **automatically skipped** if PostgreSQL is not available
- No manual configuration needed

### Running Tests

**Without PostgreSQL:**
```bash
pytest tests/jobs/dbaccess/integration/test_local_postgres.py -v
# Result: 4 skipped ⏭️
```

**With PostgreSQL:**
```bash
# Start PostgreSQL first
pytest tests/jobs/dbaccess/integration/test_local_postgres.py -v
# Result: 4 passed ✅
```

### Setup PostgreSQL (Optional)

**Using Docker:**
```bash
docker run --name test-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

**Using Homebrew (macOS):**
```bash
brew install postgresql
brew services start postgresql
createuser -s postgres
psql -U postgres -c "ALTER USER postgres PASSWORD 'postgres';"
```

### Test Coverage
1. `test_create_table` - Creates entity_test table
2. `test_insert_sample_data` - Inserts sample records
3. `test_verify_table_exists` - Verifies table creation
4. `test_verify_data_inserted` - Verifies data insertion

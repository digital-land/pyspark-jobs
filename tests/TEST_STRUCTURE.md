# Test Suite Structure

This test suite mirrors the `/src` directory structure with organized test types.

## Structure

```
tests/
├── jobs/                           # Mirrors src/jobs/
│   ├── config/
│   │   ├── unit/                   # Unit tests for config module
│   │   ├── integration/            # Integration tests for config module
│   │   └── acceptance/             # Acceptance tests for config module
│   ├── dbaccess/
│   │   ├── unit/                   # Unit tests for dbaccess module
│   │   ├── integration/            # Integration tests for dbaccess module
│   │   └── acceptance/             # Acceptance tests for dbaccess module
│   └── utils/
│       ├── unit/                   # Unit tests for utils module
│       ├── integration/            # Integration tests for utils module
│       └── acceptance/             # Acceptance tests for utils module
└── infra/                          # Mirrors src/infra/
    └── emr/
        ├── unit/                   # Unit tests for EMR module
        ├── integration/            # Integration tests for EMR module
        └── acceptance/             # Acceptance tests for EMR module
```

## Test Types

- **unit/**: Tests for individual functions/classes in isolation
- **integration/**: Tests for component interactions
- **acceptance/**: End-to-end tests for full workflows

## Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test type
pytest tests/jobs/utils/unit/
pytest tests/jobs/dbaccess/integration/
pytest tests/infra/emr/acceptance/

# Run tests for specific module
pytest tests/jobs/utils/
pytest tests/jobs/dbaccess/
```

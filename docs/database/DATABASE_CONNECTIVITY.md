# Database Connectivity in PySpark Jobs

## Overview

This project uses PostgreSQL connectivity for data processing and storage. Due to EMR Serverless compatibility requirements, we use pure Python database drivers instead of binary-compiled alternatives.

## Why pg8000 Instead of psycopg2-binary?

### The Problem with psycopg2-binary

`psycopg2-binary` is a popular PostgreSQL adapter for Python, but it has critical compatibility issues with EMR Serverless:

1. **Binary Compatibility Issues**: 
   - Contains platform-specific compiled C extensions
   - Built for specific CPU architectures (x86_64, ARM64)
   - Built for specific operating systems (macOS, Windows, Linux)

2. **EMR Serverless Environment**:
   - Runs on Amazon Linux x86_64
   - Requires Linux-compatible binaries
   - Development on macOS/Windows creates incompatible binaries

3. **Common Error**:
   ```
   ModuleNotFoundError: No module named 'psycopg2._psycopg'
   ```

### Our Solution: pg8000

We use `pg8000>=1.30.0` as our PostgreSQL driver because:

✅ **Pure Python**: No compiled C extensions, works everywhere  
✅ **Cross-Platform**: Same code works on macOS, Windows, Linux  
✅ **EMR Compatible**: No binary compatibility issues  
✅ **Fully Featured**: Supports all PostgreSQL features we need  
✅ **Maintained**: Active development and security updates  

## Implementation

### Database Connection Code

Our database connectivity code in `src/jobs/dbaccess/postgres_connectivity.py` is designed to use pg8000:

```python
try:
    import pg8000
    from pg8000.exceptions import DatabaseError
except ImportError:
    pg8000 = None
    print("pg8000 not available - database features may be limited")

def get_connection_params(db_host, db_port, db_name, db_user, db_password):
    """Get connection parameters for pg8000."""
    return {
        "host": db_host,
        "port": int(db_port),
        "database": db_name,  # pg8000 uses 'database' not 'dbname'
        "user": db_user,
        "password": db_password
    }

def create_connection(**conn_params):
    """Create database connection using pg8000."""
    if not pg8000:
        raise ImportError("pg8000 required for database connections")
    return pg8000.connect(**conn_params)
```

### Performance Considerations

While `pg8000` is pure Python (vs. C-compiled `psycopg2`), the performance difference is minimal for our use cases:

- **ETL Operations**: Network I/O is the bottleneck, not driver overhead
- **Bulk Inserts**: JDBC bulk writes are handled by Spark, not the Python driver
- **Query Execution**: PostgreSQL does the heavy lifting, not the Python driver

## Local Development

For local development, you can still use `psycopg2-binary` if needed by installing it separately:

```bash
# Optional for local development only
pip install psycopg2-binary>=2.9.0
```

However, we recommend using `pg8000` everywhere for consistency:

```bash
# Recommended: Same driver everywhere
pip install pg8000>=1.30.0
```

## Migration Notes

If migrating from `psycopg2-binary`, note these differences:

### Connection Parameters
```python
# psycopg2 style
conn = psycopg2.connect(
    host=host,
    port=port, 
    dbname=database,  # psycopg2 uses 'dbname'
    user=user,
    password=password
)

# pg8000 style  
conn = pg8000.connect(
    host=host,
    port=port,
    database=database,  # pg8000 uses 'database'
    user=user,
    password=password
)
```

### Exception Handling
```python
# psycopg2 exceptions
from psycopg2 import DatabaseError, InterfaceError

# pg8000 exceptions
from pg8000.exceptions import DatabaseError, InterfaceError
```

## Troubleshooting

### Common Issues

1. **Import Error**: 
   ```
   ModuleNotFoundError: No module named 'pg8000'
   ```
   **Solution**: Install pg8000: `pip install pg8000>=1.30.0`

2. **Connection Parameter Error**:
   ```
   TypeError: connect() got an unexpected keyword argument 'dbname'
   ```
   **Solution**: Use `database` instead of `dbname` for pg8000

3. **SSL Connection Issues**:
   ```python
   # Enable SSL for production
   conn = pg8000.connect(
       host=host,
       port=port,
       database=database,
       user=user,
       password=password,
       ssl_context=True  # pg8000 SSL syntax
   )
   ```

## Best Practices

1. **Always use pg8000** for consistency across environments
2. **Test locally** with the same driver used in production
3. **Use connection pooling** for high-throughput applications
4. **Enable SSL** for production database connections
5. **Handle exceptions** gracefully with proper error messages

## References

- [pg8000 Documentation](https://github.com/tlocke/pg8000)
- [EMR Serverless Python Libraries Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python-libraries.html)
- [PostgreSQL Python Drivers Comparison](https://wiki.postgresql.org/wiki/Python)

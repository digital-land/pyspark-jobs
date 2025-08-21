from setuptools import setup, find_packages

setup(
    name='pyspark_jobs',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={
        # 'jobs.config': ['datasets.json'],
        'jobs.config': ['*.json'],  # Include all JSON files in src/config
    },
    install_requires=[
        # Full dependencies for local development and wheel building
        'pyspark>=3.3.0,<4.0.0',
        'boto3>=1.26.0',
        'botocore>=1.29.0',
        'pg8000>=1.30.0',  # Pure Python PostgreSQL driver (EMR Serverless compatible)
        # NOTE: psycopg2-binary causes EMR compatibility issues - see docs/DATABASE_CONNECTIVITY.md
        'PyYAML>=6.0',
        'typing-extensions>=4.0.0',
        'setuptools>=65.0.0',
    ],
    author='M Santana',
    description='PySpark jobs for EMR Serverless with Airflow integration',
    python_requires='>=3.8',
)

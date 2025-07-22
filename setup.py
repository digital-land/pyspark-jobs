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
    install_requires=[],
    author='M Santana',
    description='PySpark jobs for EMR Serverless with Airflow integration',
    python_requires='>=3.8',
)

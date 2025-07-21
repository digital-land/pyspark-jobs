from setuptools import setup, find_packages

setup(
    name='pyspark_jobs',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        # 'pandas>=1.3.0',
        # 'numpy>=1.21.0',
        # Add other dependencies here, but exclude pyspark
    ],
    include_package_data=True,
    author='M Santana',
    description='PySpark jobs for EMR Serverless with Airflow integration',
    python_requires='>=3.8',
)

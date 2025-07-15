# Airflow Project for AWS MWAA and EMR Serverless

This repository contains Apache Airflow DAGs and plugins designed to run on **Amazon Managed Workflows for Apache Airflow (MWAA)**.

airflow-project/
│
├── dags/                  # All DAG files go here
│   ├── my_dag_1.py
│   └── my_dag_2.py
│
├── plugins/               # Custom plugins (operators, hooks, etc.)
│   └── my_plugin.py
│
├── requirements.txt       # Python dependencies for MWAA environment
├── airflow.cfg            # Local Airflow config for testing
├── .env                   # Environment variables for local dev
├── README.md              # Project documentation (this file)
└── scripts/               # Helper scripts for deployment, testing, etc.
    └── deploy.sh
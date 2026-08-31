FROM apache/airflow:2.10.0-python3.11

USER airflow

# 1. Airflow Orchestration & Ingestion dependencies (Fast install, no conflicts)
RUN pip install --no-cache-dir \
    "astronomer-cosmos==1.6.0" \
    "pyarrow>=15.0.0" \
    "minio>=7.2.0" \
    "psycopg2-binary>=2.9.9" \
    "requests>=2.31.0" \
    "duckdb>=1.0.0"

# 2. Isolated Virtual Environment for dbt
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.8.9" \
    "dbt-duckdb==1.8.4" \
    "duckdb>=1.0.0"
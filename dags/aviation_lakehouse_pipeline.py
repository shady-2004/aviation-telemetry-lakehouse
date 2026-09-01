import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.constants import TestBehavior

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ingestion.ingest_opensky import ingest_opensky as run_ingest_opensky
from src.ingestion.ingest_ourairports import ingest_outairports as run_ingest_ourairports
from src.ingestion.ingest_postgres import ingest_postgres as run_ingest_postgres

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt"

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
)

project_config = ProjectConfig(DBT_PROJECT_DIR)

profile_config = ProfileConfig(
    profile_name="aviation_lakehouse",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

default_args = {
    "owner": "aviation_data_eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="aviation_lakehouse_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # Serializes tasks to prevent DuckDB write locks
)
def aviation_lakehouse_dag():

    # 1. Ingestion Tasks (Bronze Landing in MinIO)
    @task
    def ingest_opensky():
        run_ingest_opensky()

    @task
    def ingest_ourairports():
        run_ingest_ourairports()

    @task
    def ingest_postgres():
        run_ingest_postgres()

    t_opensky = ingest_opensky()
    t_airports = ingest_ourairports()
    t_postgres = ingest_postgres()

    # 2. Unified Transformation & Testing Group (Silver Views -> Gold Marts -> Tests)
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models"],
            test_behavior=TestBehavior.AFTER_ALL,  # All models build first, then tests run
        ),
    )

    # Ingestion lands Bronze data -> dbt runs end-to-end pipeline
    [t_opensky, t_airports, t_postgres] >> dbt_transformations

aviation_lakehouse_dag()
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow.decorators import dag, task
# 1. Added ExecutionConfig import
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig

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
)
def aviation_lakehouse_dag():

    # 1. Ingestion Tasks (Bronze Landing)
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

    # 2. Staging Layer (Bronze -> Cleaned Silver Views)
    dbt_staging = DbtTaskGroup(
        group_id="dbt_staging_layer",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,  # <-- Added
        render_config=RenderConfig(
            select=["path:models/silver/staging"],
        ),
    )

    # 3. Intermediate Layer (Joined & Standardized Entities)
    dbt_intermediate = DbtTaskGroup(
        group_id="dbt_intermediate_layer",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,  # <-- Added
        render_config=RenderConfig(
            select=["path:models/silver/intermediate"],
        ),
    )

    # 4. Gold Marts Layer 
    dbt_marts = DbtTaskGroup(
        group_id="dbt_marts_layer",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,  # <-- Added
        render_config=RenderConfig(
            select=["path:models/gold/marts"],
        ),
    )

    # Layered Orchestration Pipeline
    [t_opensky, t_airports, t_postgres] >> dbt_staging >> dbt_intermediate >> dbt_marts

aviation_lakehouse_dag()
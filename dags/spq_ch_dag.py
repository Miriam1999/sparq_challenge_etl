from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

_DAG_DIR = Path(__file__).resolve().parent
_SQL_DIR = _DAG_DIR / "sql"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "SparqChallenge_ETL_snowflake",
    default_args=default_args,
    description="ETL",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 3, 22),
    catchup=False,
    tags=["snowflake"],
    template_searchpath=[str(_SQL_DIR)],
) as dag:

    run_bronze_job = SnowflakeOperator(
        task_id="run_bronze_job",
        snowflake_conn_id="snowflake_default",
        sql="bronze_job.sql",
    )

    run_dimensions_refresh = SnowflakeOperator(
        task_id="run_dimensions_refresh",
        snowflake_conn_id="snowflake_default",
        sql="dimensions_refresh.sql",
    )

    run_silver_job = SnowflakeOperator(
        task_id="run_silver_job",
        snowflake_conn_id="snowflake_default",
        sql="silver_job.sql",
    )

    run_gold_job = SnowflakeOperator(
        task_id="run_gold_job",
        snowflake_conn_id="snowflake_default",
        sql="gold_job.sql",
    )

    [run_bronze_job, run_dimensions_refresh] >> run_silver_job >> run_gold_job
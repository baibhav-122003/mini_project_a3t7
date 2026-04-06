from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    "owner": "Gautam_Shrivas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="databricks_15_min_pipeline",
    default_args=default_args,
    description="Bronze 15 Minutes → Silver 15 Minutes pipeline",
    schedule_interval=None,   # manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "lakehouse"]
) as dag:

    # Bronze 15 Minutes Job
    bronze_15_minutes = DatabricksRunNowOperator(
        task_id="run_bronze_15_minutes",
        databricks_conn_id="databricks_default",
        job_id=197414564629234   # Replace with Bronze_15_Minutes Job ID
    )

    # Silver 15 Minutes Job
    silver_15_minutes = DatabricksRunNowOperator(
        task_id="run_silver_15_minutes",
        databricks_conn_id="databricks_default",
        job_id=757510792299674  # Replace with Silver_15_Minutes Job ID
    )

    # Dependency
    bronze_15_minutes >> silver_15_minutes
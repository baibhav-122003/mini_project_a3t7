from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "Gautam_Shrivas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# DAG Definition
with DAG(
    dag_id="databricks_realtime_pipeline",
    default_args=default_args,
    description="Bronze → Silver Real-Time pipeline",
    schedule_interval=None,   # manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "realtime", "lakehouse"]
) as dag:

    # Bronze Real-Time Job
    bronze_realtime = DatabricksRunNowOperator(
        task_id="run_bronze_realtime",
        databricks_conn_id="databricks_default",
        job_id=879764539989551
    )

    # Silver Real-Time Job
    silver_realtime = DatabricksRunNowOperator(
        task_id="run_silver_realtime",
        databricks_conn_id="databricks_default",
        job_id=154133464005033
    )

    # Task Dependency
    bronze_realtime >> silver_realtime


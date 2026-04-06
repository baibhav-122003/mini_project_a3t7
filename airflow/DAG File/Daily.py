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
    dag_id="databricks_daily_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold → KPI pipelines",
    schedule_interval=None,   # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "lakehouse"]
) as dag:

    bronze_daily = DatabricksRunNowOperator(
        task_id="run_bronze_daily",
        databricks_conn_id="databricks_default",
        job_id=748899058244993  # replace with Bronze job id
    )

    silver_daily = DatabricksRunNowOperator(
        task_id="run_silver_daily",
        databricks_conn_id="databricks_default",
        job_id=493504441222237  # replace with Silver job id
    )

    gold_daily = DatabricksRunNowOperator(
        task_id="run_gold_daily",
        databricks_conn_id="databricks_default",
        job_id=1021777294400253  # replace with Gold job id
    )

    demand_kpi = DatabricksRunNowOperator(
        task_id="run_demand_intelligence_kpi",
        databricks_conn_id="databricks_default",
        job_id=761191343891618  # replace with Demand KPI job id
    )

    inventory_kpi = DatabricksRunNowOperator(
        task_id="run_inventory_kpi",
        databricks_conn_id="databricks_default",
        job_id=346351527715941 # replace with Inventory KPI job id
    )

    # Pipeline Flow
    bronze_daily >> silver_daily >> gold_daily
    gold_daily >> [demand_kpi, inventory_kpi]
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
    dag_id="databricks_pipeline_Hourly",
    default_args=default_args,
    description="Run Bronze → Silver → Gold → KPI notebooks in Databricks",
    schedule_interval=None,   # No scheduling (manual trigger)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "lakehouse"]
) as dag:

    # Bronze Hourly Job
    bronze_hourly = DatabricksRunNowOperator(
        task_id="run_bronze_hourly",
        databricks_conn_id="databricks_default",
        job_id=250589541057169 # Replace with Bronze_Hourly job ID
    )

    # Silver Hourly Job
    silver_hourly = DatabricksRunNowOperator(
        task_id="run_silver_hourly",
        databricks_conn_id="databricks_default",
        job_id=631844064541127 # Replace with Silver_Hourly job ID
    )

    # Gold Daily Job
    gold_daily = DatabricksRunNowOperator(
        task_id="run_gold_hourly",
        databricks_conn_id="databricks_default",
        job_id=678482705227427  # Replace with Gold_Daily job ID
    )

    # Inventory KPI Job
    inventory_kpi = DatabricksRunNowOperator(
        task_id="run_inventory_kpi",
        databricks_conn_id="databricks_default",
        job_id=346351527715941 # Replace with Inventory_KPI job ID
    )

    # Supplier KPI Job
    supplier_kpi = DatabricksRunNowOperator(
        task_id="run_supplier_kpi",
        databricks_conn_id="databricks_default",
        job_id=223088397689750 # Replace with Supplier_KPI job ID
    )

    # Task Dependencies
    bronze_hourly >> silver_hourly >> gold_daily
    gold_daily >> [inventory_kpi, supplier_kpi]
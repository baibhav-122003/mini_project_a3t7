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
    dag_id="databricks_weekly_pipeline",
    default_args=default_args,
    description="Run Bronze, Silver, Gold and KPI notebooks in Databricks",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "lakehouse"]
) as dag:

    # Bronze Notebook Job
    bronze_task = DatabricksRunNowOperator(
        task_id="run_bronze_notebook",
        databricks_conn_id="databricks_default",
        job_id=315230325088952
    )

    # Silver Notebook Job
    silver_task = DatabricksRunNowOperator(
        task_id="run_silver_notebook",
        databricks_conn_id="databricks_default",
        job_id=727001770443459
    )

    # Gold Notebook Job
    gold_task = DatabricksRunNowOperator(
        task_id="run_gold_notebook",
        databricks_conn_id="databricks_default",
        job_id=589244917280393
    )

    # KPI Jobs (Parallel after Gold)

    demand_kpi_task = DatabricksRunNowOperator(
        task_id="run_demand_intelligence_kpi",
        databricks_conn_id="databricks_default",
        job_id=761191343891618  # Replace with Demand KPI job id
    )

    inventory_kpi_task = DatabricksRunNowOperator(
        task_id="run_inventory_kpi",
        databricks_conn_id="databricks_default",
        job_id=346351527715941  # Replace with Inventory KPI job id
    )

    supplier_kpi_task = DatabricksRunNowOperator(
        task_id="run_supplier_kpi",
        databricks_conn_id="databricks_default",
        job_id=223088397689750   # Replace with Supplier KPI job id
    )

    # Task Dependency
    bronze_task >> silver_task >> gold_task
    gold_task >> [demand_kpi_task, inventory_kpi_task, supplier_kpi_task]
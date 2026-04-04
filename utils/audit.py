# utils/audit.py
import uuid
from datetime import datetime

def start_audit(spark, pipeline_name, target_table, 
                source_table=None, extra=None):
    """
    Call at the start of every pipeline.
    Returns run_id — pass this to end_audit when done.
    """
    import json
    with open("/Workspace/Shared/mini_project_a3t7/config/config.json") as f:
        cfg = json.load(f)
    
    AUDIT_TABLE = cfg["tables"]["pipeline_audit"]
    run_id = str(uuid.uuid4())

    row = [{
        "run_id":           run_id,
        "pipeline_name":    pipeline_name,
        "source_table":     source_table,
        "target_table":     target_table,
        "start_time":       datetime.utcnow(),
        "end_time":         None,
        "rows_read":        None,
        "rows_written":     None,
        "rows_rejected":    None,
        "status":           "RUNNING",
        "error_message":    None,
        "spark_app_id":     spark.conf.get("spark.app.id", None),
        "extra_metadata":   extra or {}
    }]

    audit_table_schema = spark.table("azure3_team7_project.platform.pipeline_audit").schema

    spark.createDataFrame(row, schema=audit_table_schema) \
         .write.format("delta") \
         .mode("append") \
         .saveAsTable(AUDIT_TABLE)

    print(f"[AUDIT] Started: {pipeline_name} | run_id: {run_id}")
    return run_id


def end_audit(spark, run_id, status, 
              rows_read=None, rows_written=None, 
              rows_rejected=None, error=None, extra=None):
    """
    Call at the end of every pipeline — both success and failure.
    Status should be: SUCCESS or FAILED
    """
    import json
    with open("/Workspace/Shared/mini_project_a3t7/config/config.json") as f:
        cfg = json.load(f)

    AUDIT_TABLE = cfg["tables"]["pipeline_audit"]

    from delta.tables import DeltaTable
    dt = DeltaTable.forName(spark, AUDIT_TABLE)

    if extra:
        map_entries = ", ".join(
            [f"'{k}', '{v}'" for k, v in extra.items()]
        )
        extra_expr = f"map({map_entries})"
    else:
        extra_expr = "null"

    dt.update(
        condition=f"run_id = '{run_id}'",
        set={
            "end_time":       "current_timestamp()",
            "status":         f"'{status}'",
            "rows_read":      str(rows_read)     if rows_read     is not None else "null",
            "rows_written":   str(rows_written)  if rows_written  is not None else "null",
            "rows_rejected":  str(rows_rejected) if rows_rejected is not None else "null",
            "error_message":  f"'{error}'"       if error         else "null",
            "extra_metadata": extra_expr
        }
    )

    print(f"[AUDIT] Ended: run_id {run_id} | status: {status} | "
          f"rows_read: {rows_read} | rows_written: {rows_written}")


def get_last_successful_run_time(spark, pipeline_name):
    """
    Returns the end_time of the last successful run for a given pipeline.
    Used as watermark for incremental spark.read — only process new rows
    added to source table since last successful run.
 
    Returns:
        datetime : end_time of last successful run
        None     : if no successful run exists (first run — process everything)
 
    Usage:
        last_run_time = get_last_successful_run_time(spark, PIPELINE)
 
        if last_run_time:
            df = spark.read.table(SOURCE_TABLE)
                     .filter(col("ingested_at") > lit(last_run_time))
        else:
            df = spark.read.table(SOURCE_TABLE)  # first run — all rows
    """
    import json
    from pyspark.sql.functions import col
 
    with open("/Workspace/Shared/mini_project_a3t7/config/config.json") as f:
        cfg = json.load(f)
 
    AUDIT_TABLE = cfg["tables"]["pipeline_audit"]
 
    result = (
        spark.read.table(AUDIT_TABLE)
        .filter(
            (col("pipeline_name") == pipeline_name) &
            (col("status") == "SUCCESS")
        )
        .orderBy(col("end_time").desc())
        .limit(1)
        .select("end_time")
        .collect()
    )
 
    # No successful run found — first run, process everything
    if not result or result[0]["end_time"] is None:
        print(f"[AUDIT] No previous successful run found for {pipeline_name} "
              f"— processing all rows")
        return None
 
    last_run_time = result[0]["end_time"]
    print(f"[AUDIT] Last successful run for {pipeline_name}: {last_run_time} "
          f"— processing only new rows")
    return last_run_time

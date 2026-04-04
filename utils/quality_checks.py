# utils/quality_checks.py

def assert_not_empty(df, label):
    """Fails if dataframe has zero rows."""
    count = df.count()
    assert count > 0, f"[QC FAIL] {label}: 0 rows — pipeline halted"
    print(f"[QC PASS] {label}: {count} rows")
    return count


def assert_no_nulls(df, columns, label):
    """Fails if any of the specified columns contain nulls."""
    from pyspark.sql.functions import col
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        assert null_count == 0, \
            f"[QC FAIL] {label}: {null_count} nulls found in '{col_name}'"
        print(f"[QC PASS] {label}: no nulls in '{col_name}'")


def assert_no_duplicates(df, key_columns, label):
    """Fails if duplicate rows exist on the given key columns."""
    total    = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    dupes    = total - distinct
    assert dupes == 0, \
        f"[QC FAIL] {label}: {dupes} duplicate rows on {key_columns}"
    print(f"[QC PASS] {label}: no duplicates on {key_columns}")


def assert_freshness(df, ts_column, max_lag_hours, label):
    """Fails if the most recent record is older than max_lag_hours."""
    from pyspark.sql.functions import max as spark_max
    from datetime import datetime, timedelta
    max_ts    = df.agg(spark_max(ts_column)).collect()[0][0]
    threshold = datetime.utcnow() - timedelta(hours=max_lag_hours)
    assert max_ts >= threshold, \
        f"[QC FAIL] {label}: latest {ts_column} is {max_ts}, " \
        f"older than {max_lag_hours}h threshold"
    print(f"[QC PASS] {label}: data is fresh — latest {ts_column}: {max_ts}")
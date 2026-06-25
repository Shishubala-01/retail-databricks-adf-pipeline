"""
Shared data quality utilities.

Used by the bronze -> silver notebooks so the same checks are applied consistently
and logged the same way every run. In the real project this is also what I'd point
to in the interview as "how were quality checks implemented" - a small reusable
library rather than copy-pasted checks in every notebook.

Each check function returns a tuple: (clean_df, rejected_df, metric_dict)
so callers can quarantine bad rows AND log a metric row to the dq_log table.
"""

from pyspark.sql import DataFrame, functions as F
from datetime import datetime


def check_not_null(df: DataFrame, columns: list) -> tuple:
    """Flag rows where any of the given columns is null. Returns (clean, rejected, metrics)."""
    null_condition = None
    for c in columns:
        cond = F.col(c).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    rejected = df.filter(null_condition).withColumn("rejection_reason", F.lit(f"null_check_failed:{columns}"))
    clean = df.filter(~null_condition)

    metrics = {
        "check_name": "not_null",
        "columns": str(columns),
        "rows_checked": df.count(),
        "rows_rejected": rejected.count(),
    }
    return clean, rejected, metrics


def check_no_duplicates(df: DataFrame, key_columns: list) -> tuple:
    """Drop duplicate rows on the given key, keeping the first occurrence."""
    window_count = df.groupBy(*key_columns).count().filter("count > 1")
    dup_keys = window_count.select(*key_columns)

    rejected = (
        df.join(dup_keys, on=key_columns, how="inner")
        .withColumn("rejection_reason", F.lit(f"duplicate_key:{key_columns}"))
    )
    clean = df.dropDuplicates(key_columns)

    metrics = {
        "check_name": "no_duplicates",
        "columns": str(key_columns),
        "rows_checked": df.count(),
        "rows_rejected": rejected.count(),
    }
    return clean, rejected, metrics


def check_referential_integrity(df: DataFrame, fk_column: str, ref_df: DataFrame, ref_key_column: str) -> tuple:
    """Flag rows whose foreign key doesn't exist in the reference (dimension) table."""
    valid_keys = ref_df.select(F.col(ref_key_column).alias("_valid_key")).distinct()

    joined = df.join(valid_keys, df[fk_column] == valid_keys["_valid_key"], how="left")
    rejected = (
        joined.filter(F.col("_valid_key").isNull())
        .drop("_valid_key")
        .withColumn("rejection_reason", F.lit(f"orphan_fk:{fk_column}"))
    )
    clean = joined.filter(F.col("_valid_key").isNotNull()).drop("_valid_key")

    metrics = {
        "check_name": "referential_integrity",
        "columns": fk_column,
        "rows_checked": df.count(),
        "rows_rejected": rejected.count(),
    }
    return clean, rejected, metrics


def check_value_range(df: DataFrame, column: str, min_value=0, max_value=None) -> tuple:
    """Flag rows where a numeric column falls outside an expected range (e.g. negative quantity)."""
    condition = F.col(column) < min_value
    if max_value is not None:
        condition = condition | (F.col(column) > max_value)

    rejected = df.filter(condition).withColumn("rejection_reason", F.lit(f"out_of_range:{column}"))
    clean = df.filter(~condition)

    metrics = {
        "check_name": "value_range",
        "columns": column,
        "rows_checked": df.count(),
        "rows_rejected": rejected.count(),
    }
    return clean, rejected, metrics


def write_dq_log(spark, run_metrics: list, layer: str, table_name: str, dq_log_path: str):
    """Append a row per check into the dq_log Delta table for monitoring/alerting."""
    rows = [
        {
            "run_ts": datetime.utcnow().isoformat(),
            "layer": layer,
            "table_name": table_name,
            "check_name": m["check_name"],
            "columns_checked": m["columns"],
            "rows_checked": m["rows_checked"],
            "rows_rejected": m["rows_rejected"],
        }
        for m in run_metrics
    ]
    log_df = spark.createDataFrame(rows)
    log_df.write.format("delta").mode("append").save(dq_log_path)

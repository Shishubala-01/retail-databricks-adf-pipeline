"""
Reusable PySpark data quality check functions.

Kept here, separate from the Databricks notebooks, specifically so they can be
unit tested locally with pytest and a local SparkSession - no cluster needed.
The notebook versions currently inline this same logic directly (simplest path
for running interactively in Databricks); this module is the testable source
of truth the logic should match. If wiring up Databricks Repos, the notebook
could import directly from here instead of duplicating it.

Every function follows the same shape on purpose: takes a DataFrame in,
returns (clean_rows, rejected_rows) out. That consistency is what makes them
composable - each check's clean output feeds straight into the next check.
"""

from pyspark.sql import DataFrame, functions as F


def check_nulls(df: DataFrame, columns: list) -> tuple:
    """
    USE: catches rows where a required business key is missing,
    e.g. an order with no customer_id - can't process that order at all.
    HOW: counts how many of the given columns are null per row;
    any row with a count above 0 gets rejected.
    """
    null_count = sum(F.col(c).isNull().cast("int") for c in columns)
    rejected = df.filter(null_count > 0)
    clean = df.filter(null_count == 0)
    return clean, rejected


def check_duplicates(df: DataFrame, key_column: str) -> tuple:
    """
    USE: catches the same record landing twice, e.g. from a retried file copy.
    HOW: finds key values that appear more than once. The rejected side keeps
    every occurrence of a duplicated key (for audit visibility); the clean
    side keeps exactly one copy of each key via dropDuplicates.
    """
    duplicate_keys = df.groupBy(key_column).count().filter("count > 1").select(key_column)
    rejected = df.join(duplicate_keys, key_column, "inner")
    clean = df.dropDuplicates([key_column])
    return clean, rejected


def check_negative(df: DataFrame, column: str) -> tuple:
    """
    USE: catches values that should never be negative, e.g. quantity ordered.
    A negative quantity usually means a data entry or extract error upstream.
    """
    rejected = df.filter(F.col(column) < 0)
    clean = df.filter(F.col(column) >= 0)
    return clean, rejected


def check_orphan_keys(df: DataFrame, fk_column: str, reference_df: DataFrame, reference_column: str) -> tuple:
    """
    USE: catches a foreign key that points to something that doesn't exist,
    e.g. an order for a product_id that isn't in the product catalog
    (the product may have been retired, or the catalog extract is stale).
    HOW: "left_anti" join keeps only rows from df with NO match in the
    reference table (rejected); "left_semi" keeps only rows that DO match
    (clean) - the opposite of each other.
    """
    valid_keys = reference_df.select(reference_column).distinct()
    rejected = df.join(valid_keys, df[fk_column] == valid_keys[reference_column], "left_anti")
    clean = df.join(valid_keys, df[fk_column] == valid_keys[reference_column], "left_semi")
    return clean, rejected


def check_future_dates(df: DataFrame, date_column: str) -> tuple:
    """
    USE: catches an order dated after today - usually a typo'd year on the
    source system, or a clock/timezone error during extraction. A "sale" that
    hasn't happened yet shouldn't be in a sales report.
    HOW: compares the column to current_date(); anything strictly greater
    than today is rejected.
    """
    rejected = df.filter(F.col(date_column) > F.current_date())
    clean = df.filter(F.col(date_column) <= F.current_date())
    return clean, rejected

"""
Tests for the existing, already-built quality checks.

These are written *after* the logic (not strict red-green-refactor TDD, since
the functions already existed) - but they serve the same real purpose: a
safety net. If anyone changes check_nulls, check_duplicates, check_negative,
or check_orphan_keys later, these tests catch a regression immediately,
without needing a Databricks cluster to find out.
"""

from quality_checks import check_nulls, check_duplicates, check_negative, check_orphan_keys


def test_check_nulls_keeps_rows_with_no_nulls(spark):
    df = spark.createDataFrame(
        [("O1", "C1", "P1"), ("O2", "C2", "P2")],
        ["order_id", "customer_id", "product_id"],
    )
    clean, rejected = check_nulls(df, ["order_id", "customer_id", "product_id"])
    assert clean.count() == 2
    assert rejected.count() == 0


def test_check_nulls_rejects_row_with_missing_customer_id(spark):
    df = spark.createDataFrame(
        [("O1", "C1", "P1"), ("O2", None, "P2")],
        ["order_id", "customer_id", "product_id"],
    )
    clean, rejected = check_nulls(df, ["order_id", "customer_id", "product_id"])
    assert clean.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["order_id"] == "O2"


def test_check_duplicates_keeps_one_copy_of_each_key(spark):
    df = spark.createDataFrame(
        [("O1", 10), ("O1", 10), ("O2", 20)],
        ["order_id", "amount"],
    )
    clean, rejected = check_duplicates(df, "order_id")
    assert clean.count() == 2  # one O1, one O2
    assert rejected.count() == 2  # BOTH copies of O1 logged for audit visibility


def test_check_duplicates_with_no_duplicates_rejects_nothing(spark):
    df = spark.createDataFrame([("O1", 10), ("O2", 20)], ["order_id", "amount"])
    clean, rejected = check_duplicates(df, "order_id")
    assert clean.count() == 2
    assert rejected.count() == 0


def test_check_negative_rejects_negative_quantity(spark):
    df = spark.createDataFrame([("O1", 5), ("O2", -2)], ["order_id", "quantity"])
    clean, rejected = check_negative(df, "quantity")
    assert clean.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["order_id"] == "O2"


def test_check_negative_allows_zero_quantity(spark):
    df = spark.createDataFrame([("O1", 0)], ["order_id", "quantity"])
    clean, rejected = check_negative(df, "quantity")
    assert clean.count() == 1
    assert rejected.count() == 0


def test_check_orphan_keys_rejects_unmatched_foreign_key(spark):
    orders = spark.createDataFrame([("O1", "P1"), ("O2", "P99")], ["order_id", "product_id"])
    products = spark.createDataFrame([("P1",)], ["product_id"])
    clean, rejected = check_orphan_keys(orders, "product_id", products, "product_id")
    assert clean.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["order_id"] == "O2"


def test_check_orphan_keys_accepts_all_when_every_key_matches(spark):
    orders = spark.createDataFrame([("O1", "P1"), ("O2", "P2")], ["order_id", "product_id"])
    products = spark.createDataFrame([("P1",), ("P2",)], ["product_id"])
    clean, rejected = check_orphan_keys(orders, "product_id", products, "product_id")
    assert clean.count() == 2
    assert rejected.count() == 0

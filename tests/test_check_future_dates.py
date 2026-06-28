"""
A live TDD example: check_future_dates doesn't exist yet when this file is
written. These tests are written first, against the function we WANT to
exist, then run (and watched fail), before any implementation is written.
"""

from quality_checks import check_future_dates
from datetime import date, timedelta
from pyspark.sql import functions as F


def test_rejects_order_dated_in_the_future(spark):
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    df = spark.createDataFrame([("O1", tomorrow)], ["order_id", "order_date_parsed"]) \
        .withColumn("order_date_parsed", F.col("order_date_parsed").cast("date"))
    clean, rejected = check_future_dates(df, "order_date_parsed")
    assert clean.count() == 0
    assert rejected.count() == 1


def test_keeps_order_dated_today_or_earlier(spark):
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    df = spark.createDataFrame([("O1", today), ("O2", yesterday)], ["order_id", "order_date_parsed"]) \
        .withColumn("order_date_parsed", F.col("order_date_parsed").cast("date"))
    clean, rejected = check_future_dates(df, "order_date_parsed")
    assert clean.count() == 2
    assert rejected.count() == 0


def test_handles_a_mix_of_past_and_future_dates(spark):
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    next_week = (date.today() + timedelta(days=7)).isoformat()
    df = spark.createDataFrame([("O1", yesterday), ("O2", next_week)], ["order_id", "order_date_parsed"]) \
        .withColumn("order_date_parsed", F.col("order_date_parsed").cast("date"))
    clean, rejected = check_future_dates(df, "order_date_parsed")
    assert clean.count() == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["order_id"] == "O2"

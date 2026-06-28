# Databricks notebook: 01_bronze_ingest_validate
#
# Purpose: land raw files exactly as received from ADF into Bronze Delta tables.
# Bronze = schema-enforced, append-only, no business cleaning yet. This preserves
# a full history of what actually arrived, which matters for auditability and for
# re-running Silver if a transformation bug is found later.
#
# Triggered by ADF as a "Notebook Activity" after the Copy Activities land files in
# the `raw` container. ADF passes the ingestion date as a parameter.
#
# Note: uses the _metadata.file_path column rather than the legacy input_file_name()
# function - input_file_name() is blocked under Unity Catalog (UC_COMMAND_NOT_SUPPORTED),
# since it can expose raw file paths outside the governed access model.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

# --- Parameters passed in from ADF ---
dbutils.widgets.text("ingestion_date", "2024-01-14")
dbutils.widgets.text("raw_base_path", "abfss://raw@retaildemoadls01.dfs.core.windows.net")
dbutils.widgets.text("bronze_base_path", "abfss://processed@retaildemoadls01.dfs.core.windows.net/bronze")

ingestion_date = dbutils.widgets.get("ingestion_date")
raw_base_path = dbutils.widgets.get("raw_base_path")
bronze_base_path = dbutils.widgets.get("bronze_base_path")

# --- Explicit schemas (fail fast on unexpected source schema changes,
#     rather than silently inferring the wrong type - this is the first
#     line of defence against "schema drift") ---
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),  # kept as string here; parsed/validated in Silver
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("region", StringType(), True),
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("supplier_id", StringType(), True),
])

clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device_type", StringType(), True),
])


def land_to_bronze(source_format, source_path, schema, table_name):
    reader = spark.read.schema(schema)
    df = reader.json(source_path) if source_format == "json" else reader.option("header", True).csv(source_path)

    df = (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_date", F.lit(ingestion_date))
    )

    # Row-count reconciliation check: compare landed row count against the source
    # file's line count. ADF's Copy Activity also reports rows-copied in its own
    # output JSON, which we cross-check in the orchestration pipeline (see adf/pipelines).
    landed_count = df.count()
    print(f"[{table_name}] landed {landed_count} rows from {source_path}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{bronze_base_path}/{table_name}")
    )
    return landed_count


counts = {
    "orders": land_to_bronze("csv", f"{raw_base_path}/orders_raw.csv", orders_schema, "orders"),
    "customers": land_to_bronze("csv", f"{raw_base_path}/customers.csv", customers_schema, "customers"),
    "products": land_to_bronze("csv", f"{raw_base_path}/products.csv", products_schema, "products"),
    "clickstream": land_to_bronze("json", f"{raw_base_path}/clickstream_events.jsonl", clickstream_schema, "clickstream"),
}

print(counts)
dbutils.notebook.exit(str(counts))

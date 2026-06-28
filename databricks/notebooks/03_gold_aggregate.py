# Databricks notebook: 03_gold_aggregate
#
# Purpose: turn clean Silver data into the business-ready "data product" -
# the curated tables that Power BI / the merchandising team actually consume.
# This is the layer you point to when asked "what was the output dataset used for".
#
# Layout: Gold tables use Liquid Clustering (CLUSTER BY), not ZORDER/partitioning.
# Clustering keys were picked from what Power BI actually filters/groups by, not
# guessed - e.g. the daily dashboard always filters by order_date and region first.
# This requires registering tables via saveAsTable rather than a path-only
# .save() - clusterBy() needs the table API to persist the clustering metadata.

from pyspark.sql import functions as F

dbutils.widgets.text("silver_base_path", "abfss://processed@retaildemoadls01.dfs.core.windows.net/silver")

silver_base_path = dbutils.widgets.get("silver_base_path")

spark.sql("CREATE DATABASE IF NOT EXISTS retail_gold")

orders = spark.read.format("delta").load(f"{silver_base_path}/orders")
products = spark.read.format("delta").load(f"{silver_base_path}/products")
customers = spark.read.format("delta").load(f"{silver_base_path}/customers")

orders_enriched = (
    orders.join(products, "product_id", "left")
    .join(customers, "customer_id", "left")
)

# --- Data product 1: daily sales summary, by region & category ---
# Used for: the daily ops/merchandising Power BI dashboard - "how did we trade yesterday"
daily_sales_summary = (
    orders_enriched
    .groupBy("order_date_parsed", "region", "category")
    .agg(
        F.sum("net_amount").alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .withColumnRenamed("order_date_parsed", "order_date")
)
daily_sales_summary.write.format("delta").mode("overwrite").clusterBy("order_date", "region").saveAsTable("retail_gold.daily_sales_summary")

# --- Data product 2: top products ranking ---
# Used for: merchandising team's weekly "what's moving" review
top_products = (
    orders_enriched
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("net_amount").alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    .orderBy(F.desc("total_revenue"))
)
top_products.write.format("delta").mode("overwrite").clusterBy("category").saveAsTable("retail_gold.top_products")

# --- Data product 3: customer value summary ---
# Used for: marketing segmentation / CRM campaign targeting
customer_value = (
    orders_enriched
    .groupBy("customer_id", "first_name", "last_name", "customer_segment")
    .agg(
        F.sum("net_amount").alias("lifetime_value"),
        F.countDistinct("order_id").alias("order_count"),
        F.max("order_date_parsed").alias("last_order_date"),
    )
)
customer_value.write.format("delta").mode("overwrite").clusterBy("customer_segment").saveAsTable("retail_gold.customer_value")

print("Gold tables written:")
print(" - daily_sales_summary:", daily_sales_summary.count(), "rows")
print(" - top_products:", top_products.count(), "rows")
print(" - customer_value:", customer_value.count(), "rows")

dbutils.notebook.exit("gold_layer_complete")

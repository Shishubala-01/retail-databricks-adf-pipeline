# Databricks notebook: 02_silver_clean_transform
#
# Purpose: take Bronze (raw-as-landed) and apply the actual data quality rules:
#   - not-null checks on business keys
#   - de-duplication on natural key
#   - referential integrity against dimension tables
#   - value-range checks (e.g. negative quantity)
#   - date parsing / standardisation
#
# Anything that fails a check is NOT silently dropped - it's written to a
# `rejected` location with a reason code, and a row is appended to `dq_log`
# so failures are visible (and can trigger an ADF alert if the reject rate
# crosses a threshold - see the master pipeline's "If Condition" activity).

import sys
sys.path.append("/Workspace/Repos/retail-databricks-adf-pipeline/tests")  # adjust to your repo path in Databricks

from pyspark.sql import functions as F
from data_quality_checks import (
    check_not_null, check_no_duplicates, check_referential_integrity,
    check_value_range, write_dq_log
)

dbutils.widgets.text("bronze_base_path", "/mnt/bronze")
dbutils.widgets.text("silver_base_path", "/mnt/silver")
dbutils.widgets.text("rejected_base_path", "/mnt/rejected")
dbutils.widgets.text("dq_log_path", "/mnt/silver/_dq_log")

bronze_base_path = dbutils.widgets.get("bronze_base_path")
silver_base_path = dbutils.widgets.get("silver_base_path")
rejected_base_path = dbutils.widgets.get("rejected_base_path")
dq_log_path = dbutils.widgets.get("dq_log_path")

# --- Load Bronze ---
orders_bronze = spark.read.format("delta").load(f"{bronze_base_path}/orders")
customers_bronze = spark.read.format("delta").load(f"{bronze_base_path}/customers")
products_bronze = spark.read.format("delta").load(f"{bronze_base_path}/products")

# --- Dimensions pass through with light cleaning (dedup only) ---
customers_clean, customers_rejected, m1 = check_no_duplicates(customers_bronze, ["customer_id"])
products_clean, products_rejected, m2 = check_no_duplicates(products_bronze, ["product_id"])

# --- Standardise order_date: source sometimes sends DD-MM-YYYY instead of YYYY-MM-DD
#     (this is a real example of "schema/format drift" worth raising in the interview) ---
orders_parsed = orders_bronze.withColumn(
    "order_date_parsed",
    F.coalesce(
        F.to_date("order_date", "yyyy-MM-dd"),
        F.to_date("order_date", "dd-MM-yyyy"),
    )
)

# --- Fact table quality checks, chained ---
all_rejected = []

orders_step1, rej1, metric1 = check_not_null(orders_parsed, ["order_id", "customer_id", "product_id"])
all_rejected.append((rej1, metric1))

orders_step2, rej2, metric2 = check_no_duplicates(orders_step1, ["order_id"])
all_rejected.append((rej2, metric2))

orders_step3, rej3, metric3 = check_value_range(orders_step2, "quantity", min_value=0)
all_rejected.append((rej3, metric3))

orders_step4, rej4, metric4 = check_referential_integrity(orders_step3, "customer_id", customers_clean, "customer_id")
all_rejected.append((rej4, metric4))

orders_clean, rej5, metric5 = check_referential_integrity(orders_step4, "product_id", products_clean, "product_id")
all_rejected.append((rej5, metric5))

orders_clean = orders_clean.withColumn(
    "net_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)

# --- Write Silver (clean, conformed) ---
orders_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_base_path}/orders")
customers_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/customers")
products_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/products")

# --- Quarantine rejects with reason codes, for the data team to review/backfill ---
for rejected_df, _ in all_rejected:
    if rejected_df.count() > 0:
        rejected_df.write.format("delta").mode("append").save(f"{rejected_base_path}/orders")

# --- Log every check's metrics so reject rates are queryable / alertable ---
write_dq_log(spark, [m1, m2, metric1, metric2, metric3, metric4, metric5], layer="silver", table_name="orders", dq_log_path=dq_log_path)

reject_rate = sum(m["rows_rejected"] for _, m in all_rejected) / max(orders_bronze.count(), 1)
print(f"Reject rate this run: {reject_rate:.2%}")

# ADF reads this exit value via "Notebook Output" and can branch (e.g. alert if > 5%)
dbutils.notebook.exit(str({"reject_rate": reject_rate, "clean_rows": orders_clean.count()}))

# Databricks notebook: 02_silver_clean_transform
#
# Purpose: take Bronze (raw-as-landed) and apply the actual data quality rules:
#   - not-null checks on business keys
#   - de-duplication on natural key
#   - value-range checks (e.g. negative quantity)
#   - referential integrity against dimension tables (orphaned foreign keys)
#   - date format standardisation
#
# This is the version actually validated end-to-end on Serverless compute against
# the real ADLS Gen2 account (see README for the live build notes). Earlier drafts
# used DBFS mounts and a separate shared-module import - both were replaced after
# hitting real Unity Catalog/serverless incompatibilities during the build:
#   - DBFS mounts don't work on serverless compute -> switched to Unity Catalog
#     External Locations (abfss:// paths), authenticated via an Access Connector
#     managed identity instead of an account key.
#   - input_file_name() is blocked under Unity Catalog -> use _metadata.file_path.
#   - to_date() raises CANNOT_PARSE_TIMESTAMP on a format mismatch under ANSI mode
#     (the current Databricks default) instead of returning null -> use try_to_date,
#     which returns NULL on a parse failure, so the coalesce() fallback actually works.
#
# Anything that fails a check is NOT silently dropped - it's written to the
# `rejected` external location with a reason code, so failures stay visible and
# reviewable rather than disappearing.

from pyspark.sql import functions as F

dbutils.widgets.text("bronze_base_path", "abfss://processed@retaildemoadls01.dfs.core.windows.net/bronze")
dbutils.widgets.text("silver_base_path", "abfss://processed@retaildemoadls01.dfs.core.windows.net/silver")
dbutils.widgets.text("rejected_base_path", "abfss://rejected@retaildemoadls01.dfs.core.windows.net")

bronze_base_path = dbutils.widgets.get("bronze_base_path")
silver_base_path = dbutils.widgets.get("silver_base_path")
rejected_base_path = dbutils.widgets.get("rejected_base_path")

orders = spark.read.format("delta").load(f"{bronze_base_path}/orders")
customers = spark.read.format("delta").load(f"{bronze_base_path}/customers")
products = spark.read.format("delta").load(f"{bronze_base_path}/products")

orders_original_count = orders.count()
print("Starting orders count:", orders_original_count)


# --- Reusable quality-check functions -------------------------------------
# Every function follows the same pattern on purpose: takes a DataFrame in,
# returns (clean_rows, rejected_rows) out. That consistency is what makes
# them composable - each check's clean output feeds straight into the next.

def check_nulls(df, columns):
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


def check_duplicates(df, key_column):
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


def check_negative(df, column):
    """
    USE: catches values that should never be negative, e.g. quantity ordered.
    A negative quantity usually means a data entry or extract error upstream.
    """
    rejected = df.filter(F.col(column) < 0)
    clean = df.filter(F.col(column) >= 0)
    return clean, rejected


def check_orphan_keys(df, fk_column, reference_df, reference_column):
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


# --- Clean the dimension tables (dedup only - no audit trail needed for these) ---
customers_clean, _ = check_duplicates(customers, "customer_id")
products_clean, _ = check_duplicates(products, "product_id")
print("Customers:", customers_clean.count(), " Products:", products_clean.count())

# --- Standardise order_date: source sometimes sends DD-MM-YYYY instead of YYYY-MM-DD.
#     try_to_date (not to_date) so a format mismatch returns NULL instead of raising
#     under ANSI mode, which is what the coalesce() fallback actually needs.
orders = orders.withColumn(
    "order_date_parsed",
    F.coalesce(
        F.expr("try_to_date(order_date, 'yyyy-MM-dd')"),
        F.expr("try_to_date(order_date, 'dd-MM-yyyy')"),
    )
)

# --- Run every check on orders, in order, each feeding its clean output into the next ---
rejected_pieces = []

orders, rej = check_nulls(orders, ["order_id", "customer_id", "product_id"])
rejected_pieces.append(rej.withColumn("reason", F.lit("missing_key")))

orders, rej = check_duplicates(orders, "order_id")
rejected_pieces.append(rej.withColumn("reason", F.lit("duplicate_order_id")))

orders, rej = check_negative(orders, "quantity")
rejected_pieces.append(rej.withColumn("reason", F.lit("negative_quantity")))

orders, rej = check_orphan_keys(orders, "customer_id", customers_clean, "customer_id")
rejected_pieces.append(rej.withColumn("reason", F.lit("orphan_customer_id")))

orders, rej = check_orphan_keys(orders, "product_id", products_clean, "product_id")
rejected_pieces.append(rej.withColumn("reason", F.lit("orphan_product_id")))

# --- Combine and quarantine every rejected row, tagged with why it was rejected ---
all_rejected = rejected_pieces[0]
for piece in rejected_pieces[1:]:
    all_rejected = all_rejected.unionByName(piece, allowMissingColumns=True)

reject_rate = all_rejected.count() / orders_original_count
print(f"Total rejected: {all_rejected.count()} out of {orders_original_count} ({reject_rate:.0%})")

all_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{rejected_base_path}/orders")

# --- Add the business calculation, write the clean Silver tables ---
orders = orders.withColumn(
    "net_amount",
    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)

orders.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_base_path}/orders")
customers_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/customers")
products_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/products")

print("Final clean orders:", orders.count())

# ADF reads this exit value via "Notebook Output" and can branch (e.g. alert if reject_rate > 5%)
dbutils.notebook.exit(str({"reject_rate": reject_rate, "clean_rows": orders.count()}))

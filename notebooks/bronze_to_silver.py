# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 ➡️ 🥈 Bronze to Silver Medallion Transformation
# MAGIC 
# MAGIC ### Overview
# MAGIC This notebook implements the **Silver Layer Data Hygiene & Refinement** pipeline on Databricks:
# MAGIC 1. **Batch Ingestion**: Reads raw CSV batches from ADLS Gen2 `bronze/` container using `PERMISSIVE` parsing mode.
# MAGIC 2. **Data Hygiene & Schema Enforcement**: Enforces explicit data types, parses timestamps to UTC, trims strings, and standardizes status codes.
# MAGIC 3. **Quarantine & Error Logging**: Isolates corrupted or invalid records using `_corrupt_record` and writes them to a quarantine Delta table.
# MAGIC 4. **Deduplication**: Removes duplicate primary keys (`order_id`) using window ranking.
# MAGIC 5. **Delta Lake Optimization**: Writes cleansed records into ADLS Gen2 `silver/` container partitioned by year/month and executes `OPTIMIZE ... ZORDER BY`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Configuration & Interactive Widgets

# COMMAND ----------

# Create Databricks interactive parameters (Widgets)
dbutils.widgets.text("storage_account", "stlakehousemedallion", "Storage Account Name")
dbutils.widgets.text("bronze_container", "bronze", "Bronze Container")
dbutils.widgets.text("silver_container", "silver", "Silver Container")
dbutils.widgets.dropdown("dataset_type", "ecommerce", ["ecommerce", "nyctaxi"], "Dataset Type")
dbutils.widgets.text("raw_file_path", "raw/ecommerce/olist_orders_dataset.csv", "Raw File Subpath")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
BRONZE_CONTAINER = dbutils.widgets.get("bronze_container")
SILVER_CONTAINER = dbutils.widgets.get("silver_container")
DATASET_TYPE = dbutils.widgets.get("dataset_type")
RAW_FILE_PATH = dbutils.widgets.get("raw_file_path")

DFS_HOST = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_URI = f"abfss://{BRONZE_CONTAINER}@{DFS_HOST}/{RAW_FILE_PATH.lstrip('/')}"
SILVER_URI = f"abfss://{SILVER_CONTAINER}@{DFS_HOST}/orders"
QUARANTINE_URI = f"abfss://{SILVER_CONTAINER}@{DFS_HOST}/quarantine/corrupted_orders"

print(f"Bronze Input:     {BRONZE_URI}")
print(f"Silver Output:    {SILVER_URI}")
print(f"Quarantine Store: {QUARANTINE_URI}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: ADLS Gen2 Authentication (Direct Spark Conf or Secret Scope)
# MAGIC *(Uncomment and configure your secret scope if not using Unity Catalog / Azure Managed Identity)*

# COMMAND ----------

# Example: Configure OAuth using Azure Key Vault secrets via Databricks Secret Scope
# tenant_id = dbutils.secrets.get(scope="lakehouse-secrets", key="azure-tenant-id")
# client_id = dbutils.secrets.get(scope="lakehouse-secrets", key="azure-client-id")
# client_secret = dbutils.secrets.get(scope="lakehouse-secrets", key="azure-client-secret")
# 
# spark.conf.set(f"fs.azure.account.auth.type.{DFS_HOST}", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{DFS_HOST}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{DFS_HOST}", client_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{DFS_HOST}", client_secret)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{DFS_HOST}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Define Explicit Schema & Read Bronze CSV with Permissive Parsing

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, input_file_name, current_timestamp, trim, lower, to_timestamp, year, month, when, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Explicit Schema with _corrupt_record capture
SCHEMA_ORDERS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# Read with PERMISSIVE mode
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(SCHEMA_ORDERS)
    .load(BRONZE_URI)
    .withColumn("_source_file", input_file_name())
)

print(f"Loaded raw records. Total count: {df_raw.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Quarantine Corrupted Rows & Null Identifiers

# COMMAND ----------

# Define corruption / invalid condition
is_corrupt_condition = (
    col("_corrupt_record").isNotNull()
    | col("order_id").isNull()
    | (trim(col("order_id")) == "")
    | col("customer_id").isNull()
)

df_corrupted = (
    df_raw.filter(is_corrupt_condition)
    .withColumn("_quarantined_at", current_timestamp())
)

df_valid = df_raw.filter(~is_corrupt_condition).drop("_corrupt_record")

corrupt_count = df_corrupted.count()
print(f"Quarantined Records: {corrupt_count:,}")
print(f"Valid Records:       {df_valid.count():,}")

if corrupt_count > 0:
    display(df_corrupted.select("order_id", "_corrupt_record", "_source_file"))
    (
        df_corrupted.write.format("delta")
        .mode("append")
        .save(QUARANTINE_URI)
    )
    print(f"Saved corrupted records to quarantine at: {QUARANTINE_URI}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Data Hygiene, Timestamp Parsing & Partition Extraction

# COMMAND ----------

df_cleaned = (
    df_valid.withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date")))
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
    # Partition columns
    .withColumn("order_year", when(col("order_purchase_timestamp").isNotNull(), year(col("order_purchase_timestamp"))).otherwise(1970))
    .withColumn("order_month", when(col("order_purchase_timestamp").isNotNull(), month(col("order_purchase_timestamp"))).otherwise(1))
    .withColumn("_ingested_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Deduplication by Primary Key

# COMMAND ----------

# Deduplicate by primary key keeping the latest record
window_spec = Window.partitionBy("order_id").orderBy(
    col("order_purchase_timestamp").desc_nulls_last(),
    col("_source_file").desc()
)

df_deduped = (
    df_cleaned.withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

display(df_deduped.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Write to Silver as Partitioned Delta Lake Table (with MERGE)

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, SILVER_URI):
    print(f"Existing Delta table found at {SILVER_URI}. Executing Delta MERGE (Upsert)...")
    silver_table = DeltaTable.forPath(spark, SILVER_URI)
    (
        silver_table.alias("target")
        .merge(
            source=df_deduped.alias("source"),
            condition="target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print(f"Creating new partitioned Delta table at {SILVER_URI}...")
    (
        df_deduped.write.format("delta")
        .partitionBy("order_year", "order_month")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_URI)
    )

print(f"✓ Successfully wrote to Silver Delta Lake: {SILVER_URI}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Delta Lake Maintenance & Query Verification

# COMMAND ----------

# Register table in Spark catalog for SQL queries
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_orders
    USING DELTA
    LOCATION '{SILVER_URI}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize file sizes and Z-Order index on high-cardinality query keys
# MAGIC OPTIMIZE silver_orders ZORDER BY (customer_id, order_purchase_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analytical preview of cleaned orders
# MAGIC SELECT 
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     order_status,
# MAGIC     COUNT(order_id) AS order_count
# MAGIC FROM silver_orders
# MAGIC GROUP BY order_year, order_month, order_status
# MAGIC ORDER BY order_year DESC, order_month DESC;

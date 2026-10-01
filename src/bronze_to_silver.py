"""Bronze to Silver PySpark Transformation Engine.

Medallion Architecture: Bronze (Raw CSV) -> Silver (Cleaned Delta Lake)

Responsibilities:
  1. Read raw CSV batches from ADLS Gen2 bronze/ container with explicit schema & PERMISSIVE mode.
  2. Perform data hygiene:
     - Detect, quarantine, and log malformed / corrupted rows using Spark's `_corrupt_record`.
     - Filter out null or empty primary identifiers.
     - Enforce explicit data types (Double, Timestamp, String).
     - Standardize text columns (trim, lowercase).
     - Deduplicate primary keys (keeping the latest record).
     - Append `_ingested_at` audit timestamp and `_source_file` lineage.
  3. Derive partition columns (e.g. `order_year`, `order_month`).
  4. Write cleaned records into ADLS Gen2 silver/ container as a partitioned Delta Lake table.
  5. Perform Delta Lake Upsert (MERGE) if target table already exists.

Usage:
  python src/bronze_to_silver.py --dataset ecommerce --table orders
  python src/bronze_to_silver.py --dataset nyctaxi
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lower,
    month,
    row_number,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from config.settings import settings
from config.spark_session import get_spark_session
from src.connectors.adls_connector import ADLSConnector
from src.utils.logger import setup_logger

logger = setup_logger("bronze_to_silver")


# ==============================================================================
# 1. EXPLICIT SCHEMAS (BRONZE INGESTION)
# ==============================================================================
# Permissive schema capturing malformed CSV rows into _corrupt_record
SCHEMA_ECOMMERCE_ORDERS = StructType([
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

SCHEMA_NYC_TAXI = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("congestion_surcharge", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


# ==============================================================================
# 2. BRONZE TO SILVER TRANSFORMER
# ==============================================================================
class BronzeToSilverTransformer:
    """Transforms raw bronze CSV batches into cleansed, partitioned Delta Lake tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()

    def read_bronze_csv(
        self,
        source_path: str,
        schema: StructType,
    ) -> DataFrame:
        """Reads raw CSV batch using PERMISSIVE mode, isolating corrupted records."""
        logger.info(f"Reading raw CSV batch from: {source_path}")

        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .load(source_path)
            .withColumn("_source_file", input_file_name())
        )

    def quarantine_corrupted_records(
        self,
        df_raw: DataFrame,
        primary_key_col: str,
        quarantine_table_name: str,
    ) -> tuple[DataFrame, DataFrame]:
        """Separates valid records from corrupted or invalid rows.

        Corrupted rows are logged and written to the quarantine path.
        Returns: (df_valid, df_corrupted)
        """
        # Flag corrupt rows from CSV parser or rows with missing/blank primary keys
        is_corrupt_condition = (
            col("_corrupt_record").isNotNull()
            | col(primary_key_col).isNull()
            | (trim(col(primary_key_col)) == "")
        )

        df_corrupted = (
            df_raw.filter(is_corrupt_condition)
            .withColumn("_quarantined_at", current_timestamp())
        )

        df_valid = df_raw.filter(~is_corrupt_condition).drop("_corrupt_record")

        corrupt_count = df_corrupted.count()
        if corrupt_count > 0:
            logger.warning(
                f"🚨 Detected {corrupt_count} corrupted or invalid records! "
                f"Writing to quarantine table '{quarantine_table_name}'..."
            )
            quarantine_path = self.adls.get_table_path("silver", f"quarantine/{quarantine_table_name}")
            (
                df_corrupted.write.format("delta")
                .mode("append")
                .save(quarantine_path)
            )
            # Sample corrupted records for inspection
            logger.warning(
                f"Sample corrupted row: {df_corrupted.select(primary_key_col, '_corrupt_record', '_source_file').first()}"
            )
        else:
            logger.info("✓ Zero corrupted or invalid rows detected.")

        return df_valid, df_corrupted

    def clean_ecommerce_orders(self, df_valid: DataFrame) -> DataFrame:
        """Cleanses orders dataset:

        - Enforces explicit types and trims text
        - Standardizes order_status to lowercase
        - Parses UTC timestamps
        - Deduplicates by primary key (order_id)
        - Extracts order_year and order_month for Delta Lake partitioning
        - Appends `_ingested_at` audit timestamp
        """
        logger.info("Executing hygiene on E-Commerce orders...")

        # 1. Type casting and timestamp parsing
        df_typed = (
            df_valid.withColumn("order_id", trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("order_status", lower(trim(col("order_status"))))
            .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
            .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
            .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date")))
            .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
            .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
        )

        # 2. Derive partition columns from order_purchase_timestamp
        df_partitioned = (
            df_typed.withColumn(
                "order_year",
                when(col("order_purchase_timestamp").isNotNull(), year(col("order_purchase_timestamp"))).otherwise(1970),
            )
            .withColumn(
                "order_month",
                when(col("order_purchase_timestamp").isNotNull(), month(col("order_purchase_timestamp"))).otherwise(1),
            )
            .withColumn("_ingested_at", current_timestamp())
        )

        # 3. Deduplication on primary key (order_id) keeping latest purchase timestamp
        window_spec = Window.partitionBy("order_id").orderBy(
            col("order_purchase_timestamp").desc_nulls_last(),
            col("_source_file").desc(),
        )

        df_deduped = (
            df_partitioned.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

        return df_deduped

    def clean_nyctaxi_trips(self, df_valid: DataFrame) -> DataFrame:
        """Cleanses NYC Yellow Taxi trips:

        - Casts numeric and monetary metrics to Double / Integer
        - Parses pickup and dropoff timestamps
        - Filters out nonsensical negative fares or zero distances
        - Extracts pickup_year and pickup_month for partitioning
        - Appends `_ingested_at` audit timestamp
        """
        logger.info("Executing hygiene on NYC Taxi trips...")

        df_typed = (
            df_valid.withColumn("VendorID", col("VendorID").cast(IntegerType()))
            .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
            .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
            .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
            .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
            .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
            .withColumn("store_and_fwd_flag", trim(col("store_and_fwd_flag")))
            .withColumn("PULocationID", col("PULocationID").cast(IntegerType()))
            .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType()))
            .withColumn("payment_type", col("payment_type").cast(IntegerType()))
            .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
            .withColumn("extra", col("extra").cast(DoubleType()))
            .withColumn("mta_tax", col("mta_tax").cast(DoubleType()))
            .withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
            .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType()))
            .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType()))
            .withColumn("total_amount", col("total_amount").cast(DoubleType()))
            .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType()))
        )

        # Hygiene filters: total_amount must be >= 0 and trip_distance >= 0
        df_cleaned = df_typed.filter(
            (col("fare_amount") >= 0.0)
            & (col("total_amount") >= 0.0)
            & (col("trip_distance") >= 0.0)
            & col("pickup_datetime").isNotNull()
        )

        # Derive partition keys
        df_partitioned = (
            df_cleaned.withColumn("pickup_year", year(col("pickup_datetime")))
            .withColumn("pickup_month", month(col("pickup_datetime")))
            .withColumn("_ingested_at", current_timestamp())
        )

        # Deduplicate on vendor + pickup + pulocation
        df_deduped = df_partitioned.dropDuplicates(["VendorID", "pickup_datetime", "PULocationID"])
        return df_deduped

    def write_silver_delta(
        self,
        df: DataFrame,
        target_table_name: str,
        partition_cols: list[str],
        primary_key: str | None = None,
    ) -> str:
        """Writes the cleaned DataFrame to ADLS Gen2 silver/ as a partitioned Delta Lake table.

        Performs Delta MERGE (upsert) if primary_key is provided and table exists.
        """
        target_path = self.adls.get_table_path("silver", target_table_name)
        logger.info(f"Writing Silver Delta table to: {target_path} (partitioned by {partition_cols})")

        if primary_key and DeltaTable.isDeltaTable(self.spark, target_path):
            logger.info(f"Existing Delta table found at {target_path}. Executing idempotent MERGE (Upsert)...")
            delta_target = DeltaTable.forPath(self.spark, target_path)

            (
                delta_target.alias("target")
                .merge(
                    source=df.alias("source"),
                    condition=f"target.{primary_key} = source.{primary_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info(f"Initializing partitioned Delta table at {target_path}...")
            (
                df.write.format("delta")
                .partitionBy(*partition_cols)
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )

        total_records = self.spark.read.format("delta").load(target_path).count()
        logger.info(f"✓ Silver table '{target_table_name}' now contains {total_records:,} cleaned records.")
        return target_path


# ==============================================================================
# 3. PIPELINE RUNNER & CLI
# ==============================================================================
def run_bronze_to_silver(
    dataset: str = "ecommerce",
    table: str = "orders",
    source_path: str | None = None,
) -> str:
    """Executes Bronze -> Silver transformation pipeline."""
    spark = get_spark_session(f"BronzeToSilver-{dataset}-{table}")
    transformer = BronzeToSilverTransformer(spark)

    try:
        if dataset == "ecommerce" and table == "orders":
            # 1. Resolve bronze path
            default_bronze = (
                settings.get_layer_path("bronze", "raw/ecommerce/olist_orders_dataset.csv")
                if settings.USE_LOCAL_STORAGE_EMULATION
                else settings.azure.get_abfss_uri(
                    settings.azure.CONTAINER_BRONZE, "raw/ecommerce/olist_orders_dataset.csv"
                )
            )
            raw_csv_path = source_path or default_bronze

            # 2. Read with schema & permissive corrupt detection
            df_raw = transformer.read_bronze_csv(raw_csv_path, SCHEMA_ECOMMERCE_ORDERS)

            # 3. Quarantine corrupt rows
            df_valid, _ = transformer.quarantine_corrupted_records(
                df_raw,
                primary_key_col="order_id",
                quarantine_table_name="corrupted_orders",
            )

            # 4. Cleanse, type cast, deduplicate
            df_clean = transformer.clean_ecommerce_orders(df_valid)

            # 5. Write partitioned Delta Lake table
            target_path = transformer.write_silver_delta(
                df=df_clean,
                target_table_name="orders",
                partition_cols=["order_year", "order_month"],
                primary_key="order_id",
            )
            return target_path

        elif dataset == "nyctaxi":
            default_bronze = (
                settings.get_layer_path("bronze", "raw/nyctaxi/nyc_yellow_taxi_trips.csv")
                if settings.USE_LOCAL_STORAGE_EMULATION
                else settings.azure.get_abfss_uri(
                    settings.azure.CONTAINER_BRONZE, "raw/nyctaxi/nyc_yellow_taxi_trips.csv"
                )
            )
            raw_csv_path = source_path or default_bronze

            df_raw = transformer.read_bronze_csv(raw_csv_path, SCHEMA_NYC_TAXI)
            df_valid, _ = transformer.quarantine_corrupted_records(
                df_raw,
                primary_key_col="VendorID",
                quarantine_table_name="corrupted_nyctaxi",
            )
            df_clean = transformer.clean_nyctaxi_trips(df_valid)

            target_path = transformer.write_silver_delta(
                df=df_clean,
                target_table_name="nyc_taxi_trips",
                partition_cols=["pickup_year", "pickup_month"],
                primary_key=None,
            )
            return target_path

        else:
            raise ValueError(f"Unsupported dataset/table combination: dataset={dataset}, table={table}")

    finally:
        pass


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver Medallion Architecture Transformation")
    parser.add_argument(
        "--dataset",
        choices=["ecommerce", "nyctaxi"],
        default="ecommerce",
        help="Dataset family (default: ecommerce)",
    )
    parser.add_argument(
        "--table",
        choices=["orders"],
        default="orders",
        help="Table name to process for ecommerce (default: orders)",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        default=None,
        help="Optional explicit Bronze CSV source path",
    )
    args = parser.parse_args()

    target_path = run_bronze_to_silver(
        dataset=args.dataset,
        table=args.table,
        source_path=args.source_path,
    )
    print(f"\n🎉 Successfully completed Bronze -> Silver transformation. Delta Table: {target_path}\n")


if __name__ == "__main__":
    main()

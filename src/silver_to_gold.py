"""Silver to Gold PySpark Analytical Aggregation Engine.

Medallion Architecture: Silver (Cleaned Delta Tables) -> Gold (Aggregated Business Marts)

Responsibilities:
  1. Read cleansed Delta Lake tables from ADLS Gen2 silver/ container (or local emulation).
  2. Perform multi-dimensional analytical rollups:
     - Daily aggregates by date, status, zone/category.
     - Volume metrics, total revenue, average order value, delivery lead times, average distance.
  3. Enforce strict column-level schema definitions for deterministic downstream BI consumption.
  4. Write curated business marts into the ADLS Gen2 gold/ container as partitioned Delta Lake tables.
  5. Optimize Delta Lake tables via Compaction and multidimensional Z-Ordering.

Usage:
  python src/silver_to_gold.py --dataset ecommerce
  python src/silver_to_gold.py --dataset nyctaxi
  python src/silver_to_gold.py --dataset all
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
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    month,
    round,
    sum,
    to_date,
    when,
    year,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import settings
from config.spark_session import get_spark_session
from src.connectors.adls_connector import ADLSConnector
from src.utils.logger import setup_logger

logger = setup_logger("silver_to_gold")


# ==============================================================================
# 1. COLUMN-LEVEL GOLD SCHEMAS (STRICT BUSINESS MARTS)
# ==============================================================================
SCHEMA_GOLD_DAILY_ORDERS = StructType([
    StructField("order_date", DateType(), False),
    StructField("order_status", StringType(), False),
    StructField("total_orders", LongType(), False),
    StructField("unique_customers", LongType(), False),
    StructField("avg_delivery_days", DoubleType(), True),
    StructField("delivered_orders", LongType(), False),
    StructField("cancelled_orders", LongType(), False),
    StructField("report_year", IntegerType(), False),
    StructField("report_month", IntegerType(), False),
    StructField("_gold_calculated_at", TimestampType(), False),
])

SCHEMA_GOLD_DAILY_ZONE_METRICS = StructType([
    StructField("pickup_date", DateType(), False),
    StructField("PULocationID", IntegerType(), False),
    StructField("payment_type", IntegerType(), False),
    StructField("total_trips", LongType(), False),
    StructField("total_revenue", DoubleType(), False),
    StructField("avg_trip_distance", DoubleType(), False),
    StructField("avg_fare_per_trip", DoubleType(), False),
    StructField("avg_tip_amount", DoubleType(), False),
    StructField("report_year", IntegerType(), False),
    StructField("report_month", IntegerType(), False),
    StructField("_gold_calculated_at", TimestampType(), False),
])


# ==============================================================================
# 2. SILVER TO GOLD AGGREGATION ENGINE
# ==============================================================================
class SilverToGoldAggregator:
    """Computes multi-dimensional business marts and writes optimized Gold Delta tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()

    def aggregate_ecommerce_orders(self, df_silver_orders: DataFrame) -> DataFrame:
        """Rolls up silver orders into a daily business aggregation mart:

        - Grain: order_date + order_status
        - Metrics: total volume, customer reach, delivery performance
        """
        logger.info("Computing multi-dimensional rollups for E-Commerce daily orders...")

        df_daily = (
            df_silver_orders.withColumn("order_date", to_date("order_purchase_timestamp"))
            .withColumn(
                "delivery_lead_days",
                datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")),
            )
            .groupBy("order_date", "order_status")
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                round(avg(when(col("order_status") == "delivered", col("delivery_lead_days"))), 2).alias(
                    "avg_delivery_days"
                ),
                sum(when(col("order_status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
                sum(when(col("order_status") == "canceled", 1).otherwise(0)).alias("cancelled_orders"),
            )
            .withColumn("report_year", year("order_date"))
            .withColumn("report_month", month("order_date"))
            .withColumn("_gold_calculated_at", current_timestamp())
        )

        # Enforce column selection and types matching SCHEMA_GOLD_DAILY_ORDERS
        df_enforced = (
            df_daily.select(
                col("order_date").cast(DateType()),
                col("order_status").cast(StringType()),
                col("total_orders").cast(LongType()),
                col("unique_customers").cast(LongType()),
                col("avg_delivery_days").cast(DoubleType()),
                col("delivered_orders").cast(LongType()),
                col("cancelled_orders").cast(LongType()),
                col("report_year").cast(IntegerType()),
                col("report_month").cast(IntegerType()),
                col("_gold_calculated_at").cast(TimestampType()),
            )
            .filter(col("order_date").isNotNull())
            .orderBy("order_date", "order_status")
        )

        return df_enforced

    def aggregate_nyctaxi_zone_metrics(self, df_silver_taxi: DataFrame) -> DataFrame:
        """Rolls up silver taxi trips into daily zone & payment performance mart:

        - Grain: pickup_date + PULocationID + payment_type
        - Metrics: trip volume, total revenue, average distance, average fare, tips
        """
        logger.info("Computing multi-dimensional rollups for NYC Taxi zone metrics...")

        df_zone = (
            df_silver_taxi.withColumn("pickup_date", to_date("pickup_datetime"))
            .groupBy("pickup_date", "PULocationID", "payment_type")
            .agg(
                count("*").alias("total_trips"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                round(avg("fare_amount"), 2).alias("avg_fare_per_trip"),
                round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            )
            .withColumn("report_year", year("pickup_date"))
            .withColumn("report_month", month("pickup_date"))
            .withColumn("_gold_calculated_at", current_timestamp())
        )

        df_enforced = (
            df_zone.select(
                col("pickup_date").cast(DateType()),
                col("PULocationID").cast(IntegerType()),
                col("payment_type").cast(IntegerType()),
                col("total_trips").cast(LongType()),
                col("total_revenue").cast(DoubleType()),
                col("avg_trip_distance").cast(DoubleType()),
                col("avg_fare_per_trip").cast(DoubleType()),
                col("avg_tip_amount").cast(DoubleType()),
                col("report_year").cast(IntegerType()),
                col("report_month").cast(IntegerType()),
                col("_gold_calculated_at").cast(TimestampType()),
            )
            .filter(col("pickup_date").isNotNull() & col("PULocationID").isNotNull())
            .orderBy("pickup_date", "PULocationID")
        )

        return df_enforced

    def save_gold_mart(
        self,
        df: DataFrame,
        target_mart_name: str,
        partition_cols: list[str],
        zorder_cols: list[str] | None = None,
    ) -> str:
        """Saves aggregated business mart to gold/ container with schema enforcement & Z-Ordering."""
        target_path = self.adls.get_table_path("gold", target_mart_name)
        logger.info(f"[Gold] Saving business mart to Delta Lake: {target_path}")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*partition_cols)
            .save(target_path)
        )

        # Delta Lake Table Optimization & Multidimensional Z-Ordering
        delta_table = DeltaTable.forPath(self.spark, target_path)

        if zorder_cols:
            logger.info(f"[Gold] Executing multidimensional Z-Ordering on: {zorder_cols}...")
            try:
                delta_table.optimize().executeZOrderBy(*zorder_cols)
                logger.info(f"[Gold] ✓ Successfully applied Z-Ordering on {zorder_cols}.")
            except Exception as e:
                logger.warning(f"[Gold] Z-Order optimization skipped/unsupported in this engine context: {e}")
        else:
            logger.info("[Gold] Executing Delta compaction...")
            try:
                delta_table.optimize().executeCompaction()
            except Exception as e:
                logger.warning(f"[Gold] Compaction skipped: {e}")

        total_rows = delta_table.toDF().count()
        logger.info(f"[Gold] ✓ Successfully created Gold mart '{target_mart_name}' ({total_rows:,} aggregated rows).")
        return target_path


# ==============================================================================
# 3. CLI ENTRYPOINT
# ==============================================================================
def run_silver_to_gold(dataset: str = "ecommerce") -> dict[str, str]:
    """Executes Silver -> Gold analytical aggregation pipeline."""
    spark = get_spark_session(f"SilverToGold-{dataset}")
    aggregator = SilverToGoldAggregator(spark)
    results = {}

    try:
        adls = aggregator.adls

        if dataset in ("ecommerce", "all"):
            silver_orders_path = adls.get_table_path("silver", "orders")
            if not DeltaTable.isDeltaTable(spark, silver_orders_path):
                raise FileNotFoundError(f"Silver Delta table not found at: {silver_orders_path}. Run bronze_to_silver first.")

            df_silver_orders = spark.read.format("delta").load(silver_orders_path)
            df_gold_orders = aggregator.aggregate_ecommerce_orders(df_silver_orders)

            path_orders_mart = aggregator.save_gold_mart(
                df=df_gold_orders,
                target_mart_name="gold_daily_orders_summary",
                partition_cols=["report_year", "report_month"],
                zorder_cols=["order_status"],
            )
            results["gold_daily_orders_summary"] = path_orders_mart

        if dataset in ("nyctaxi", "all"):
            silver_taxi_path = adls.get_table_path("silver", "nyc_taxi_trips")
            if not DeltaTable.isDeltaTable(spark, silver_taxi_path):
                raise FileNotFoundError(f"Silver Delta table not found at: {silver_taxi_path}. Run bronze_to_silver first.")

            df_silver_taxi = spark.read.format("delta").load(silver_taxi_path)
            df_gold_taxi = aggregator.aggregate_nyctaxi_zone_metrics(df_silver_taxi)

            path_taxi_mart = aggregator.save_gold_mart(
                df=df_gold_taxi,
                target_mart_name="gold_daily_zone_metrics",
                partition_cols=["report_year", "report_month"],
                zorder_cols=["PULocationID", "payment_type"],
            )
            results["gold_daily_zone_metrics"] = path_taxi_mart

        return results

    finally:
        pass


def main():
    parser = argparse.ArgumentParser(description="Silver to Gold Medallion Architecture Analytical Aggregator")
    parser.add_argument(
        "--dataset",
        choices=["ecommerce", "nyctaxi", "all"],
        default="ecommerce",
        help="Dataset family to aggregate into business marts (default: ecommerce)",
    )
    args = parser.parse_args()

    results = run_silver_to_gold(dataset=args.dataset)
    print("\n" + "=" * 70)
    print("🎉 SILVER ➡️ GOLD ANALYTICAL AGGREGATION COMPLETE")
    print("=" * 70)
    for mart_name, path in results.items():
        print(f"  ✓ {mart_name}: {path}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

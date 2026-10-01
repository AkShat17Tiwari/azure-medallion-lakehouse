"""Unit tests for Silver transformations and Gold aggregations."""
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.silver.transformation import SilverTransformation
from src.gold.aggregation import GoldAggregation


def test_silver_clean_orders_deduplication_and_nulls(spark: SparkSession):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
    ])

    # Row 1 and Row 2 have the same order_id (duplicate), Row 2 has newer _ingested_at
    # Row 3 has null order_id (should be filtered out)
    data = [
        ("ORD-1", "CUST-A", "2026-09-25 10:00:00", 100.0, "COMPLETED", datetime(2026, 9, 25, 10, 0, 0)),
        ("ORD-1", "CUST-A", "2026-09-25 10:00:00", 100.0, "COMPLETED", datetime(2026, 9, 25, 10, 5, 0)),
        (None, "CUST-B", "2026-09-25 11:00:00", 50.0, "COMPLETED", datetime(2026, 9, 25, 11, 0, 0)),
        ("ORD-2", "CUST-B", "2026-09-25 12:00:00", 250.0, "CANCELLED", datetime(2026, 9, 25, 12, 0, 0)),
    ]

    df_bronze = spark.createDataFrame(data, schema=schema)
    transformer = SilverTransformation(spark)
    df_silver = transformer.clean_orders(df_bronze)

    rows = df_silver.collect()
    # Null order_id dropped and duplicate resolved => 2 distinct orders
    assert len(rows) == 2

    order_ids = {r["order_id"] for r in rows}
    assert order_ids == {"ORD-1", "ORD-2"}

    # Status should be lowercased
    statuses = {r["status"] for r in rows}
    assert statuses == {"completed", "cancelled"}


def test_gold_aggregation_metrics(spark: SparkSession):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
    ])

    data = [
        ("ORD-1", "CUST-A", 100.0, "completed", datetime(2026, 9, 25, 10, 0, 0)),
        ("ORD-2", "CUST-A", 50.0, "completed", datetime(2026, 9, 25, 14, 0, 0)),
        ("ORD-3", "CUST-B", 200.0, "cancelled", datetime(2026, 9, 25, 16, 0, 0)),
    ]

    df_silver = spark.createDataFrame(data, schema=schema)
    gold = GoldAggregation(spark)

    # 1. Test Daily Sales Summary
    df_daily = gold.build_daily_sales_summary(df_silver)
    daily_row = df_daily.first()
    assert daily_row["total_orders"] == 3
    assert daily_row["unique_customers"] == 2
    assert daily_row["total_completed_revenue"] == 150.0
    assert daily_row["completed_orders"] == 2
    assert daily_row["cancelled_orders"] == 1

    # 2. Test Customer Summary
    df_cust = gold.build_customer_summary(df_silver)
    cust_a = df_cust.filter(df_cust.customer_id == "CUST-A").first()
    assert cust_a["total_orders"] == 2
    assert cust_a["total_lifetime_spend"] == 150.0
    assert cust_a["avg_order_value"] == 75.0

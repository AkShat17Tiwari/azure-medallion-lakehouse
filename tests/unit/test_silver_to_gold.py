"""Unit tests for SilverToGoldAggregator."""
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.silver_to_gold import SilverToGoldAggregator


def test_aggregate_ecommerce_orders(spark: SparkSession):
    aggregator = SilverToGoldAggregator(spark)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
    ])

    data = [
        # Day 1: delivered order with 5 days lead time
        ("ORD-1", "CUST-1", "delivered", datetime(2026, 5, 1, 10, 0, 0), datetime(2026, 5, 6, 10, 0, 0)),
        # Day 1: second delivered order for same customer with 3 days lead time
        ("ORD-2", "CUST-1", "delivered", datetime(2026, 5, 1, 15, 0, 0), datetime(2026, 5, 4, 15, 0, 0)),
        # Day 1: canceled order
        ("ORD-3", "CUST-2", "canceled", datetime(2026, 5, 1, 12, 0, 0), None),
        # Day 2: delivered order
        ("ORD-4", "CUST-3", "delivered", datetime(2026, 5, 2, 8, 0, 0), datetime(2026, 5, 5, 8, 0, 0)),
    ]

    df_silver = spark.createDataFrame(data, schema=schema)
    df_gold = aggregator.aggregate_ecommerce_orders(df_silver)

    rows = df_gold.collect()
    assert len(rows) == 3  # (May 1, delivered), (May 1, canceled), (May 2, delivered)

    # Inspect (May 1, delivered)
    may1_del = [r for r in rows if str(r["order_date"]) == "2026-05-01" and r["order_status"] == "delivered"][0]
    assert may1_del["total_orders"] == 2
    assert may1_del["unique_customers"] == 1
    assert may1_del["delivered_orders"] == 2
    assert may1_del["cancelled_orders"] == 0
    assert may1_del["avg_delivery_days"] == 4.0  # (5 + 3) / 2
    assert may1_del["report_year"] == 2026
    assert may1_del["report_month"] == 5


def test_aggregate_nyctaxi_zone_metrics(spark: SparkSession):
    aggregator = SilverToGoldAggregator(spark)

    schema = StructType([
        StructField("pickup_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
    ])

    data = [
        # Trip 1: Zone 10, Credit Card (payment_type=1)
        (datetime(2026, 6, 1, 9, 0, 0), 10, 1, 30.0, 5.0, 25.0, 5.0),
        # Trip 2: Zone 10, Credit Card (payment_type=1)
        (datetime(2026, 6, 1, 10, 0, 0), 10, 1, 50.0, 10.0, 40.0, 10.0),
        # Trip 3: Zone 10, Cash (payment_type=2)
        (datetime(2026, 6, 1, 11, 0, 0), 10, 2, 20.0, 3.0, 20.0, 0.0),
    ]

    df_silver = spark.createDataFrame(data, schema=schema)
    df_gold = aggregator.aggregate_nyctaxi_zone_metrics(df_silver)

    rows = df_gold.collect()
    assert len(rows) == 2  # (Zone 10, Card) and (Zone 10, Cash)

    card_row = [r for r in rows if r["payment_type"] == 1][0]
    assert card_row["total_trips"] == 2
    assert card_row["total_revenue"] == 80.0
    assert card_row["avg_trip_distance"] == 7.5
    assert card_row["avg_fare_per_trip"] == 32.5
    assert card_row["avg_tip_amount"] == 7.5

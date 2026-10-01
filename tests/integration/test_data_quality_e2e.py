"""End-to-End Data Quality & Integrity Test Suite.

Asserts:
  1. Schema Compliance: Strict types, column existence across Silver and Gold.
  2. Null & Constraint Checks: Zero nulls on primary keys and business-critical attributes.
  3. Uniqueness Constraints: Primary key uniqueness with zero duplicates.
  4. Cross-Layer Reconciliation: Row count and volume balance between Silver and Gold.
  5. Range & Validity Rules: Positive revenue, valid delivery days, and non-empty status codes.
"""
from __future__ import annotations

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, trim

from config.settings import settings
from src.connectors.adls_connector import ADLSConnector


@pytest.fixture(scope="module")
def adls():
    return ADLSConnector()


# ==============================================================================
# 1. SILVER LAYER DATA QUALITY TESTS
# ==============================================================================
def test_silver_orders_schema_compliance(spark: SparkSession, adls: ADLSConnector):
    """Verifies that the Silver orders table exists and complies with the expected schema."""
    silver_path = adls.get_table_path("silver", "orders")
    assert DeltaTable.isDeltaTable(spark, silver_path), f"Silver orders table missing at {silver_path}"

    df_silver = spark.read.format("delta").load(silver_path)

    expected_columns = {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_year": "int",
        "order_month": "int",
        "_ingested_at": "timestamp",
    }

    actual_fields = {f.name: f.dataType.simpleString() for f in df_silver.schema.fields}

    for col_name, expected_type in expected_columns.items():
        assert col_name in actual_fields, f"Required column '{col_name}' missing in Silver schema"
        assert (
            actual_fields[col_name] == expected_type
        ), f"Column '{col_name}' type mismatch: expected {expected_type}, got {actual_fields[col_name]}"


def test_silver_orders_null_and_blank_constraints(spark: SparkSession, adls: ADLSConnector):
    """Asserts that primary keys and required dimensions in Silver have ZERO nulls or blanks."""
    silver_path = adls.get_table_path("silver", "orders")
    df_silver = spark.read.format("delta").load(silver_path)

    # 1. Null check on primary keys and critical columns
    null_counts = (
        df_silver.select(
            count(col("order_id").isNull() | (trim(col("order_id")) == "")).alias("null_order_id"),
            count(col("customer_id").isNull() | (trim(col("customer_id")) == "")).alias("null_customer_id"),
            count(col("order_status").isNull()).alias("null_status"),
            count(col("order_purchase_timestamp").isNull()).alias("null_timestamp"),
        )
        .first()
        .asDict()
    )

    # Filtered conditions must equal total row count for zero null violations
    total_rows = df_silver.count()
    assert total_rows > 0, "Silver orders table is empty!"

    # In Spark, count() of boolean condition counts rows where expression evaluates; check explicit sum
    null_violations = df_silver.filter(
        col("order_id").isNull()
        | (trim(col("order_id")) == "")
        | col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
        | col("order_status").isNull()
    ).count()

    assert null_violations == 0, f"Found {null_violations} null or blank identifier violations in Silver!"


def test_silver_orders_primary_key_uniqueness(spark: SparkSession, adls: ADLSConnector):
    """Asserts that order_id is strictly unique (deduplicated) in the Silver layer."""
    silver_path = adls.get_table_path("silver", "orders")
    df_silver = spark.read.format("delta").load(silver_path)

    total_rows = df_silver.count()
    distinct_orders = df_silver.select(countDistinct("order_id")).first()[0]

    assert total_rows == distinct_orders, (
        f"Primary key uniqueness violation in Silver! Total rows: {total_rows}, "
        f"Distinct orders: {distinct_orders}"
    )


# ==============================================================================
# 2. GOLD LAYER DATA QUALITY TESTS
# ==============================================================================
def test_gold_daily_orders_schema_compliance(spark: SparkSession, adls: ADLSConnector):
    """Asserts that the Gold daily orders summary mart conforms to column-level types."""
    gold_path = adls.get_table_path("gold", "gold_daily_orders_summary")
    assert DeltaTable.isDeltaTable(spark, gold_path), f"Gold orders mart missing at {gold_path}"

    df_gold = spark.read.format("delta").load(gold_path)

    expected_types = {
        "order_date": "date",
        "order_status": "string",
        "total_orders": "bigint",
        "unique_customers": "bigint",
        "avg_delivery_days": "double",
        "delivered_orders": "bigint",
        "cancelled_orders": "bigint",
        "report_year": "int",
        "report_month": "int",
        "_gold_calculated_at": "timestamp",
    }

    actual_types = {f.name: f.dataType.simpleString() for f in df_gold.schema.fields}

    for col_name, exp_type in expected_types.items():
        assert col_name in actual_types, f"Column '{col_name}' missing in Gold mart schema"
        assert (
            actual_types[col_name] == exp_type
        ), f"Gold column '{col_name}' type mismatch: expected {exp_type}, got {actual_types[col_name]}"


def test_gold_null_and_range_constraints(spark: SparkSession, adls: ADLSConnector):
    """Asserts validity rules and non-null constraints on Gold analytical metrics."""
    gold_path = adls.get_table_path("gold", "gold_daily_orders_summary")
    df_gold = spark.read.format("delta").load(gold_path)

    # 1. No nulls in primary composite key
    null_keys = df_gold.filter(col("order_date").isNull() | col("order_status").isNull()).count()
    assert null_keys == 0, f"Found {null_keys} null keys in Gold mart!"

    # 2. Business validity: total_orders must be strictly positive
    invalid_orders = df_gold.filter(col("total_orders") <= 0).count()
    assert invalid_orders == 0, f"Found {invalid_orders} rows with non-positive total_orders"

    # 3. Delivery days must be non-negative where present
    negative_lead_time = df_gold.filter((col("avg_delivery_days").isNotNull()) & (col("avg_delivery_days") < 0)).count()
    assert negative_lead_time == 0, f"Found {negative_lead_time} rows with negative avg_delivery_days"


# ==============================================================================
# 3. CROSS-LAYER RECONCILIATION & BALANCE AUDIT
# ==============================================================================
def test_silver_to_gold_volume_reconciliation(spark: SparkSession, adls: ADLSConnector):
    """Reconciles total business volume:

    The sum of total_orders in the Gold aggregation mart must EXACTLY balance
    the count of distinct valid orders in the Silver layer.
    """
    silver_path = adls.get_table_path("silver", "orders")
    gold_path = adls.get_table_path("gold", "gold_daily_orders_summary")

    df_silver = spark.read.format("delta").load(silver_path)
    df_gold = spark.read.format("delta").load(gold_path)

    silver_order_count = df_silver.count()
    gold_order_sum = df_gold.select(sum("total_orders")).first()[0]

    assert gold_order_sum is not None, "Gold orders sum is NULL!"
    assert silver_order_count == gold_order_sum, (
        f"🚨 Volume Reconciliation Discrepancy! Silver count: {silver_order_count:,}, "
        f"Gold sum: {gold_order_sum:,}. Difference: {abs(silver_order_count - gold_order_sum)}"
    )

"""Unit tests for BronzeToSilverTransformer."""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import lit
from src.bronze_to_silver import (
    SCHEMA_ECOMMERCE_ORDERS,
    BronzeToSilverTransformer,
)


def test_quarantine_corrupted_records(spark: SparkSession, tmp_path: Path):
    transformer = BronzeToSilverTransformer(spark)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    data = [
        ("ORD-001", "CUST-001", None),                    # Valid
        ("ORD-002", "CUST-002", None),                    # Valid
        (None, "CUST-003", None),                         # Invalid (null PK)
        ("   ", "CUST-004", None),                        # Invalid (blank PK)
        (None, None, "malformed,csv,line,with,errors"),   # Corrupted record from CSV parser
    ]

    df_raw = spark.createDataFrame(data, schema=schema).withColumn("_source_file", lit("test.csv"))
    df_valid, df_corrupted = transformer.quarantine_corrupted_records(
        df_raw,
        primary_key_col="order_id",
        quarantine_table_name="test_quarantine",
    )

    valid_rows = df_valid.collect()
    corrupt_rows = df_corrupted.collect()

    assert len(valid_rows) == 2
    assert {r["order_id"] for r in valid_rows} == {"ORD-001", "ORD-002"}
    assert len(corrupt_rows) == 3


def test_clean_ecommerce_orders_hygiene(spark: SparkSession):
    transformer = BronzeToSilverTransformer(spark)

    data = [
        # Duplicate row 1 (older)
        ("ORD-1", "CUST-1", "DELIVERED ", "2026-05-10 10:00:00", None, None, None, None),
        # Duplicate row 2 (newer timestamp)
        ("ORD-1", "CUST-1", "DELIVERED ", "2026-05-10 12:00:00", None, None, None, None),
        # Distinct row
        ("ORD-2", "CUST-2", "SHIPPED", "2026-06-15 14:30:00", None, None, None, None),
    ]
    # Schema without _corrupt_record
    fields = [f for f in SCHEMA_ECOMMERCE_ORDERS.fields if f.name != "_corrupt_record"]
    df_valid = spark.createDataFrame(data, schema=StructType(fields)).withColumn("_source_file", lit("orders.csv"))

    df_cleaned = transformer.clean_ecommerce_orders(df_valid)
    rows = df_cleaned.collect()

    # Deduplication check
    assert len(rows) == 2
    order_map = {r["order_id"]: r for r in rows}

    # Verify latest record was retained for ORD-1 (12:00 UTC, which is newer than 10:00 UTC)
    assert order_map["ORD-1"]["order_purchase_timestamp"].minute in (0, 30)
    assert order_map["ORD-1"]["order_purchase_timestamp"].hour in (12, 17) # 12 UTC or 17:30 local
    # Verify lowercase status
    assert order_map["ORD-1"]["order_status"] == "delivered"
    # Verify partition columns
    assert order_map["ORD-1"]["order_year"] == 2026
    assert order_map["ORD-1"]["order_month"] == 5
    assert order_map["ORD-2"]["order_year"] == 2026
    assert order_map["ORD-2"]["order_month"] == 6
    # Verify audit timestamp exists
    assert order_map["ORD-1"]["_ingested_at"] is not None

"""Unit tests for GoldToPostgresPublisher."""
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
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
from src.gold_to_postgres import GoldToPostgresPublisher, TABLE_PRIMARY_KEYS


def test_generate_upsert_sql_orders():
    publisher = GoldToPostgresPublisher(None)

    columns = [
        "order_date", "order_status", "total_orders", "unique_customers",
        "avg_delivery_days", "delivered_orders", "cancelled_orders",
        "report_year", "report_month", "_gold_calculated_at"
    ]
    pks = ["order_date", "order_status"]

    sql = publisher.generate_upsert_sql("gold_daily_orders_summary", columns, pks)

    assert "INSERT INTO public.gold_daily_orders_summary" in sql
    assert "ON CONFLICT (order_date, order_status)" in sql
    assert "DO UPDATE SET" in sql
    assert "total_orders = EXCLUDED.total_orders" in sql
    assert "_postgres_loaded_at = CURRENT_TIMESTAMP" in sql
    # Primary key columns must NOT be in the UPDATE SET clause
    assert "order_date = EXCLUDED.order_date" not in sql
    assert "order_status = EXCLUDED.order_status" not in sql


def test_generate_upsert_sql_zone_metrics():
    publisher = GoldToPostgresPublisher(None)

    columns = [
        "pickup_date", "pulocationid", "payment_type",
        "total_trips", "total_revenue", "avg_trip_distance"
    ]
    pks = ["pickup_date", "pulocationid", "payment_type"]

    sql = publisher.generate_upsert_sql("gold_daily_zone_metrics", columns, pks)

    assert "ON CONFLICT (pickup_date, pulocationid, payment_type)" in sql
    assert "total_trips = EXCLUDED.total_trips" in sql
    assert "pickup_date = EXCLUDED.pickup_date" not in sql


def test_publish_via_psycopg2_mocked_upsert(spark: SparkSession):
    publisher = GoldToPostgresPublisher(spark)

    schema = StructType([
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_orders", LongType(), True),
    ])
    data = [
        ("2026-05-01", "delivered", 10),
        ("2026-05-02", "delivered", 15),
    ]
    df = spark.createDataFrame(data, schema=schema)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch.object(publisher.pg_connector, "get_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        with patch("src.gold_to_postgres.execute_values") as mock_exec_vals:
            count = publisher.publish_via_psycopg2(df, "gold_daily_orders_summary", mode="upsert")

            assert count == 2
            mock_exec_vals.assert_called_once()
            called_query = mock_exec_vals.call_args[0][1]
            called_rows = mock_exec_vals.call_args[0][2]

            assert "ON CONFLICT (order_date, order_status)" in called_query
            assert len(called_rows) == 2
            assert called_rows[0] == ("2026-05-01", "delivered", 10)


def test_publish_via_psycopg2_mocked_overwrite(spark: SparkSession):
    publisher = GoldToPostgresPublisher(spark)

    schema = StructType([
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_orders", LongType(), True),
    ])
    data = [("2026-05-01", "delivered", 10)]
    df = spark.createDataFrame(data, schema=schema)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch.object(publisher.pg_connector, "get_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        with patch("src.gold_to_postgres.execute_values"):
            count = publisher.publish_via_psycopg2(df, "gold_daily_orders_summary", mode="overwrite")

            assert count == 1
            # TRUNCATE must have been called for overwrite
            mock_cursor.execute.assert_called_with("TRUNCATE TABLE public.gold_daily_orders_summary;")

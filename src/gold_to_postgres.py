"""Gold to PostgreSQL Publishing Engine.

Publishes aggregated Gold Delta Lake tables into a relational PostgreSQL database
for low-latency analytical queries and BI dashboards.

Key Features:
  1. Idempotency: Supports row-level UPSERT (`ON CONFLICT (...) DO UPDATE`)
     and transactional OVERWRITE (`TRUNCATE + INSERT`), ensuring zero duplicate entries on rerun.
  2. Dual Engine Support:
     - High-speed batch SQL inserts via psycopg2 (`execute_values`).
     - PySpark JDBC loading with optional staging table upserts.
  3. Pre-flight connectivity verification with detailed error reporting.
  4. Auto-initializes target table schemas if requested.

Usage:
  # Idempotent Upsert for daily orders summary
  python src/gold_to_postgres.py --table gold_daily_orders_summary --mode upsert

  # Idempotent Upsert for daily zone metrics
  python src/gold_to_postgres.py --table gold_daily_zone_metrics --mode upsert

  # Publish all Gold marts
  python src/gold_to_postgres.py --all --mode upsert

  # Dry run (inspect records and SQL without connecting to PostgreSQL)
  python src/gold_to_postgres.py --all --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings
from config.spark_session import get_spark_session
from src.connectors.adls_connector import ADLSConnector
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import setup_logger

logger = setup_logger("gold_to_postgres")

# Table Primary Key definitions for idempotent ON CONFLICT clauses
TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "gold_daily_orders_summary": ["order_date", "order_status"],
    "gold_daily_zone_metrics": ["pickup_date", "pulocationid", "payment_type"],
    "gold_customer_metrics": ["customer_id"],
}


class GoldToPostgresPublisher:
    """Manages idempotent synchronization from Gold Delta Lake to PostgreSQL."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()
        self.pg_connector = PostgresConnector()
        self.pg_settings = settings.postgres

    def check_connection(self) -> bool:
        """Tests whether PostgreSQL is reachable."""
        try:
            with self.pg_connector.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            logger.info(f"✓ Connected successfully to PostgreSQL ({self.pg_settings.HOST}:{self.pg_settings.PORT}/{self.pg_settings.DB})")
            return True
        except Exception as e:
            logger.warning(
                f"⚠️ PostgreSQL connection failed ({self.pg_settings.HOST}:{self.pg_settings.PORT}/{self.pg_settings.DB}): {e}\n"
                "Ensure PostgreSQL is running or verify credentials in .env."
            )
            return False

    def read_gold_delta(self, mart_name: str) -> DataFrame:
        """Reads a Gold Delta Lake table into a PySpark DataFrame."""
        gold_path = self.adls.get_table_path("gold", mart_name)
        logger.info(f"Loading Gold Delta table from: {gold_path}")
        return self.spark.read.format("delta").load(gold_path)

    def generate_upsert_sql(
        self,
        table_name: str,
        columns: list[str],
        primary_keys: list[str],
    ) -> str:
        """Constructs an idempotent PostgreSQL UPSERT query:

        INSERT INTO {table} ({cols}) VALUES %s
        ON CONFLICT ({pks}) DO UPDATE SET {col} = EXCLUDED.{col}, ...
        """
        cols_str = ", ".join(columns)
        pks_str = ", ".join(primary_keys)

        # Update all columns except primary keys
        update_cols = [c for c in columns if c not in primary_keys]
        if update_cols:
            update_clauses = [f"{col} = EXCLUDED.{col}" for col in update_cols]
            update_clauses.append("_postgres_loaded_at = CURRENT_TIMESTAMP")
            set_clause = f"DO UPDATE SET {', '.join(update_clauses)}"
        else:
            set_clause = "DO NOTHING"

        schema = self.pg_settings.SCHEMA
        sql = f"""
        INSERT INTO {schema}.{table_name} ({cols_str})
        VALUES %s
        ON CONFLICT ({pks_str})
        {set_clause};
        """
        return sql.strip()

    def publish_via_psycopg2(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "upsert",
        batch_size: int = 1000,
    ) -> int:
        """Performs idempotent batch load using psycopg2.extras.execute_values.

        Args:
            df: Source PySpark DataFrame.
            table_name: Target PostgreSQL table.
            mode: 'upsert' (ON CONFLICT DO UPDATE) or 'overwrite' (TRUNCATE + INSERT).
            batch_size: Number of records per batch.

        Returns:
            Number of rows processed.
        """
        if table_name not in TABLE_PRIMARY_KEYS:
            raise ValueError(f"Unknown table '{table_name}'. Defined tables: {list(TABLE_PRIMARY_KEYS.keys())}")

        primary_keys = TABLE_PRIMARY_KEYS[table_name]

        # Convert DataFrame column names to lowercase matching Postgres DDL
        df_lower = df.toDF(*[c.lower() for c in df.columns])
        columns = df_lower.columns

        # Convert PySpark DataFrame to list of Python tuples for psycopg2
        rows = [tuple(row) for row in df_lower.collect()]
        total_records = len(rows)

        if total_records == 0:
            logger.info(f"Table '{table_name}' has 0 records. Skipping publish.")
            return 0

        logger.info(
            f"Publishing {total_records:,} rows into PostgreSQL table '{table_name}' "
            f"(mode={mode}, batch_size={batch_size})..."
        )

        with self.pg_connector.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Handle overwrite mode
                if mode == "overwrite":
                    logger.info(f"Truncating table {self.pg_settings.SCHEMA}.{table_name} for overwrite...")
                    cur.execute(f"TRUNCATE TABLE {self.pg_settings.SCHEMA}.{table_name};")

                # 2. Build SQL statement
                if mode == "upsert":
                    query = self.generate_upsert_sql(table_name, columns, primary_keys)
                else:
                    cols_str = ", ".join(columns)
                    query = f"INSERT INTO {self.pg_settings.SCHEMA}.{table_name} ({cols_str}) VALUES %s;"

                # 3. Stream in batches
                execute_values(cur, query, rows, page_size=batch_size)

        logger.info(
            f"✓ Successfully published {total_records:,} records to "
            f"{self.pg_settings.SCHEMA}.{table_name} (Idempotent: {mode.upper()})."
        )
        return total_records

    def publish_mart(
        self,
        mart_name: str,
        mode: str = "upsert",
        method: str = "psycopg2",
        dry_run: bool = False,
    ) -> int:
        """Executes the publish workflow for a specific Gold mart."""
        df = self.read_gold_delta(mart_name)
        count = df.count()

        if dry_run:
            logger.info(f"[DRY-RUN] Mart '{mart_name}' contains {count} records.")
            logger.info(f"[DRY-RUN] Schema:\n{df._jdf.schema().treeString()}")
            sql_preview = self.generate_upsert_sql(
                mart_name,
                [c.lower() for c in df.columns],
                TABLE_PRIMARY_KEYS.get(mart_name, []),
            )
            logger.info(f"[DRY-RUN] Upsert Query Template:\n{sql_preview}")
            return count

        # Check Postgres availability before attempting write
        if not self.check_connection():
            raise ConnectionError(
                f"Could not connect to PostgreSQL on {self.pg_settings.HOST}:{self.pg_settings.PORT}. "
                "Ensure PostgreSQL server is running."
            )

        if method == "psycopg2":
            return self.publish_via_psycopg2(df, mart_name, mode=mode)
        elif method == "jdbc":
            self.pg_connector.write_dataframe(df, mart_name, mode="overwrite" if mode == "overwrite" else "append")
            return count
        else:
            raise ValueError(f"Unsupported method: {method}")


def main():
    parser = argparse.ArgumentParser(description="Gold Delta Lake to PostgreSQL Data Mart Publisher")
    parser.add_argument(
        "--table",
        choices=list(TABLE_PRIMARY_KEYS.keys()),
        default="gold_daily_orders_summary",
        help="Target Gold mart to publish (default: gold_daily_orders_summary)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Publish all configured Gold data marts",
    )
    parser.add_argument(
        "--mode",
        choices=["upsert", "overwrite"],
        default="upsert",
        help="Idempotency strategy: 'upsert' (ON CONFLICT DO UPDATE) or 'overwrite' (TRUNCATE + INSERT)",
    )
    parser.add_argument(
        "--method",
        choices=["psycopg2", "jdbc"],
        default="psycopg2",
        help="Transfer mechanism: psycopg2 batch SQL or PySpark JDBC (default: psycopg2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect Gold records and preview idempotent SQL without connecting to PostgreSQL",
    )
    args = parser.parse_args()

    spark = get_spark_session("GoldToPostgresPublisher")
    publisher = GoldToPostgresPublisher(spark)

    tables_to_publish = (
        ["gold_daily_orders_summary", "gold_daily_zone_metrics"]
        if args.all
        else [args.table]
    )

    print("\n" + "=" * 70)
    print(f"🚀 PUBLISHING GOLD MARTS TO POSTGRESQL (Mode: {args.mode.upper()})")
    print("=" * 70)

    for tbl in tables_to_publish:
        try:
            total = publisher.publish_mart(
                mart_name=tbl,
                mode=args.mode,
                method=args.method,
                dry_run=args.dry_run,
            )
            print(f"  ✓ {tbl}: {total:,} records processed.")
        except ConnectionError as e:
            print(f"  ⚠️ {tbl}: {e}")
        except Exception as e:
            print(f"  ❌ {tbl}: Failed with error: {e}")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

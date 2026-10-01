"""PostgreSQL Connector for PySpark JDBC and psycopg2 queries."""
from __future__ import annotations

import contextlib
from typing import Generator
import psycopg2
from pyspark.sql import DataFrame, SparkSession
from config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class PostgresConnector:
    """Handles PySpark JDBC writes/reads and psycopg2 client connections for PostgreSQL."""

    def __init__(self):
        self.settings = settings.postgres

    @contextlib.contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Provides a managed psycopg2 database connection."""
        conn = psycopg2.connect(
            host=self.settings.HOST,
            port=self.settings.PORT,
            dbname=self.settings.DB,
            user=self.settings.USER,
            password=self.settings.PASSWORD,
            sslmode=self.settings.SSLMODE,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def write_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        truncate: bool = True,
    ) -> None:
        """Writes a Spark DataFrame directly to PostgreSQL via JDBC.

        Args:
            df: PySpark DataFrame to write.
            table_name: Destination PostgreSQL table name (e.g. 'gold_daily_sales').
            mode: Save mode ('overwrite', 'append', 'ignore', 'errorifexists').
            truncate: If True and mode='overwrite', truncates table instead of dropping DDL.
        """
        full_table = f"{self.settings.SCHEMA}.{table_name}"
        logger.info(f"Writing DataFrame to PostgreSQL table: {full_table} (mode={mode})")

        writer = (
            df.write.format("jdbc")
            .option("url", self.settings.jdbc_url)
            .option("dbtable", full_table)
            .option("user", self.settings.USER)
            .option("password", self.settings.PASSWORD)
            .option("driver", self.settings.JDBC_DRIVER)
            .mode(mode)
        )

        if mode == "overwrite" and truncate:
            writer = writer.option("truncate", "true")

        writer.save()
        logger.info(f"Successfully saved {df.count()} records to PostgreSQL table: {full_table}")

    def read_dataframe(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Reads a PostgreSQL table into a PySpark DataFrame via JDBC."""
        full_table = f"{self.settings.SCHEMA}.{table_name}"
        logger.info(f"Reading from PostgreSQL table: {full_table}")

        return (
            spark.read.format("jdbc")
            .option("url", self.settings.jdbc_url)
            .option("dbtable", full_table)
            .option("user", self.settings.USER)
            .option("password", self.settings.PASSWORD)
            .option("driver", self.settings.JDBC_DRIVER)
            .load()
        )

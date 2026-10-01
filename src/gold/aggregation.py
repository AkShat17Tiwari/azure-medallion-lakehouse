"""Gold Layer: Curated business aggregations, star schema metrics, and PostgreSQL export.

Responsibilities:
  - Read cleansed data from Silver Delta Lake tables
  - Calculate business KPIs, dimension aggregates, and analytical views
  - Save business metrics into Gold Delta Lake tables for lakehouse analytics
  - Export final analytical tables into PostgreSQL for BI, dashboards, and APIs
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    countDistinct,
    current_timestamp,
    max,
    round,
    sum,
    to_date,
    when,
)
from src.connectors.adls_connector import ADLSConnector
from src.connectors.postgres_connector import PostgresConnector
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class GoldAggregation:
    """Computes Gold business layer metrics and synchronizes with PostgreSQL."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()
        self.postgres = PostgresConnector()

    def build_daily_sales_summary(self, df_silver: DataFrame) -> DataFrame:
        """Aggregates sales metrics at the daily grain."""
        return (
            df_silver.withColumn("order_day", to_date("order_timestamp"))
            .groupBy("order_day")
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                round(
                    sum(when(df_silver["status"] == "completed", df_silver["amount"]).otherwise(0.0)),
                    2,
                ).alias("total_completed_revenue"),
                sum(when(df_silver["status"] == "completed", 1).otherwise(0)).alias("completed_orders"),
                sum(when(df_silver["status"] == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
            )
            .withColumn("_gold_calculated_at", current_timestamp())
            .orderBy("order_day")
        )

    def build_customer_summary(self, df_silver: DataFrame) -> DataFrame:
        """Aggregates customer lifetime value and purchase metrics."""
        return (
            df_silver.groupBy("customer_id")
            .agg(
                count("order_id").alias("total_orders"),
                round(
                    sum(when(df_silver["status"] == "completed", df_silver["amount"]).otherwise(0.0)),
                    2,
                ).alias("total_lifetime_spend"),
                round(
                    avg(when(df_silver["status"] == "completed", df_silver["amount"]).otherwise(None)),
                    2,
                ).alias("avg_order_value"),
                max("order_timestamp").alias("latest_order_timestamp"),
            )
            .withColumn("_gold_calculated_at", current_timestamp())
            .orderBy("total_lifetime_spend", ascending=False)
        )

    def save_gold_table(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> str:
        """Saves aggregated DataFrame as a Gold Delta Lake table."""
        target_path = self.adls.get_table_path("gold", table_name)
        logger.info(f"[Gold] Saving Gold Delta table: {target_path}")

        (
            df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(target_path)
        )
        return target_path

    def process_gold_pipeline(
        self,
        silver_table_name: str = "cleaned_orders",
        export_to_postgres: bool = True,
    ) -> dict[str, str]:
        """Runs the Gold aggregation pipeline and exports to PostgreSQL."""
        silver_path = self.adls.get_table_path("silver", silver_table_name)
        logger.info(f"[Gold] Loading Silver data from: {silver_path}")
        df_silver = self.spark.read.format("delta").load(silver_path)

        # 1. Compute aggregates
        daily_sales_df = self.build_daily_sales_summary(df_silver)
        customer_summary_df = self.build_customer_summary(df_silver)

        # 2. Persist to Gold Delta Lake
        path_daily = self.save_gold_table(daily_sales_df, "gold_daily_sales")
        path_customer = self.save_gold_table(customer_summary_df, "gold_customer_metrics")

        # 3. Export to PostgreSQL serving layer
        if export_to_postgres:
            try:
                logger.info("[Gold] Exporting gold_daily_sales to PostgreSQL...")
                self.postgres.write_dataframe(daily_sales_df, "gold_daily_sales", mode="overwrite")

                logger.info("[Gold] Exporting gold_customer_metrics to PostgreSQL...")
                self.postgres.write_dataframe(customer_summary_df, "gold_customer_metrics", mode="overwrite")
            except Exception as e:
                logger.warning(
                    f"[Gold] Could not export to PostgreSQL (is Postgres reachable?): {e}. "
                    "Delta Gold tables were still successfully written."
                )

        return {
            "gold_daily_sales": path_daily,
            "gold_customer_metrics": path_customer,
        }

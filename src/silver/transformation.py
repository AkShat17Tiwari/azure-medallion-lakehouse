"""Silver Layer: Cleansed, standardized, and validated Delta Lake tables.

Responsibilities:
  - Read from Bronze Delta Lake tables
  - Schema enforcement, null filtering, data type casting
  - Deduplication and data enrichment
  - Idempotent Upserts using Delta Lake MERGE statement
  - Maintain cleaned tables in Silver storage layer
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    row_number,
    to_timestamp,
    trim,
)
from pyspark.sql.window import Window
from src.connectors.adls_connector import ADLSConnector
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class SilverTransformation:
    """Transforms raw bronze data into cleaned, standardized silver Delta tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()

    def clean_orders(self, df_bronze: DataFrame) -> DataFrame:
        """Cleanses raw order records:
          - Filters out invalid/null primary keys
          - Standardizes status codes to lowercase
          - Enforces timestamp casting
          - Deduplicates by order_id keeping latest updated_at
        """
        # Filter nulls & cast columns
        df_filtered = (
            df_bronze.filter(col("order_id").isNotNull())
            .withColumn("order_id", col("order_id").cast("string"))
            .withColumn("customer_id", col("customer_id").cast("string"))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("status", lower(trim(col("status"))))
            .withColumn("order_timestamp", to_timestamp(col("order_date")))
        )

        # Deduplication using Window function (latest updated_at or ingested_at)
        window_spec = Window.partitionBy("order_id").orderBy(
            col("_ingested_at").desc_nulls_last()
        )

        df_deduped = (
            df_filtered.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .withColumn("_silver_processed_at", current_timestamp())
        )

        return df_deduped

    def upsert_to_silver(
        self,
        df_clean: DataFrame,
        target_table_name: str,
        primary_key: str = "order_id",
    ) -> str:
        """Upserts cleaned DataFrame into the Silver Delta table using Delta MERGE."""
        target_path = self.adls.get_table_path("silver", target_table_name)
        logger.info(f"[Silver] Upserting cleaned data into: {target_path}")

        if DeltaTable.isDeltaTable(self.spark, target_path):
            logger.info(f"[Silver] Existing Delta table found at {target_path}. Performing MERGE.")
            silver_table = DeltaTable.forPath(self.spark, target_path)

            (
                silver_table.alias("target")
                .merge(
                    source=df_clean.alias("source"),
                    condition=f"target.{primary_key} = source.{primary_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info(f"[Silver] Initializing new Delta table at {target_path}.")
            (
                df_clean.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )

        count = self.spark.read.format("delta").load(target_path).count()
        logger.info(f"[Silver] Completed processing. Silver table '{target_table_name}' contains {count} records.")
        return target_path

    def process_orders_pipeline(
        self,
        bronze_table_name: str = "raw_orders",
        silver_table_name: str = "cleaned_orders",
    ) -> str:
        """Reads from Bronze Delta table, executes cleaning transformations, and upserts to Silver."""
        bronze_path = self.adls.get_table_path("bronze", bronze_table_name)
        logger.info(f"[Silver] Reading Bronze Delta table from {bronze_path}")

        df_bronze = self.spark.read.format("delta").load(bronze_path)
        df_clean = self.clean_orders(df_bronze)
        return self.upsert_to_silver(df_clean, silver_table_name)

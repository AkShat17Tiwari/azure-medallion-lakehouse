"""Bronze Layer: Raw data ingestion into Delta Lake format.

Responsibilities:
  - Ingest raw source files (JSON, CSV, Parquet) with schema-on-read
  - Retain raw data faithfully without lossy transformations
  - Append ingestion audit metadata (_ingested_at, _source_file, _batch_id)
  - Persist into Delta Lake format in Bronze container / directory
"""
from __future__ import annotations

import uuid
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from src.connectors.adls_connector import ADLSConnector
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class BronzeIngestion:
    """Handles raw data ingestion into Bronze Delta Lake tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.adls = ADLSConnector()

    def ingest_json_source(
        self,
        source_path: str,
        table_name: str,
        mode: str = "append",
    ) -> str:
        """Reads raw JSON data, appends audit columns, and saves to Bronze Delta Lake.

        Args:
            source_path: Local or cloud path to the incoming JSON files.
            table_name: Name of the Bronze Delta table (e.g. 'raw_orders').
            mode: PySpark write mode ('append' or 'overwrite').

        Returns:
            The destination storage path for the Bronze Delta table.
        """
        logger.info(f"[Bronze] Starting ingestion from {source_path} into table '{table_name}'")

        # Read raw JSON
        df_raw: DataFrame = self.spark.read.option("multiline", "true").json(source_path)

        # Attach metadata audit attributes
        batch_id = str(uuid.uuid4())
        df_bronze = (
            df_raw.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_batch_id", lit(batch_id))
        )

        target_path = self.adls.get_table_path("bronze", table_name)
        logger.info(f"[Bronze] Saving Delta table to: {target_path}")

        (
            df_bronze.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target_path)
        )

        count = self.spark.read.format("delta").load(target_path).count()
        logger.info(f"[Bronze] Completed ingestion. Table '{table_name}' now contains {count} records.")
        return target_path

"""Main Lakehouse Medallion Pipeline Orchestrator.

CLI Entrypoint to execute:
  1. Bronze ingestion (Raw landing -> Bronze Delta Lake)
  2. Silver transformation (Bronze Delta Lake -> Cleansed Silver Delta Lake with Upserts)
  3. Gold aggregation (Silver Delta Lake -> Aggregated Gold Delta Lake -> PostgreSQL)

Usage:
  python -m src.pipelines.run_pipeline --stage all
  python -m src.pipelines.run_pipeline --stage bronze --source-path ./data/raw/sample_orders.json
  python -m src.pipelines.run_pipeline --stage silver
  python -m src.pipelines.run_pipeline --stage gold
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from config.spark_session import get_spark_session
from src.bronze.ingestion import BronzeIngestion
from src.silver.transformation import SilverTransformation
from src.gold.aggregation import GoldAggregation
from src.connectors.adls_connector import ADLSConnector
from src.utils.logger import setup_logger

logger = setup_logger("lakehouse_orchestrator")


def run_pipeline(stage: str = "all", source_path: str | None = None) -> None:
    """Executes Medallion Architecture pipeline stages."""
    logger.info(f"Starting Medallion Architecture Pipeline [Stage={stage.upper()}]")

    # Ensure directories exist if using local emulation
    adls = ADLSConnector()
    adls.ensure_local_directories()

    spark = get_spark_session()

    try:
        if stage in ("bronze", "all"):
            # Resolve source data path
            if not source_path:
                default_data = Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "sample_orders.json"
                source_path = str(default_data)

            logger.info("================== STAGE 1: BRONZE INGESTION ==================")
            bronze_svc = BronzeIngestion(spark)
            bronze_svc.ingest_json_source(source_path=source_path, table_name="raw_orders")

        if stage in ("silver", "all"):
            logger.info("================== STAGE 2: SILVER TRANSFORMATION ==================")
            silver_svc = SilverTransformation(spark)
            silver_svc.process_orders_pipeline(
                bronze_table_name="raw_orders",
                silver_table_name="cleaned_orders",
            )

        if stage in ("gold", "all"):
            logger.info("================== STAGE 3: GOLD AGGREGATION & EXPORT ==================")
            gold_svc = GoldAggregation(spark)
            gold_svc.process_gold_pipeline(
                silver_table_name="cleaned_orders",
                export_to_postgres=True,
            )

        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Medallion Lakehouse Pipeline Runner")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline stage to execute (default: all)",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        default=None,
        help="Path to raw source file/directory for bronze ingestion",
    )
    args = parser.parse_args()
    run_pipeline(stage=args.stage, source_path=args.source_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""End-to-End Lakehouse Medallion Pipeline Orchestrator.

Orchestrates the entire data lifecycle sequentially:
  [Raw Data] -> Bronze Ingestion -> Silver Hygiene & Merge -> Gold Aggregation & Z-Order -> PostgreSQL Serving Layer

Usage:
  # Run full pipeline with sample E-Commerce data
  python main.py --dataset ecommerce --records 1000

  # Run full pipeline for NYC Taxi dataset
  python main.py --dataset nyctaxi --records 2000

  # Run complete suite for all datasets
  python main.py --dataset all --records 1000

  # Run pipeline skipping PostgreSQL connection
  python main.py --dataset all --skip-postgres

  # Run with dry-run PostgreSQL publication
  python main.py --dataset all --dry-run-postgres
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from config.spark_session import get_spark_session
from src.bronze_to_silver import run_bronze_to_silver
from src.connectors.adls_connector import ADLSConnector
from src.gold_to_postgres import GoldToPostgresPublisher
from src.ingest_bronze import ADLSBronzeUploader, DatasetIngestor
from src.silver_to_gold import run_silver_to_gold
from src.utils.logger import setup_logger

logger = setup_logger("pipeline_runner")


def format_duration(seconds: float) -> str:
    """Formats duration in seconds to human-readable string."""
    return f"{seconds:.2f}s"


def run_pipeline(
    dataset: str = "all",
    records: int = 1000,
    skip_postgres: bool = False,
    dry_run_postgres: bool = False,
    pg_mode: str = "upsert",
) -> None:
    """Executes the full end-to-end Medallion pipeline."""
    start_total = time.time()
    logger.info(f"Starting Medallion pipeline run [dataset={dataset}, records={records}, pg_mode={pg_mode}]")

    # Metrics collection dictionary
    metrics: dict[str, dict[str, str]] = {
        "bronze": {},
        "silver": {},
        "gold": {},
        "postgres": {},
    }

    # Ensure local emulation storage paths exist if local mode enabled
    adls = ADLSConnector()
    adls.ensure_local_directories()

    # --------------------------------------------------------------------------
    # STAGE 1: BRONZE RAW DATA INGESTION
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🥉 STAGE 1: BRONZE RAW INGESTION & ADLS GEN2 UPLOAD")
    print("=" * 80)
    t0 = time.time()

    staging_dir = Path(settings.LOCAL_DATA_DIR) / "raw" / "landing"
    ingestor = DatasetIngestor(staging_dir)
    uploader = ADLSBronzeUploader()

    datasets_to_ingest = ["ecommerce", "nyctaxi"] if dataset == "all" else [dataset]

    for ds in datasets_to_ingest:
        logger.info(f"[Bronze] Generating & acquiring raw dataset: {ds} ({records} records)...")
        if ds == "ecommerce":
            raw_files = ingestor.generate_ecommerce_dataset(num_orders=records)
            uploaded_paths = uploader.upload_batch(raw_files, remote_folder="raw/ecommerce")
            metrics["bronze"]["ecommerce_files"] = f"{len(uploaded_paths)} files"
        elif ds == "nyctaxi":
            raw_files = ingestor.generate_nyctaxi_dataset(num_records=records)
            uploaded_paths = uploader.upload_batch(raw_files, remote_folder="raw/nyctaxi")
            metrics["bronze"]["nyctaxi_files"] = f"{len(uploaded_paths)} files"

    dur_bronze = time.time() - t0
    logger.info(f"✓ Stage 1 (Bronze) completed in {format_duration(dur_bronze)}")

    # --------------------------------------------------------------------------
    # STAGE 2: SILVER HYGIENE, DEDUPLICATION & PARTITIONED MERGE
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🥈 STAGE 2: SILVER DATA HYGIENE, DEDUPLICATION & DELTA MERGE")
    print("=" * 80)
    t0 = time.time()

    for ds in datasets_to_ingest:
        if ds == "ecommerce":
            logger.info("[Silver] Processing Bronze -> Silver for E-Commerce orders...")
            silver_path = run_bronze_to_silver(dataset="ecommerce", table="orders")
            metrics["silver"]["orders_delta"] = silver_path
        elif ds == "nyctaxi":
            logger.info("[Silver] Processing Bronze -> Silver for NYC Taxi trips...")
            silver_path = run_bronze_to_silver(dataset="nyctaxi")
            metrics["silver"]["taxi_delta"] = silver_path

    dur_silver = time.time() - t0
    logger.info(f"✓ Stage 2 (Silver) completed in {format_duration(dur_silver)}")

    # --------------------------------------------------------------------------
    # STAGE 3: GOLD ANALYTICAL MARTS & MULTIDIMENSIONAL Z-ORDERING
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🥇 STAGE 3: GOLD MULTI-DIMENSIONAL AGGREGATIONS & Z-ORDERING")
    print("=" * 80)
    t0 = time.time()

    gold_results = run_silver_to_gold(dataset=dataset)
    for mart_name, mart_path in gold_results.items():
        metrics["gold"][mart_name] = mart_path

    dur_gold = time.time() - t0
    logger.info(f"✓ Stage 3 (Gold) completed in {format_duration(dur_gold)}")

    # --------------------------------------------------------------------------
    # STAGE 4: POSTGRESQL IDEMPOTENT PUBLISHING
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🐘 STAGE 4: POSTGRESQL SERVING LAYER PUBLICATION")
    print("=" * 80)
    t0 = time.time()

    if skip_postgres:
        logger.info("[PostgreSQL] Skipping PostgreSQL publication as requested (--skip-postgres).")
        metrics["postgres"]["status"] = "Skipped"
    else:
        spark = get_spark_session("Main-PostgresPublisher")
        publisher = GoldToPostgresPublisher(spark)

        tables_to_publish = []
        if dataset in ("ecommerce", "all"):
            tables_to_publish.append("gold_daily_orders_summary")
        if dataset in ("nyctaxi", "all"):
            tables_to_publish.append("gold_daily_zone_metrics")

        for tbl in tables_to_publish:
            try:
                count = publisher.publish_mart(
                    mart_name=tbl,
                    mode=pg_mode,
                    method="psycopg2",
                    dry_run=dry_run_postgres,
                )
                metrics["postgres"][tbl] = f"{count:,} records ({'Dry-Run' if dry_run_postgres else 'Published'})"
            except ConnectionError as e:
                logger.warning(f"Could not connect to PostgreSQL: {e}. Delta Gold tables remain intact.")
                metrics["postgres"][tbl] = "Connection Offline (Delta Only)"
            except Exception as e:
                logger.error(f"Failed publishing {tbl}: {e}")
                metrics["postgres"][tbl] = f"Error: {e}"

    dur_pg = time.time() - t0
    logger.info(f"✓ Stage 4 (PostgreSQL) completed in {format_duration(dur_pg)}")

    # --------------------------------------------------------------------------
    # PIPELINE SUMMARY & AUDIT RECONCILIATION
    # --------------------------------------------------------------------------
    dur_total = time.time() - start_total
    print("\n" + "=" * 70)
    print("MEDALLION PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Total Duration: {format_duration(dur_total)}")
    print("-" * 70)
    print(f"Bronze Ingestion:     {format_duration(dur_bronze)}")
    for k, v in metrics["bronze"].items():
        print(f"  - {k}: {v}")
    print(f"Silver Hygiene:       {format_duration(dur_silver)}")
    for k, v in metrics["silver"].items():
        print(f"  - {k}: {v}")
    print(f"Gold Marts & Z-Order: {format_duration(dur_gold)}")
    for k, v in metrics["gold"].items():
        print(f"  - {k}: {v}")
    print(f"PostgreSQL Serving:   {format_duration(dur_pg)}")
    for k, v in metrics["postgres"].items():
        print(f"  - {k}: {v}")
    print("=" * 70)
    logger.info("Pipeline execution completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="End-to-End Lakehouse Medallion Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset",
        choices=["ecommerce", "nyctaxi", "all"],
        default="all",
        help="Dataset pipeline to execute: 'ecommerce', 'nyctaxi', or 'all' (default: all)",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=500,
        help="Number of records to generate/ingest for sample datasets (default: 500)",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip PostgreSQL publish stage (Delta Lake tables only)",
    )
    parser.add_argument(
        "--dry-run-postgres",
        action="store_true",
        help="Inspect Gold records and preview SQL upserts without connecting to PostgreSQL",
    )
    parser.add_argument(
        "--pg-mode",
        choices=["upsert", "overwrite"],
        default="upsert",
        help="PostgreSQL idempotency mode: 'upsert' or 'overwrite' (default: upsert)",
    )

    args = parser.parse_args()

    run_pipeline(
        dataset=args.dataset,
        records=args.records,
        skip_postgres=args.skip_postgres,
        dry_run_postgres=args.dry_run_postgres,
        pg_mode=args.pg_mode,
    )


if __name__ == "__main__":
    main()

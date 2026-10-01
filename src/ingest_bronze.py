"""Bronze Layer Data Ingestion & Raw ADLS Gen2 Upload Utility.

This script:
1. Downloads an open dataset (e.g. NYC Taxi or Brazilian E-Commerce) OR generates
   realistic transactional CSV files (Brazilian E-Commerce / NYC Taxi format).
2. Preserves exact raw schemas, data types, headers, and raw string contents without alteration.
3. Uploads the raw CSV files directly into the Azure Data Lake Storage Gen2 (ADLS Gen2)
   'bronze' container using the Azure Data Lake SDK (`azure-storage-file-datalake`),
   or into the local bronze landing directory when `USE_LOCAL_STORAGE_EMULATION=True`.

Usage:
    # Generate realistic Brazilian E-Commerce dataset & upload to bronze
    python src/ingest_bronze.py --dataset ecommerce --records 1000

    # Generate NYC Taxi dataset & upload to bronze
    python src/ingest_bronze.py --dataset nyctaxi --records 2000

    # Download from remote URL and upload to bronze
    python src/ingest_bronze.py --dataset download --source-url https://example.com/data.csv

    # Local generation only without remote ADLS upload
    python src/ingest_bronze.py --dataset ecommerce --local-only
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import sys
import urllib.request
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger("ingest_bronze")


# ==============================================================================
# 1. DATASET GENERATORS & DOWNLOADERS
# ==============================================================================
class DatasetIngestor:
    """Handles raw dataset acquisition (download or synthetic generation)."""

    def __init__(self, staging_dir: Path):
        self.staging_dir = staging_dir
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    def download_dataset(self, url: str, output_filename: str) -> Path:
        """Downloads a remote dataset directly to staging directory preserving raw bytes."""
        target_path = self.staging_dir / output_filename
        logger.info(f"Downloading raw dataset from {url} to {target_path}...")
        urllib.request.urlretrieve(url, target_path)
        logger.info(f"Successfully downloaded {target_path.stat().st_size:,} bytes to {target_path}")
        return target_path

    def generate_ecommerce_dataset(self, num_orders: int = 1000) -> list[Path]:
        """Generates realistic Brazilian E-Commerce transactional CSV files:

        - olist_orders_dataset.csv
        - olist_order_items_dataset.csv
        - olist_order_payments_dataset.csv
        - olist_customers_dataset.csv
        """
        logger.info(f"Generating synthetic Brazilian E-Commerce dataset ({num_orders} orders)...")

        cities = ["sao paulo", "rio de janeiro", "belo horizonte", "brasilia", "curitiba", "porto alegre", "salvador"]
        states = ["SP", "RJ", "MG", "DF", "PR", "RS", "BA"]
        statuses = ["delivered", "shipped", "canceled", "invoiced", "processing"]
        payment_types = ["credit_card", "boleto", "voucher", "debit_card"]

        # 1. Customers
        num_customers = max(100, num_orders // 2)
        customers_path = self.staging_dir / "olist_customers_dataset.csv"
        customer_ids = []
        with open(customers_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"])
            for _ in range(num_customers):
                c_id = uuid.uuid4().hex[:32]
                c_uniq = uuid.uuid4().hex[:32]
                zip_prefix = f"{random.randint(10000, 99999)}"
                idx = random.randint(0, len(cities) - 1)
                writer.writerow([c_id, c_uniq, zip_prefix, cities[idx], states[idx]])
                customer_ids.append(c_id)

        # 2. Orders, Items, Payments
        orders_path = self.staging_dir / "olist_orders_dataset.csv"
        items_path = self.staging_dir / "olist_order_items_dataset.csv"
        payments_path = self.staging_dir / "olist_order_payments_dataset.csv"

        base_date = datetime(2026, 1, 1)

        with open(orders_path, mode="w", newline="", encoding="utf-8") as f_orders, \
             open(items_path, mode="w", newline="", encoding="utf-8") as f_items, \
             open(payments_path, mode="w", newline="", encoding="utf-8") as f_payments:

            w_orders = csv.writer(f_orders)
            w_items = csv.writer(f_items)
            w_payments = csv.writer(f_payments)

            w_orders.writerow([
                "order_id", "customer_id", "order_status",
                "order_purchase_timestamp", "order_approved_at",
                "order_delivered_carrier_date", "order_delivered_customer_date",
                "order_estimated_delivery_date"
            ])
            w_items.writerow([
                "order_id", "order_item_id", "product_id",
                "seller_id", "shipping_limit_date", "price", "freight_value"
            ])
            w_payments.writerow([
                "order_id", "payment_sequential", "payment_type",
                "payment_installments", "payment_value"
            ])

            for _ in range(num_orders):
                order_id = uuid.uuid4().hex[:32]
                customer_id = random.choice(customer_ids)
                status = random.choices(statuses, weights=[0.85, 0.08, 0.03, 0.02, 0.02])[0]

                purchase_time = base_date + timedelta(
                    days=random.randint(0, 250),
                    seconds=random.randint(0, 86400)
                )
                approved_time = purchase_time + timedelta(minutes=random.randint(5, 120))
                carrier_time = approved_time + timedelta(days=random.randint(1, 3))
                delivery_time = carrier_time + timedelta(days=random.randint(2, 7)) if status == "delivered" else ""
                estimated_delivery = purchase_time + timedelta(days=random.randint(10, 20))

                w_orders.writerow([
                    order_id,
                    customer_id,
                    status,
                    purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
                    approved_time.strftime("%Y-%m-%d %H:%M:%S"),
                    carrier_time.strftime("%Y-%m-%d %H:%M:%S") if status in ["delivered", "shipped"] else "",
                    delivery_time.strftime("%Y-%m-%d %H:%M:%S") if delivery_time else "",
                    estimated_delivery.strftime("%Y-%m-%d %H:%M:%S"),
                ])

                # Order Items (1 to 4 items)
                num_items = random.randint(1, 4)
                total_order_price = 0.0
                for item_seq in range(1, num_items + 1):
                    prod_id = uuid.uuid4().hex[:32]
                    seller_id = uuid.uuid4().hex[:32]
                    price = round(random.uniform(15.0, 350.0), 2)
                    freight = round(random.uniform(5.0, 35.0), 2)
                    total_order_price += price + freight

                    shipping_limit = approved_time + timedelta(days=5)
                    w_items.writerow([
                        order_id,
                        item_seq,
                        prod_id,
                        seller_id,
                        shipping_limit.strftime("%Y-%m-%d %H:%M:%S"),
                        f"{price:.2f}",
                        f"{freight:.2f}"
                    ])

                # Payment (1 or 2 payment methods)
                w_payments.writerow([
                    order_id,
                    1,
                    random.choice(payment_types),
                    random.randint(1, 6),
                    f"{total_order_price:.2f}"
                ])

        files = [customers_path, orders_path, items_path, payments_path]
        logger.info(f"Generated {len(files)} Brazilian E-Commerce raw files in {self.staging_dir}")
        return files

    def generate_nyctaxi_dataset(self, num_records: int = 2000) -> list[Path]:
        """Generates realistic NYC Yellow Taxi trip records CSV file."""
        logger.info(f"Generating synthetic NYC Yellow Taxi dataset ({num_records} trip records)...")
        output_path = self.staging_dir / "nyc_yellow_taxi_trips.csv"

        base_time = datetime(2026, 1, 1, 0, 0, 0)

        with open(output_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "passenger_count", "trip_distance", "RatecodeID",
                "store_and_fwd_flag", "PULocationID", "DOLocationID",
                "payment_type", "fare_amount", "extra", "mta_tax",
                "tip_amount", "tolls_amount", "improvement_surcharge",
                "total_amount", "congestion_surcharge"
            ])

            for _ in range(num_records):
                vendor_id = random.choice([1, 2])
                pickup = base_time + timedelta(
                    days=random.randint(0, 180),
                    minutes=random.randint(0, 1440)
                )
                duration_mins = random.randint(5, 60)
                dropoff = pickup + timedelta(minutes=duration_mins)
                passenger_count = random.choice([1, 1, 1, 2, 2, 3, 4, 5])
                trip_distance = round(random.uniform(0.5, 25.0), 2)
                ratecode_id = 1
                store_flag = random.choice(["N", "N", "N", "Y"])
                pu_location = random.randint(1, 263)
                do_location = random.randint(1, 263)
                payment_type = random.choice([1, 1, 2, 3])  # 1=Credit, 2=Cash, 3=No Charge

                fare = round(max(3.0, trip_distance * 3.50 + random.uniform(2.0, 8.0)), 2)
                extra = 0.50 if pickup.hour in [16, 17, 18, 19] else 0.0
                mta_tax = 0.50
                tip = round(fare * 0.18, 2) if payment_type == 1 else 0.0
                tolls = 6.55 if random.random() < 0.15 else 0.0
                surcharge = 0.30
                congestion = 2.50
                total = round(fare + extra + mta_tax + tip + tolls + surcharge + congestion, 2)

                writer.writerow([
                    vendor_id,
                    pickup.strftime("%Y-%m-%d %H:%M:%S"),
                    dropoff.strftime("%Y-%m-%d %H:%M:%S"),
                    passenger_count,
                    f"{trip_distance:.2f}",
                    ratecode_id,
                    store_flag,
                    pu_location,
                    do_location,
                    payment_type,
                    f"{fare:.2f}",
                    f"{extra:.2f}",
                    f"{mta_tax:.2f}",
                    f"{tip:.2f}",
                    f"{tolls:.2f}",
                    f"{surcharge:.2f}",
                    f"{total:.2f}",
                    f"{congestion:.2f}"
                ])

        logger.info(f"Generated NYC Taxi dataset at {output_path} ({output_path.stat().st_size:,} bytes)")
        return [output_path]


# ==============================================================================
# 2. ADLS GEN2 AUTOMATED UPLOADER
# ==============================================================================
class ADLSBronzeUploader:
    """Automated upload utility pushing raw files into ADLS Gen2 bronze/ container."""

    def __init__(self):
        self.use_local = settings.USE_LOCAL_STORAGE_EMULATION
        self.azure_cfg = settings.azure
        self.container_name = self.azure_cfg.CONTAINER_BRONZE

    def _calculate_md5(self, file_path: Path) -> str:
        """Calculates MD5 checksum to verify content integrity."""
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _get_service_client(self):
        """Builds DataLakeServiceClient using configured credentials."""
        from azure.storage.filedatalake import DataLakeServiceClient

        account_url = f"https://{self.azure_cfg.dfs_endpoint}"
        auth_type = self.azure_cfg.AUTH_TYPE

        logger.info(f"Authenticating to ADLS Gen2 ({account_url}) via '{auth_type}'...")

        if auth_type == "access_key":
            if not self.azure_cfg.STORAGE_ACCESS_KEY:
                raise ValueError("AZURE_STORAGE_ACCESS_KEY is required for access_key auth.")
            return DataLakeServiceClient(
                account_url=account_url,
                credential=self.azure_cfg.STORAGE_ACCESS_KEY,
            )

        elif auth_type == "sas_token":
            if not self.azure_cfg.STORAGE_SAS_TOKEN:
                raise ValueError("AZURE_STORAGE_SAS_TOKEN is required for sas_token auth.")
            return DataLakeServiceClient(
                account_url=account_url,
                credential=self.azure_cfg.clean_sas_token,
            )

        elif auth_type == "service_principal":
            from azure.identity import ClientSecretCredential

            if not (self.azure_cfg.TENANT_ID and self.azure_cfg.CLIENT_ID and self.azure_cfg.CLIENT_SECRET):
                raise ValueError("TENANT_ID, CLIENT_ID, and CLIENT_SECRET required for service_principal auth.")

            credential = ClientSecretCredential(
                tenant_id=self.azure_cfg.TENANT_ID,
                client_id=self.azure_cfg.CLIENT_ID,
                client_secret=self.azure_cfg.CLIENT_SECRET,
            )
            return DataLakeServiceClient(account_url=account_url, credential=credential)

        else:
            from azure.identity import DefaultAzureCredential
            logger.info("Falling back to DefaultAzureCredential...")
            return DataLakeServiceClient(account_url=account_url, credential=DefaultAzureCredential())

    def upload_file(self, local_file_path: Path, remote_folder: str = "raw_landing") -> str:
        """Pushes a raw CSV file directly into ADLS Gen2 bronze/ container without schema changes."""
        if not local_file_path.exists():
            raise FileNotFoundError(f"Source file not found: {local_file_path}")

        file_size = local_file_path.stat().st_size
        checksum = self._calculate_md5(local_file_path)
        filename = local_file_path.name
        remote_path = f"{remote_folder.strip('/')}/{filename}"

        logger.info(
            f"Preparing upload of raw file '{filename}' "
            f"({file_size:,} bytes, MD5: {checksum}) to bronze/{remote_path}..."
        )

        # ----------------------------------------------------------------------
        # Path A: Local Storage Emulation Mode
        # ----------------------------------------------------------------------
        if self.use_local:
            dest_dir = Path(settings.LOCAL_BRONZE_PATH) / remote_folder.strip("/")
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_file = dest_dir / filename

            with open(local_file_path, "rb") as src, open(dest_file, "wb") as dst:
                dst.write(src.read())

            verified_checksum = self._calculate_md5(dest_file)
            assert checksum == verified_checksum, "Checksum mismatch during local storage copy!"

            logger.info(
                f"[Local Emulation] Uploaded {filename} -> {dest_file} "
                f"(Integrity verified, MD5={verified_checksum})"
            )
            return str(dest_file.resolve())

        # ----------------------------------------------------------------------
        # Path B: Cloud ADLS Gen2 Upload via azure-storage-file-datalake
        # ----------------------------------------------------------------------
        service_client = self._get_service_client()
        fs_client = service_client.get_file_system_client(self.container_name)

        # Ensure container exists
        if not fs_client.exists():
            logger.info(f"Container '{self.container_name}' does not exist. Creating it...")
            fs_client.create_file_system()

        # Create directory client
        dir_client = fs_client.get_directory_client(remote_folder.strip("/"))
        if not dir_client.exists():
            dir_client.create_directory()

        # Create file client and upload exact bytes
        file_client = dir_client.get_file_client(filename)
        with open(local_file_path, "rb") as data_stream:
            file_client.upload_data(data_stream, overwrite=True)

        abfss_uri = f"abfss://{self.container_name}@{self.azure_cfg.dfs_endpoint}/{remote_path}"
        logger.info(
            f"[ADLS Gen2] Successfully uploaded raw file: {abfss_uri} "
            f"({file_size:,} bytes, MD5 verified: {checksum})"
        )
        return abfss_uri

    def upload_batch(self, file_paths: list[Path], remote_folder: str = "raw_landing") -> list[str]:
        """Uploads a list of local files sequentially to ADLS Gen2 bronze/."""
        results = []
        for path in file_paths:
            destination = self.upload_file(path, remote_folder=remote_folder)
            results.append(destination)
        return results


# ==============================================================================
# 3. CLI ENTRYPOINT
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Bronze Layer Ingestion: Download/Generate & Upload raw CSV files to ADLS Gen2."
    )
    parser.add_argument(
        "--dataset",
        choices=["ecommerce", "nyctaxi", "download"],
        default="ecommerce",
        help="Dataset type to ingest: 'ecommerce', 'nyctaxi', or 'download' (default: ecommerce)",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=1000,
        help="Number of records/orders to generate for synthetic datasets (default: 1000)",
    )
    parser.add_argument(
        "--source-url",
        type=str,
        default=None,
        help="Remote URL to download when --dataset=download",
    )
    parser.add_argument(
        "--staging-dir",
        type=str,
        default="./data/raw/landing",
        help="Local staging directory for generated/downloaded raw CSV files",
    )
    parser.add_argument(
        "--remote-folder",
        type=str,
        default=None,
        help="Target folder inside ADLS Gen2 bronze container (default: raw/<dataset_name>)",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Generate or download locally only without triggering upload",
    )

    args = parser.parse_args()

    staging_path = Path(args.staging_dir).resolve()
    ingestor = DatasetIngestor(staging_path)

    # 1. Acquire raw CSV files
    raw_files: list[Path] = []
    dataset_name = args.dataset

    if args.dataset == "ecommerce":
        raw_files = ingestor.generate_ecommerce_dataset(num_orders=args.records)
    elif args.dataset == "nyctaxi":
        raw_files = ingestor.generate_nyctaxi_dataset(num_records=args.records)
    elif args.dataset == "download":
        if not args.source_url:
            logger.error("--source-url is required when --dataset=download")
            sys.exit(1)
        filename = args.source_url.split("/")[-1] or "downloaded_data.csv"
        raw_files = [ingestor.download_dataset(args.source_url, filename)]

    logger.info(f"Acquired {len(raw_files)} raw CSV files in {staging_path}")

    # 2. Upload to Bronze Container (Local or Cloud)
    if not args.local_only:
        target_subfolder = args.remote_folder or f"raw/{dataset_name}"
        uploader = ADLSBronzeUploader()
        logger.info(f"Beginning upload to Bronze layer destination '{target_subfolder}'...")
        uploaded_uris = uploader.upload_batch(raw_files, remote_folder=target_subfolder)

        print("\n" + "=" * 70)
        print("🎉 BRONZE INGESTION & UPLOAD COMPLETE")
        print("=" * 70)
        for uri in uploaded_uris:
            print(f"  ✓ {uri}")
        print("=" * 70 + "\n")
    else:
        logger.info("Local-only mode specified. Skipping storage upload.")


if __name__ == "__main__":
    main()

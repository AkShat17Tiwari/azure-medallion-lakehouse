"""Unit tests for Bronze dataset ingestion and upload utility."""
from pathlib import Path
from src.ingest_bronze import DatasetIngestor, ADLSBronzeUploader


def test_ecommerce_dataset_generator(tmp_path: Path):
    ingestor = DatasetIngestor(tmp_path)
    generated_files = ingestor.generate_ecommerce_dataset(num_orders=20)

    assert len(generated_files) == 4
    filenames = {f.name for f in generated_files}
    expected = {
        "olist_customers_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
    }
    assert filenames == expected

    # Verify orders CSV has non-empty rows and proper headers
    orders_file = tmp_path / "olist_orders_dataset.csv"
    with open(orders_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 21  # 1 header + 20 orders
    assert lines[0] == "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date"


def test_nyctaxi_dataset_generator(tmp_path: Path):
    ingestor = DatasetIngestor(tmp_path)
    files = ingestor.generate_nyctaxi_dataset(num_records=15)

    assert len(files) == 1
    taxi_file = files[0]
    assert taxi_file.name == "nyc_yellow_taxi_trips.csv"

    with open(taxi_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 16  # 1 header + 15 records
    assert lines[0].startswith("VendorID,tpep_pickup_datetime")


def test_bronze_uploader_local_integrity(tmp_path: Path, monkeypatch):
    # Test local upload and MD5 integrity verification
    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"
    landing_dir.mkdir()
    bronze_dir.mkdir()

    test_file = landing_dir / "test_data.csv"
    test_content = "id,name,value\n1,alpha,10.5\n2,beta,20.0\n"
    test_file.write_text(test_content, encoding="utf-8")

    from config.settings import settings
    uploader = ADLSBronzeUploader()
    # Force local emulation to the temporary bronze dir
    uploader.use_local = True
    monkeypatch.setattr(settings, "LOCAL_BRONZE_PATH", str(bronze_dir))

    dest = uploader.upload_file(test_file, remote_folder="test_folder")
    dest_path = Path(dest)

    assert dest_path.exists()
    assert dest_path.read_text(encoding="utf-8") == test_content

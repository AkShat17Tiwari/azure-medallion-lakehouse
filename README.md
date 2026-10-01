# 🏛️ Enterprise Data Lakehouse: Medallion Architecture

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C.svg?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.x-00ADD8.svg?logo=delta&logoColor=white)](https://delta.io/)
[![Azure ADLS Gen2](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4.svg?logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Tests](https://img.shields.io/badge/Pytest-22%20Passed-brightgreen.svg?logo=pytest&logoColor=white)](https://pytest.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](LICENSE)

An enterprise-grade, end-to-end **Data Lakehouse** implementation engineered with **Python**, **PySpark**, **Delta Lake**, **Azure Data Lake Storage Gen2 (ADLS Gen2)**, and **PostgreSQL**. The platform features automated ingestion, data quality quarantine gates, schema enforcement, idempotent Delta MERGE operations, multidimensional Z-Ordering, and relational database serving.

---

## 📐 Architecture Diagram

```text
+----------------------------------------------------------------------------------------------------+
|                                    DATA SOURCES & ACQUISITION                                      |
|  +---------------------------------------+      +-----------------------------------------------+  |
|  | Brazilian E-Commerce (Olist Public)   |      | NYC Yellow Taxi TLC Trip Records              |  |
|  | Orders, Items, Customers, Payments    |      | Pickups, Dropoffs, Zones, Fares, Tips         |  |
|  +---------------------------------------+      +-----------------------------------------------+  |
+----------------------------------------------------------------------------------------------------+
                                    |                                |
                       [HTTP / Python Ingestor]       [Azure Data Factory (ADF) Copy Activity]
                                    |                                |
                                    v                                v
+----------------------------------------------------------------------------------------------------+
|                         🥉 BRONZE LAYER: RAW LANDING (ADLS Gen2 / Delta)                           |
|  • File Formats: Raw CSV & Append-Only Delta Tables                                               |
|  • Schema Strategy: Schema-on-Read, Zero Data Loss, Raw Content Preservation                       |
|  • Ingestion Audit Metadata: _ingested_at, _source_file, _batch_id                                 |
+----------------------------------------------------------------------------------------------------+
                                                   |
                             [PySpark Permissive Read & Data Hygiene]
                                                   |
                        +--------------------------+--------------------------+
                        |                                                     |
                        v                                                     v
       +---------------------------------+           +-----------------------------------------------+
       |   🚨 DATA QUALITY QUARANTINE    |           |    🥈 SILVER LAYER: ENRICHED & CLEANED DELTA  |
       |  • Malformed / Corrupted Rows   |           |  • Explicit Type Enforcement & Casting        |
       |  • Missing / Blank Primary Keys |           |  • Timestamp Normalization (UTC)              |
       |  • Isolated for Root-Cause Fix  |           |  • Primary Key Deduplication (Window Ranking) |
       +---------------------------------+           |  • Partitioned by (order_year, order_month)   |
                                                     |  • Idempotent Delta MERGE (Upsert)            |
                                                     +-----------------------------------------------+
                                                                             |
                                                      [PySpark Multi-Dimensional Rollups & Z-Order]
                                                                             |
                                                                             v
+----------------------------------------------------------------------------------------------------+
|                           🥇 GOLD LAYER: CURATED BUSINESS MARTS (Delta)                            |
|  • gold_daily_orders_summary: Daily volume, unique customers, delivery lead times, order statuses  |
|  • gold_daily_zone_metrics: Daily pickup zones, payment methods, revenue, average fares & tips     |
|  • Performance Optimizations: Delta File Compaction & Multidimensional Z-Ordering                  |
|  • Partition Pruning: report_year / report_month                                                   |
+----------------------------------------------------------------------------------------------------+
                                                   |
                             [Idempotent psycopg2 / JDBC Batch Loader]
                                                   |
                                                   v
+----------------------------------------------------------------------------------------------------+
|                           🐘 SERVING LAYER: RELATIONAL POSTGRESQL                                  |
|  • Target Tables: public.gold_daily_orders_summary, public.gold_daily_zone_metrics                 |
|  • Idempotency Engine: ON CONFLICT (composite_primary_keys) DO UPDATE SET ...                      |
|  • Analytical B-Tree Indexes & Pre-aggregated Materialized Reporting Views                         |
+----------------------------------------------------------------------------------------------------+
                                                   |
                                    [Sub-Second SQL / BI Serving]
                                                   |
                                                   v
+----------------------------------------------------------------------------------------------------+
|                                    ANALYTICS & BI CONSUMPTION                                      |
|             [Power BI (Direct Lake / SQL)]    [Tableau]    [REST APIs / Microservices]             |
+----------------------------------------------------------------------------------------------------+
```

---

## 💻 Technology Breakdown

| Component | Technology | Version | Purpose & Architecture Value |
| :--- | :--- | :--- | :--- |
| **Compute Engine** | Apache Spark / PySpark | `3.5.x` | Distributed batch transformation, parallel rollups, and memory-optimized DataFrames. |
| **Table Storage** | Delta Lake | `3.2.x` | ACID transactions, time travel, schema enforcement, compaction, and multidimensional Z-Ordering. |
| **Cloud Storage** | Azure ADLS Gen2 | Storage v2 | Hierarchical namespace filesystem (`abfss://`) separating Medallion containers. |
| **Orchestration** | Azure Data Factory / Python | ARM Template | Enterprise data movement with Copy Data activities and automated ingestion scripts. |
| **Relational Serving** | PostgreSQL | `15+` | High-concurrency, low-latency relational serving layer for BI dashboards and reporting queries. |
| **Database Driver** | psycopg2-binary | `2.9.x` | High-throughput batch streaming via `execute_values` with native `ON CONFLICT` upserts. |
| **Quality & Testing** | Pytest | `8.x` | 22 comprehensive unit and integration data quality tests asserting schema and row balance. |
| **Config & Typing** | Pydantic / python-dotenv | `2.x` | Strongly typed configuration handling OAuth, SAS tokens, shared keys, and connection URIs. |

---

## 📁 Repository Directory Structure

```text
data-lakehouse/
├── main.py                                    # Master orchestrator (Bronze -> Silver -> Gold -> PostgreSQL)
├── .env.example                               # Credential template for Azure & PostgreSQL
├── .gitignore                                 # Production ignore rules (secrets, venvs, checkpoints)
├── README.md                                  # Comprehensive architecture guide & documentation
├── requirements.txt                           # Production runtime dependencies
├── requirements-dev.txt                       # Development & test dependencies
│
├── config/                                    # Application Configuration & Spark Factories
│   ├── __init__.py                            # Lazy module loaders
│   ├── settings.py                            # Pydantic & dataclass environment parser
│   └── spark_session.py                       # SparkSession factory (Delta, Hadoop ABFS, JDBC, JDK detector)
│
├── src/                                       # Core Medallion Transformation Modules
│   ├── __init__.py
│   ├── ingest_bronze.py                       # Raw dataset generator/downloader & ADLS Gen2 uploader
│   ├── bronze_to_silver.py                    # Schema enforcement, quarantine gate, deduplication, Delta MERGE
│   ├── silver_to_gold.py                      # Multi-dimensional business rollups, compaction, Z-Ordering
│   ├── gold_to_postgres.py                    # Idempotent batch publisher to PostgreSQL (ON CONFLICT DO UPDATE)
│   ├── connectors/                            # Storage & Database Adapters
│   │   ├── __init__.py
│   │   ├── adls_connector.py                  # ABFSS URI builder & Azure Data Lake SDK client
│   │   └── postgres_connector.py              # PySpark JDBC writer & psycopg2 connection manager
│   ├── bronze/                                # Bronze layer modules
│   │   ├── __init__.py
│   │   └── ingestion.py                       # Raw ingestion with audit metadata
│   ├── silver/                                # Silver layer modules
│   │   ├── __init__.py
│   │   └── transformation.py                  # Data cleaning & upserts
│   ├── gold/                                  # Gold layer modules
│   │   ├── __init__.py
│   │   └── aggregation.py                     # Business KPI rollups
│   ├── pipelines/                             # Pipeline entrypoints
│   │   ├── __init__.py
│   │   └── run_pipeline.py                    # Pipeline stage orchestrator
│   └── utils/                                 # Cross-Cutting Utilities
│       ├── __init__.py
│       └── logger.py                          # Standardized logging formatter
│
├── sql/                                       # Relational Serving Layer DDL
│   └── schema.sql                             # PostgreSQL DDL with primary keys, indexes & analytical views
│
├── notebooks/                                 # Cloud Interactive Notebooks
│   └── bronze_to_silver.py                    # Databricks Repos notebook with widgets & Z-Order
│
├── templates/                                 # Infrastructure as Code
│   └── adf/                                   # Azure Data Factory templates
│       ├── README.md                          # Deployment instructions
│       ├── pipeline_copy_bronze_csv.json      # Standalone ADF Copy Data pipeline JSON
│       └── arm_template_bronze_ingestion.json # Full ARM resource deployment template
│
├── data/                                      # Local Emulation Storage & Raw Landing
│   ├── raw/landing/                           # Staging area for incoming CSVs
│   ├── bronze/                                # Bronze Delta tables & raw files (gitignored)
│   ├── silver/                                # Cleaned & partitioned Silver tables (gitignored)
│   ├── gold/                                  # Curated Gold business marts (gitignored)
│   └── checkpoints/                           # Streaming checkpoints (gitignored)
│
├── scripts/                                   # Operational Scripts
│   ├── init_postgres.sql                      # PostgreSQL initialization DDL
│   └── setup_env.sh                           # Virtual environment creation & dependency installer
│
└── tests/                                     # Automated Test Suite (22 Tests)
    ├── __init__.py
    ├── conftest.py                            # PySpark + Delta Lake testing fixtures
    ├── integration/                           # End-to-End Data Quality & Reconciliation
    │   ├── __init__.py
    │   └── test_data_quality_e2e.py           # Schema compliance, null constraints, volume reconciliation
    └── unit/                                  # Unit Tests
        ├── __init__.py
        ├── test_config.py                     # Configuration, credentials, & URI parsing
        ├── test_ingest_bronze.py              # Data generator & upload integrity verification
        ├── test_bronze_to_silver.py           # Hygiene, quarantine, & deduplication logic
        ├── test_silver_to_gold.py             # Multi-dimensional aggregations
        ├── test_gold_to_postgres.py           # Idempotent SQL upserts & psycopg2 batch loading
        └── test_transformations.py            # Transformation helper tests
```

---

## 🚀 Step-by-Step Setup Guide

### 1. Prerequisites
- **Python**: `3.9` or higher
- **Java**: `Java 11`, `17`, or `21` *(The SparkSession factory automatically detects and configures compatible JVM runtimes)*
- **Git**
- *(Optional)* **PostgreSQL**: Local instance or Docker container for serving layer verification

### 2. Automated Environment Setup
Run the automated initialization script from the project root:
```bash
./scripts/setup_env.sh
```

Or configure manually:
```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Upgrade core tooling & install dependencies
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# 3. Initialize environment variables
cp .env.example .env
```

### 3. Environment & Credential Configuration (`.env`)

The platform supports zero-cost local execution via **Local Storage Emulation** as well as enterprise cloud deployments:

#### Option A: Local Emulation (Default - No Cloud Account Needed)
```env
USE_LOCAL_STORAGE_EMULATION=true
LOCAL_DATA_DIR=./data
LOCAL_BRONZE_PATH=./data/bronze
LOCAL_SILVER_PATH=./data/silver
LOCAL_GOLD_PATH=./data/gold
```

#### Option B: Azure ADLS Gen2 Cloud Mode
Set `USE_LOCAL_STORAGE_EMULATION=false` and configure one of the three authentication methods:
```env
USE_LOCAL_STORAGE_EMULATION=false
AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
AZURE_STORAGE_ENDPOINT_SUFFIX=dfs.core.windows.net
AZURE_CONTAINER_BRONZE=bronze
AZURE_CONTAINER_SILVER=silver
AZURE_CONTAINER_GOLD=gold

# Method 1: Storage Account Key
AZURE_AUTH_TYPE=access_key
AZURE_STORAGE_ACCESS_KEY=your_base64_key==

# Method 2: Shared Access Signature (SAS Token)
# AZURE_AUTH_TYPE=sas_token
# AZURE_STORAGE_SAS_TOKEN=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupx...

# Method 3: Service Principal / Azure AD OAuth
# AZURE_AUTH_TYPE=service_principal
# AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000
# AZURE_CLIENT_ID=00000000-0000-0000-0000-000000000000
# AZURE_CLIENT_SECRET=your_client_secret
```

#### PostgreSQL Serving Configuration:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lakehouse_gold
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=public
POSTGRES_URI=postgresql://postgres:your_password@localhost:5432/lakehouse_gold?sslmode=prefer
POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/lakehouse_gold?sslmode=prefer
```

---

## 🏃 Running the Pipeline

### 1. Master Pipeline Orchestrator (`main.py`)
Run the entire end-to-end Medallion pipeline sequentially:
```bash
# Run complete end-to-end suite across all datasets
.venv/bin/python3 main.py --dataset all --records 1000

# Run with dry-run PostgreSQL publication (verifies SQL without database server)
.venv/bin/python3 main.py --dataset all --records 500 --dry-run-postgres

# Run skipping PostgreSQL publication
.venv/bin/python3 main.py --dataset ecommerce --skip-postgres
```

### 2. Localhost Interactive Web Dashboard (`serve.py`)
Launch the interactive web UI and API server on localhost to explore the Lakehouse:
```bash
.venv/bin/python3 serve.py --port 8080
# Open in browser: http://localhost:8080
```
- **KPI Metrics**: View Bronze file counts, Silver clean record counts, and Gold mart metrics.
- **Interactive Data Viewer**: Browse `gold_daily_orders_summary` and `gold_daily_zone_metrics`.
- **Live APIs**:
  - `GET http://localhost:8080/api/metrics`
  - `GET http://localhost:8080/api/gold/orders`
  - `GET http://localhost:8080/api/gold/zones`
  - `POST http://localhost:8080/api/pipeline/run`

### 3. Running Individual Medallion Stages

```bash
# Stage 1: Ingest raw CSV batches into Bronze layer
.venv/bin/python3 src/ingest_bronze.py --dataset ecommerce --records 1000
.venv/bin/python3 src/ingest_bronze.py --dataset nyctaxi --records 2000

# Stage 2: Cleanse, quarantine bad data, deduplicate & write Silver Delta table
.venv/bin/python3 src/bronze_to_silver.py --dataset ecommerce --table orders
.venv/bin/python3 src/bronze_to_silver.py --dataset nyctaxi

# Stage 3: Generate Gold business marts with compaction & Z-Ordering
.venv/bin/python3 src/silver_to_gold.py --dataset all

# Stage 4: Idempotently publish Gold marts to PostgreSQL
.venv/bin/python3 src/gold_to_postgres.py --all --mode upsert
```

---

## 🧪 Automated Testing & Data Quality Verification

The test suite contains **22 tests** covering unit logic, configuration parsing, data hygiene, and end-to-end data quality reconciliation:

```bash
.venv/bin/pytest -v
```

### Test Suite Execution Output:
```text
============================= test session starts ==============================
platform darwin -- Python 3.9.6, pytest-8.4.2, pluggy-1.6.0
rootdir: /Users/akshattiwari/Documents/data lake
collected 22 items

tests/integration/test_data_quality_e2e.py::test_silver_orders_schema_compliance PASSED       [  4%]
tests/integration/test_data_quality_e2e.py::test_silver_orders_null_and_blank_constraints PASSED [  9%]
tests/integration/test_data_quality_e2e.py::test_silver_orders_primary_key_uniqueness PASSED   [ 13%]
tests/integration/test_data_quality_e2e.py::test_gold_daily_orders_schema_compliance PASSED   [ 18%]
tests/integration/test_data_quality_e2e.py::test_gold_null_and_range_constraints PASSED       [ 22%]
tests/integration/test_data_quality_e2e.py::test_silver_to_gold_volume_reconciliation PASSED   [ 27%]
tests/unit/test_bronze_to_silver.py::test_quarantine_corrupted_records PASSED                  [ 31%]
tests/unit/test_bronze_to_silver.py::test_clean_ecommerce_orders_hygiene PASSED                [ 36%]
tests/unit/test_config.py::test_azure_settings_defaults PASSED                                 [ 40%]
tests/unit/test_config.py::test_postgres_settings_uris PASSED                                  [ 45%]
tests/unit/test_config.py::test_app_settings_local_layer_path PASSED                           [ 50%]
tests/unit/test_gold_to_postgres.py::test_generate_upsert_sql_orders PASSED                     [ 54%]
tests/unit/test_gold_to_postgres.py::test_generate_upsert_sql_zone_metrics PASSED                [ 59%]
tests/unit/test_gold_to_postgres.py::test_publish_via_psycopg2_mocked_upsert PASSED           [ 63%]
tests/unit/test_gold_to_postgres.py::test_publish_via_psycopg2_mocked_overwrite PASSED        [ 68%]
tests/unit/test_ingest_bronze.py::test_ecommerce_dataset_generator PASSED                     [ 72%]
tests/unit/test_ingest_bronze.py::test_nyctaxi_dataset_generator PASSED                        [ 77%]
tests/unit/test_ingest_bronze.py::test_bronze_uploader_local_integrity PASSED                 [ 81%]
tests/unit/test_silver_to_gold.py::test_aggregate_ecommerce_orders PASSED                      [ 86%]
tests/unit/test_silver_to_gold.py::test_aggregate_nyctaxi_zone_metrics PASSED                 [ 90%]
tests/unit/test_transformations.py::test_silver_clean_orders_deduplication_and_nulls PASSED   [ 95%]
tests/unit/test_transformations.py::test_gold_aggregation_metrics PASSED                       [100%]

============================= 22 passed in 10.75s ==============================
```

---

## 🚀 Deployment & Containerization

The project provides multiple deployment models for local development, containerized environments, and cloud PaaS hosting:

### 1. Docker & Docker Compose (Full Stack)
Spin up the complete multi-tier architecture locally, including PostgreSQL 16 (with automated schema DDL execution) and the Lakehouse Observer UI:
```bash
# Build and run all services in the background
docker compose up -d

# Check service logs and health status
docker compose ps
docker compose logs -f lakehouse-observer
```
Access the dashboard at **http://localhost:8080**.

### 2. Standalone Container Deployment
```bash
# Build production Docker container
docker build -t azure-medallion-lakehouse:latest .

# Run container exposing port 8080
docker run -p 8080:8080 azure-medallion-lakehouse:latest
```

### 3. Automated Deployment Script
```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

### 4. 1-Click Cloud Deployment (Render / Railway)
The repository includes a `render.yaml` blueprint and standard `Procfile`. Connecting this GitHub repository to [Render](https://render.com) or [Railway](https://railway.app) will automatically provision and deploy the web dashboard with environment variables configured out of the box.

### 5. Automated CI/CD Pipeline (GitHub Actions)
The repository provides `ci/github-actions-ci.yml` (ready to place into `.github/workflows/ci.yml`):
- Provisions Java 21 Temurin runtime and Python 3.11.
- Executes full PyTest test suite (unit + data quality tests).
- Asserts schema DDL syntax and ADF JSON template compliance.

---

## 💼 Key Resume Talking Points & Interview Deep Dives

Use these talking points on your resume and during senior data engineering technical interviews:

- **Medallion Lakehouse Architecture**:
  > *"Architected an end-to-end Data Lakehouse on Azure ADLS Gen2 using PySpark and Delta Lake, implementing Medallion Architecture (Bronze → Silver → Gold) to transition raw transactional CSV batches into cleaned, deduplicated, and business-aggregated data marts."*
- **Data Quality & Quarantine Gates**:
  > *"Implemented a robust data hygiene framework using PySpark's permissive CSV parser with `_corrupt_record` capture, isolating malformed records into quarantine tables to ensure 100% data auditability without pipeline interruptions."*
- **Delta Lake ACID & Idempotency**:
  > *"Engineered fully idempotent pipeline runs across all layers: utilized Delta Lake `MERGE` (Upsert) on primary keys in the Silver layer and PostgreSQL `ON CONFLICT DO UPDATE` in the serving layer, preventing duplicate records during pipeline retries."*
- **Performance Optimization & Data Skipping**:
  > *"Accelerated analytical query performance for downstream BI tools by implementing date partitioning (`report_year`, `report_month`), file bin-packing compaction, and multidimensional Z-Ordering along high-cardinality query dimensions (`order_status`, `PULocationID`)."*
- **Cross-Layer Data Reconciliation**:
  > *"Authored automated CI/CD data quality test suites using Pytest, asserting schema conformity, non-null primary key constraints, and 100% volumetric reconciliation (`SUM(total_orders)` in Gold equals row count in Silver)."*
- **Hybrid Cloud & Enterprise Governance**:
  > *"Designed modular configuration abstractions supporting Azure ADLS Gen2 Managed Identity / OAuth / SAS tokens alongside a zero-dependency local filesystem emulator, reducing local development cycle times."*

"""Application configuration module.

Loads environment variables from `.env` and provides strongly typed settings
for Azure Data Lake Storage Gen2, PostgreSQL, Spark, and local storage fallback.
Supports both Pydantic and native dataclass fallback.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

ROOT_DIR = Path(__file__).resolve().parent.parent


def _load_env_file(env_path: Path) -> None:
    """Fallback manual .env loader if python-dotenv is not installed."""
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().split(" #")[0].strip()  # Strip inline comments
            val = val.strip("\"'")  # Strip quotes
            if key not in os.environ:
                os.environ[key] = val


# Attempt loading with python-dotenv, otherwise use manual parser
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT_DIR / ".env")
except ImportError:
    _load_env_file(ROOT_DIR / ".env")


@dataclass
class AzureStorageSettings:
    """Configuration for Azure Data Lake Storage Gen2."""
    STORAGE_ACCOUNT_NAME: str = field(
        default_factory=lambda: os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "teststorageaccount")
    )
    STORAGE_ENDPOINT_SUFFIX: str = field(
        default_factory=lambda: os.getenv("AZURE_STORAGE_ENDPOINT_SUFFIX", "dfs.core.windows.net")
    )
    CONTAINER_BRONZE: str = field(
        default_factory=lambda: os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
    )
    CONTAINER_SILVER: str = field(
        default_factory=lambda: os.getenv("AZURE_CONTAINER_SILVER", "silver")
    )
    CONTAINER_GOLD: str = field(
        default_factory=lambda: os.getenv("AZURE_CONTAINER_GOLD", "gold")
    )
    CONTAINER_CHECKPOINT: str = field(
        default_factory=lambda: os.getenv("AZURE_CONTAINER_CHECKPOINT", "checkpoints")
    )
    AUTH_TYPE: str = field(
        default_factory=lambda: os.getenv("AZURE_AUTH_TYPE", "access_key")
    )
    STORAGE_ACCESS_KEY: str = field(
        default_factory=lambda: os.getenv("AZURE_STORAGE_ACCESS_KEY", "")
    )
    STORAGE_SAS_TOKEN: str = field(
        default_factory=lambda: os.getenv("AZURE_STORAGE_SAS_TOKEN", "")
    )
    TENANT_ID: str = field(
        default_factory=lambda: os.getenv("AZURE_TENANT_ID", "")
    )
    CLIENT_ID: str = field(
        default_factory=lambda: os.getenv("AZURE_CLIENT_ID", "")
    )
    CLIENT_SECRET: str = field(
        default_factory=lambda: os.getenv("AZURE_CLIENT_SECRET", "")
    )

    @property
    def clean_sas_token(self) -> str:
        """Strip leading '?' if present from SAS token for Hadoop compatibility."""
        return self.STORAGE_SAS_TOKEN.lstrip("?")

    @property
    def dfs_endpoint(self) -> str:
        """Full DFS host name, e.g. <account>.dfs.core.windows.net"""
        return f"{self.STORAGE_ACCOUNT_NAME}.{self.STORAGE_ENDPOINT_SUFFIX}"

    def get_abfss_uri(self, container: str, path: str = "") -> str:
        """Build ABFSS URI: abfss://<container>@<account>.dfs.core.windows.net/<path>"""
        clean_path = path.lstrip("/")
        return f"abfss://{container}@{self.dfs_endpoint}/{clean_path}"


@dataclass
class PostgresSettings:
    """Configuration for PostgreSQL Gold / Serving layer."""
    HOST: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    PORT: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    DB: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "lakehouse_gold"))
    USER: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    PASSWORD: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))
    SCHEMA: str = field(default_factory=lambda: os.getenv("POSTGRES_SCHEMA", "public"))
    SSLMODE: str = field(default_factory=lambda: os.getenv("POSTGRES_SSLMODE", "prefer"))
    URI: str | None = None
    JDBC_URL: str | None = None
    JDBC_DRIVER: str = field(default_factory=lambda: os.getenv("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"))

    @property
    def connection_uri(self) -> str:
        """psycopg2 / SQLAlchemy connection string."""
        if self.URI:
            return self.URI
        return (
            f"postgresql://{self.USER}:{self.PASSWORD}@"
            f"{self.HOST}:{self.PORT}/{self.DB}?sslmode={self.SSLMODE}"
        )

    @property
    def jdbc_url(self) -> str:
        """PySpark JDBC connection URL."""
        if self.JDBC_URL:
            return self.JDBC_URL
        return f"jdbc:postgresql://{self.HOST}:{self.PORT}/{self.DB}?sslmode={self.SSLMODE}"


@dataclass
class AppSettings:
    """Global Lakehouse application settings."""
    APP_ENV: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    SPARK_APP_NAME: str = field(default_factory=lambda: os.getenv("SPARK_APP_NAME", "Lakehouse-Medallion-Engine"))
    SPARK_MASTER: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))

    USE_LOCAL_STORAGE_EMULATION: bool = field(
        default_factory=lambda: os.getenv("USE_LOCAL_STORAGE_EMULATION", "true").lower() in ("true", "1", "yes")
    )
    LOCAL_DATA_DIR: str = field(default_factory=lambda: os.getenv("LOCAL_DATA_DIR", "./data"))
    LOCAL_BRONZE_PATH: str = field(default_factory=lambda: os.getenv("LOCAL_BRONZE_PATH", "./data/bronze"))
    LOCAL_SILVER_PATH: str = field(default_factory=lambda: os.getenv("LOCAL_SILVER_PATH", "./data/silver"))
    LOCAL_GOLD_PATH: str = field(default_factory=lambda: os.getenv("LOCAL_GOLD_PATH", "./data/gold"))
    LOCAL_CHECKPOINT_PATH: str = field(default_factory=lambda: os.getenv("LOCAL_CHECKPOINT_PATH", "./data/checkpoints"))

    SPARK_PACKAGES: str = field(
        default_factory=lambda: os.getenv(
            "SPARK_PACKAGES",
            (
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-azure:3.3.4,"
                "com.microsoft.azure:azure-storage:8.6.6,"
                "org.postgresql:postgresql:42.7.3"
            ),
        )
    )
    DELTA_AUTO_COMPACT: bool = field(
        default_factory=lambda: os.getenv("DELTA_AUTO_COMPACT", "true").lower() in ("true", "1")
    )
    DELTA_OPTIMIZE_WRITE: bool = field(
        default_factory=lambda: os.getenv("DELTA_OPTIMIZE_WRITE", "true").lower() in ("true", "1")
    )

    azure: AzureStorageSettings = field(default_factory=AzureStorageSettings)
    postgres: PostgresSettings = field(
        default_factory=lambda: PostgresSettings(
            URI=os.getenv("POSTGRES_URI"),
            JDBC_URL=os.getenv("POSTGRES_JDBC_URL"),
        )
    )

    def get_layer_path(self, layer: Literal["bronze", "silver", "gold", "checkpoint"], subpath: str = "") -> str:
        """Returns the appropriate storage path (local directory or Azure ABFSS URI)."""
        clean_subpath = subpath.lstrip("/")
        if self.USE_LOCAL_STORAGE_EMULATION:
            base_dir = {
                "bronze": self.LOCAL_BRONZE_PATH,
                "silver": self.LOCAL_SILVER_PATH,
                "gold": self.LOCAL_GOLD_PATH,
                "checkpoint": self.LOCAL_CHECKPOINT_PATH,
            }[layer]
            path = Path(base_dir) / clean_subpath
            return str(path.resolve())

        container = {
            "bronze": self.azure.CONTAINER_BRONZE,
            "silver": self.azure.CONTAINER_SILVER,
            "gold": self.azure.CONTAINER_GOLD,
            "checkpoint": self.azure.CONTAINER_CHECKPOINT,
        }[layer]
        return self.azure.get_abfss_uri(container, clean_subpath)


# Singleton instance
settings = AppSettings()

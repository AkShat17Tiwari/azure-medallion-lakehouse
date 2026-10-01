"""SparkSession factory with Delta Lake, ADLS Gen2, and PostgreSQL JDBC support."""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config.settings import settings

logger = logging.getLogger(__name__)


def ensure_compatible_java_home() -> None:
    """Ensures JAVA_HOME points to a compatible Java version (11, 17, or 21) on macOS."""
    if sys.platform == "darwin":
        current_java = os.environ.get("JAVA_HOME", "")
        # If JAVA_HOME is unset or pointing to experimental/unsupported JDK 26+
        if not current_java or "jdk-26" in current_java:
            for ver in ["21", "17", "11"]:
                try:
                    res = subprocess.run(
                        ["/usr/libexec/java_home", "-v", ver],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    path = res.stdout.strip()
                    if path and os.path.isdir(path):
                        os.environ["JAVA_HOME"] = path
                        break
                except Exception:
                    continue


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """Creates and returns a SparkSession configured for Delta Lake and ADLS Gen2.

    Configures:
      - Delta Lake SQL extensions and Catalog
      - Azure Hadoop ABFS connector with selected authentication (Key / SAS / OAuth)
      - PostgreSQL JDBC driver coordinates
    """
    ensure_compatible_java_home()
    session_app_name = app_name or settings.SPARK_APP_NAME

    builder = (
        SparkSession.builder.appName(session_app_name)
        .master(settings.SPARK_MASTER)
        # Delta Lake catalog configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Delta table write optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", str(settings.DELTA_OPTIMIZE_WRITE).lower())
        .config("spark.databricks.delta.autoCompact.enabled", str(settings.DELTA_AUTO_COMPACT).lower())
        # Timezone & standard warehouse config
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", settings.SPARK_PACKAGES)
    )

    # Configure Azure ADLS Gen2 Hadoop filesystem options if not in local emulation mode
    if not settings.USE_LOCAL_STORAGE_EMULATION:
        account = settings.azure.STORAGE_ACCOUNT_NAME
        dfs_host = settings.azure.dfs_endpoint

        logger.info(
            f"Configuring Spark for ADLS Gen2 account: {account} using auth: {settings.azure.AUTH_TYPE}"
        )

        if settings.azure.AUTH_TYPE == "access_key":
            if not settings.azure.STORAGE_ACCESS_KEY:
                raise ValueError("AZURE_STORAGE_ACCESS_KEY must be set when AZURE_AUTH_TYPE='access_key'")
            builder = builder.config(
                f"fs.azure.account.key.{dfs_host}",
                settings.azure.STORAGE_ACCESS_KEY,
            )

        elif settings.azure.AUTH_TYPE == "sas_token":
            if not settings.azure.STORAGE_SAS_TOKEN:
                raise ValueError("AZURE_STORAGE_SAS_TOKEN must be set when AZURE_AUTH_TYPE='sas_token'")
            builder = (
                builder.config(f"fs.azure.account.auth.type.{dfs_host}", "SAS")
                .config(
                    f"fs.azure.sas.token.provider.type.{dfs_host}",
                    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
                )
                .config(
                    f"fs.azure.sas.fixed.token.{dfs_host}",
                    settings.azure.clean_sas_token,
                )
            )

        elif settings.azure.AUTH_TYPE == "service_principal":
            if not (settings.azure.TENANT_ID and settings.azure.CLIENT_ID and settings.azure.CLIENT_SECRET):
                raise ValueError(
                    "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are required "
                    "when AZURE_AUTH_TYPE='service_principal'"
                )
            token_endpoint = (
                f"https://login.microsoftonline.com/{settings.azure.TENANT_ID}/oauth2/token"
            )
            builder = (
                builder.config(f"fs.azure.account.auth.type.{dfs_host}", "OAuth")
                .config(
                    f"fs.azure.account.oauth.provider.type.{dfs_host}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                )
                .config(
                    f"fs.azure.account.oauth2.client.id.{dfs_host}",
                    settings.azure.CLIENT_ID,
                )
                .config(
                    f"fs.azure.account.oauth2.client.secret.{dfs_host}",
                    settings.azure.CLIENT_SECRET,
                )
                .config(
                    f"fs.azure.account.oauth2.client.endpoint.{dfs_host}",
                    token_endpoint,
                )
            )
    else:
        logger.info("Using local storage emulation for Delta Lake tables.")

    # Configure Delta Lake with PySpark helper
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(settings.LOG_LEVEL)
    return spark

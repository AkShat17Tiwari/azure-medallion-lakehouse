import os
import shutil
import subprocess
import sys
import tempfile
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def ensure_compatible_java_home() -> None:
    """Ensures JAVA_HOME points to a compatible Java version (11, 17, or 21) on macOS."""
    if sys.platform == "darwin":
        current_java = os.environ.get("JAVA_HOME", "")
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


@pytest.fixture(scope="session")
def spark():
    """Provides a local SparkSession with Delta Lake support for tests."""
    ensure_compatible_java_home()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("Lakehouse-Pytest-Session")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def temp_delta_dir():
    """Provides a temporary directory that is automatically cleaned up after test."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

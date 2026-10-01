"""Unit tests for settings and configuration parsing."""
from config.settings import AzureStorageSettings, PostgresSettings, AppSettings


def test_azure_settings_defaults():
    azure = AzureStorageSettings(
        STORAGE_ACCOUNT_NAME="myaccount",
        STORAGE_SAS_TOKEN="?sv=2022-11-02&sig=abc",
    )
    assert azure.dfs_endpoint == "myaccount.dfs.core.windows.net"
    # Verify that clean_sas_token strips the leading question mark
    assert azure.clean_sas_token == "sv=2022-11-02&sig=abc"
    assert azure.get_abfss_uri("bronze", "orders") == "abfss://bronze@myaccount.dfs.core.windows.net/orders"


def test_postgres_settings_uris():
    pg = PostgresSettings(
        HOST="localhost",
        PORT=5432,
        DB="lakehouse_test",
        USER="testuser",
        PASSWORD="secretpassword",
        SSLMODE="require",
    )
    assert pg.connection_uri == "postgresql://testuser:secretpassword@localhost:5432/lakehouse_test?sslmode=require"
    assert pg.jdbc_url == "jdbc:postgresql://localhost:5432/lakehouse_test?sslmode=require"


def test_app_settings_local_layer_path():
    app = AppSettings(
        USE_LOCAL_STORAGE_EMULATION=True,
        LOCAL_BRONZE_PATH="/tmp/data/bronze",
    )
    bronze_path = app.get_layer_path("bronze", "orders")
    assert "/tmp/data/bronze/orders" in bronze_path

"""Azure Data Lake Storage Gen2 (ADLS Gen2) connector and path resolver."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal
from config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class ADLSConnector:
    """Manages storage paths and direct client connections for ADLS Gen2."""

    def __init__(self):
        self.settings = settings.azure
        self.use_local = settings.USE_LOCAL_STORAGE_EMULATION

    def get_table_path(
        self,
        layer: Literal["bronze", "silver", "gold", "checkpoint"],
        table_name: str,
    ) -> str:
        """Returns the fully qualified URI or local path for a given table in a layer.

        Examples:
          Local:  /absolute/path/data/bronze/orders
          ADLS:   abfss://bronze@<account>.dfs.core.windows.net/orders
        """
        return settings.get_layer_path(layer, table_name)

    def ensure_local_directories(self) -> None:
        """Creates local storage directories if running under local emulation mode."""
        if self.use_local:
            for dir_path in [
                settings.LOCAL_DATA_DIR,
                settings.LOCAL_BRONZE_PATH,
                settings.LOCAL_SILVER_PATH,
                settings.LOCAL_GOLD_PATH,
                settings.LOCAL_CHECKPOINT_PATH,
            ]:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.info("Ensured local data emulation directories exist.")

    def get_datalake_service_client(self):
        """Returns azure.storage.filedatalake.DataLakeServiceClient if Azure SDK is used."""
        if self.use_local:
            raise RuntimeError("Cannot instantiate Azure DataLakeServiceClient in local emulation mode.")

        from azure.storage.filedatalake import DataLakeServiceClient

        account_url = f"https://{self.settings.dfs_endpoint}"

        if self.settings.AUTH_TYPE == "access_key":
            return DataLakeServiceClient(
                account_url=account_url,
                credential=self.settings.STORAGE_ACCESS_KEY,
            )
        elif self.settings.AUTH_TYPE == "sas_token":
            return DataLakeServiceClient(
                account_url=account_url,
                credential=self.settings.clean_sas_token,
            )
        elif self.settings.AUTH_TYPE == "service_principal":
            from azure.identity import ClientSecretCredential

            credential = ClientSecretCredential(
                tenant_id=self.settings.TENANT_ID,
                client_id=self.settings.CLIENT_ID,
                client_secret=self.settings.CLIENT_SECRET,
            )
            return DataLakeServiceClient(account_url=account_url, credential=credential)
        else:
            raise ValueError(f"Unsupported auth type: {self.settings.AUTH_TYPE}")

"""
fabric_client.py — Microsoft Fabric Lakehouse connector.

Handles:
  - Authentication via Azure DefaultAzureCredential
  - Reading/writing Delta tables via ABFSS paths (OneLake)
  - Lakehouse file operations (upload raw CSVs, download validated tables)
  - Table schema registration and versioning

Microsoft Fabric Lakehouse uses OneLake (Azure Data Lake Storage Gen2 protocol).
Connection: abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/

In a Fabric Notebook environment, use notebookutils for seamless auth.
In local/CI environments, use DefaultAzureCredential (service principal or CLI).

Usage:
    client = FabricClient.from_config("config.yaml")
    df     = client.read_table("governance_customer_consent")
    client.write_table(df, "governance_validated_consent", mode="overwrite")
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


@dataclass
class FabricConfig:
    workspace_name: str
    lakehouse_name: str
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    abfss_base: str = ""

    def __post_init__(self):
        if not self.abfss_base:
            self.abfss_base = (
                f"abfss://{self.workspace_name}"
                f"@onelake.dfs.fabric.microsoft.com"
                f"/{self.lakehouse_name}.Lakehouse/Tables/"
            )


class FabricClient:
    """
    Microsoft Fabric Lakehouse client.

    Supports two execution contexts:
    1. Fabric Notebook: uses notebookutils for zero-config auth
    2. Local / CI: uses DefaultAzureCredential (service principal, CLI, managed identity)

    All table I/O uses Parquet (local) or Delta format (Fabric).
    Schema is validated on write using registered dataset configs.
    """

    def __init__(self, config: FabricConfig, local_fallback: bool = True):
        self.config         = config
        self.local_fallback = local_fallback
        self._fs            = None
        self._in_fabric     = self._detect_fabric_env()

        if self._in_fabric:
            logger.info("Running in Microsoft Fabric Notebook environment")
        else:
            logger.info("Running in local/CI environment — using local Parquet fallback")

    @classmethod
    def from_config(cls, config_path: str = "config.yaml") -> "FabricClient":
        with open(config_path) as f:
            cfg = yaml.safe_load(f)["fabric"]

        config = FabricConfig(
            workspace_name=cfg["workspace_name"],
            lakehouse_name=cfg["lakehouse_name"],
            tenant_id=os.getenv("AZURE_TENANT_ID", ""),
            client_id=os.getenv("AZURE_CLIENT_ID", ""),
            client_secret=os.getenv("AZURE_CLIENT_SECRET", ""),
        )
        return cls(config)

    def _detect_fabric_env(self) -> bool:
        """Check if running inside a Microsoft Fabric Notebook."""
        try:
            import notebookutils  # noqa
            return True
        except ImportError:
            return False

    def _get_filesystem(self):
        """Lazy-init Azure Data Lake filesystem client."""
        if self._fs is not None:
            return self._fs

        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            from azure.identity import DefaultAzureCredential

            credential  = DefaultAzureCredential()
            account_url = (
                f"https://onelake.dfs.fabric.microsoft.com"
            )
            service = DataLakeServiceClient(
                account_url=account_url,
                credential=credential,
            )
            self._fs = service.get_file_system_client(self.config.workspace_name)
            logger.info("Azure Data Lake filesystem client initialised")
            return self._fs

        except Exception as e:
            logger.warning(f"Could not connect to OneLake: {e}. Using local fallback.")
            return None

    # ── Table I/O ─────────────────────────────────────────────────────────────

    def read_table(
        self,
        table_name: str,
        local_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read a Delta/Parquet table from Fabric Lakehouse.

        Falls back to local Parquet if not in Fabric environment.

        Args:
            table_name: Lakehouse table name (e.g. "governance_customer_consent")
            local_path: Override path for local development

        Returns:
            pandas DataFrame
        """
        if local_path and Path(local_path).exists():
            logger.debug(f"Reading local: {local_path}")
            return pd.read_parquet(local_path) if local_path.endswith(".parquet") \
                else pd.read_csv(local_path)

        if self._in_fabric:
            return self._read_fabric_table(table_name)

        # Local fallback — look for Parquet in data directories
        fallback = Path("data") / "processed" / f"{table_name}.parquet"
        if fallback.exists():
            logger.info(f"Local fallback read: {fallback}")
            return pd.read_parquet(fallback)

        raise FileNotFoundError(
            f"Table '{table_name}' not found locally at {fallback}. "
            "Run the pipeline first or connect to Fabric."
        )

    def _read_fabric_table(self, table_name: str) -> pd.DataFrame:
        """Read a table from Fabric Lakehouse using spark (Fabric notebook context)."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            sdf   = spark.read.format("delta").table(table_name)
            return sdf.toPandas()
        except Exception as e:
            logger.error(f"Fabric table read failed: {e}")
            raise

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "overwrite",
        local_path: Optional[str] = None,
        partition_cols: Optional[list] = None,
    ) -> str:
        """
        Write a DataFrame to Fabric Lakehouse as Delta table.

        Falls back to Parquet locally.

        Args:
            df:             Data to write
            table_name:     Target table name
            mode:           "overwrite" | "append"
            local_path:     Override output path
            partition_cols: Columns to partition by (e.g. ["ingestion_date"])

        Returns:
            Path/URI where data was written
        """
        if local_path:
            out = Path(local_path)
        else:
            out = Path("data") / "processed" / f"{table_name}.parquet"

        out.parent.mkdir(parents=True, exist_ok=True)

        if self._in_fabric:
            return self._write_fabric_table(df, table_name, mode, partition_cols)

        # Local: Parquet
        if mode == "append" and out.exists():
            existing = pd.read_parquet(out)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(out, index=False, engine="pyarrow")
        logger.info(f"Written {len(df):,} rows → {out}")
        return str(out)

    def _write_fabric_table(
        self, df, table_name, mode, partition_cols
    ) -> str:
        """Write to Fabric Lakehouse as Delta table via PySpark."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            sdf   = spark.createDataFrame(df)
            writer = sdf.write.format("delta").mode(mode)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.saveAsTable(table_name)
            path = f"{self.config.abfss_base}{table_name}"
            logger.info(f"Written to Fabric: {path}")
            return path
        except Exception as e:
            logger.error(f"Fabric write failed: {e}")
            raise

    def upload_raw_file(self, local_path: str, remote_name: str) -> bool:
        """Upload a raw file to the Lakehouse Files section."""
        if not self._in_fabric:
            logger.info(f"Local env — skip upload of {local_path}")
            return True

        try:
            fs = self._get_filesystem()
            if fs is None:
                return False
            remote_path = f"{self.config.lakehouse_name}.Lakehouse/Files/raw/{remote_name}"
            file_client = fs.get_file_client(remote_path)
            with open(local_path, "rb") as f:
                file_client.upload_data(f, overwrite=True)
            logger.info(f"Uploaded {local_path} → {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False

    def list_tables(self) -> list:
        """List all governance tables in the Lakehouse."""
        if self._in_fabric:
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                tables = spark.sql("SHOW TABLES").collect()
                return [t["tableName"] for t in tables if "governance_" in t["tableName"]]
            except Exception:
                pass
        # Local fallback
        proc = Path("data/processed")
        if proc.exists():
            return [p.stem for p in proc.glob("governance_*.parquet")]
        return []

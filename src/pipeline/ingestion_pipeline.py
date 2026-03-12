"""
ingestion_pipeline.py — End-to-end data governance ingestion pipeline.

Flow per dataset:
  1. Load raw data (CSV / Parquet / API)
  2. Register / update metadata catalog entry
  3. Run validation suite (12+ checks)
  4. Route rows: passed → validated/, failed → quarantine/
  5. Write audit log + lineage record
  6. Write validated data to Fabric Lakehouse (Delta table)
  7. Emit alerts if quality score < threshold

Usage:
    pipeline = GovernancePipeline()
    results  = pipeline.run_all()               # all registered datasets
    result   = pipeline.run_dataset("customer_consent")
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from validation.data_quality_checks import DataQualityEngine, SuiteResult, CRITICAL
from metadata.metadata_tracker import MetadataTracker

logger = logging.getLogger(__name__)


@dataclass
class DatasetResult:
    dataset_name: str
    status: str           # success / failed / partial
    quality_score: float
    rows_ingested: int
    rows_passed: int
    rows_rejected: int
    checks_passed: int
    checks_failed: int
    critical_failures: int
    duration_seconds: float
    validated_path: str = ""
    quarantine_path: str = ""
    error: str = ""


@dataclass
class PipelineReport:
    run_timestamp: str
    total_datasets: int
    successful: int
    failed: int
    partial: int
    avg_quality_score: float
    total_rows_processed: int
    total_rows_rejected: int
    dataset_results: list = field(default_factory=list)


class GovernancePipeline:
    """
    Orchestrates the full data governance pipeline across all registered datasets.

    Features:
      - Config-driven: datasets defined in config.yaml
      - Automatic schema validation + metadata tracking
      - Row-level quarantine routing
      - Audit logging on every operation
      - Power BI export at end of each run
    """

    def __init__(
        self,
        config_path: str = "config.yaml",
        data_dir: str = "data/",
    ):
        self.config    = self._load_config(config_path)
        self.data_dir  = Path(data_dir)
        self.valid_dir = self.data_dir / "validated"
        self.reject_dir= self.data_dir / "rejected"
        self.proc_dir  = self.data_dir / "processed"

        for d in [self.valid_dir, self.reject_dir, self.proc_dir,
                  self.data_dir / "audit"]:
            d.mkdir(parents=True, exist_ok=True)

        self.engine  = DataQualityEngine()
        self.tracker = MetadataTracker()

        # Lazy-import Fabric client (avoids Azure deps in local mode)
        self._fabric = None

    def _load_config(self, path: str) -> dict:
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config not found at {path}, using defaults")
            return {"datasets": [], "validation": {}, "pipeline": {}}

    def _get_fabric(self):
        if self._fabric is None:
            try:
                from fabric.fabric_client import FabricClient
                self._fabric = FabricClient.from_config()
            except Exception as e:
                logger.warning(f"Fabric client unavailable: {e}")
        return self._fabric

    # ── Public API ────────────────────────────────────────────────────────────

    def run_all(self) -> PipelineReport:
        """Run pipeline for all registered datasets."""
        from datetime import datetime, timezone
        t0 = time.time()

        datasets = self.config.get("datasets", [])
        if not datasets:
            logger.warning("No datasets configured in config.yaml")

        results = []
        for ds_cfg in datasets:
            try:
                result = self.run_dataset(ds_cfg["name"], ds_config=ds_cfg)
                results.append(result)
            except Exception as e:
                logger.error(f"Pipeline failed for {ds_cfg['name']}: {e}")
                results.append(DatasetResult(
                    dataset_name=ds_cfg["name"], status="failed",
                    quality_score=0.0, rows_ingested=0, rows_passed=0,
                    rows_rejected=0, checks_passed=0, checks_failed=0,
                    critical_failures=0, duration_seconds=0.0, error=str(e),
                ))

        # Aggregate
        successful = [r for r in results if r.status == "success"]
        failed     = [r for r in results if r.status == "failed"]
        partial    = [r for r in results if r.status == "partial"]
        avg_score  = sum(r.quality_score for r in results) / len(results) if results else 0.0

        report = PipelineReport(
            run_timestamp        = datetime.now(timezone.utc).isoformat(),
            total_datasets       = len(results),
            successful           = len(successful),
            failed               = len(failed),
            partial              = len(partial),
            avg_quality_score    = round(avg_score, 1),
            total_rows_processed = sum(r.rows_ingested for r in results),
            total_rows_rejected  = sum(r.rows_rejected for r in results),
            dataset_results      = results,
        )

        logger.info(
            f"Pipeline complete: {len(successful)}/{len(results)} datasets passed · "
            f"avg score={avg_score:.1f} · "
            f"total rows={report.total_rows_processed:,}"
        )

        # Export Power BI files
        self.tracker.export_for_powerbi()

        return report

    def run_dataset(
        self,
        dataset_name: str,
        ds_config: Optional[dict] = None,
    ) -> DatasetResult:
        """
        Run the full governance pipeline for a single dataset.

        Steps:
          1. Load data
          2. Register metadata
          3. Validate (12+ checks)
          4. Route rows
          5. Write outputs
          6. Log audit + lineage
        """
        t0 = time.time()
        logger.info(f"▶  Processing dataset: {dataset_name}")

        # Resolve config
        if ds_config is None:
            cfg_list = self.config.get("datasets", [])
            ds_config = next((d for d in cfg_list if d["name"] == dataset_name), {})

        source_path   = ds_config.get("source_path", f"data/raw/{dataset_name}.csv")
        suite_name    = ds_config.get("validation_suite", "generic")
        schema_ver    = ds_config.get("schema_version", "1.0")
        owner         = ds_config.get("owner", "data-engineering")
        sensitivity   = ds_config.get("sensitivity", "INTERNAL")
        sla_hours     = ds_config.get("sla_freshness_hours", 24)

        # ── Step 1: Load ──────────────────────────────────────────────────────
        try:
            df = self._load_data(source_path, dataset_name)
        except Exception as e:
            return DatasetResult(
                dataset_name=dataset_name, status="failed",
                quality_score=0.0, rows_ingested=0, rows_passed=0,
                rows_rejected=0, checks_passed=0, checks_failed=0,
                critical_failures=0, duration_seconds=time.time()-t0, error=str(e),
            )

        rows_in = len(df)
        self.tracker.log_audit(dataset_name, "INGEST", rows_in,
                               f"Loaded from {source_path}")

        # ── Step 2: Register metadata ─────────────────────────────────────────
        self.tracker.register_dataset(
            name=dataset_name, schema_version=schema_ver,
            owner=owner, sensitivity=sensitivity, source_path=source_path,
            sla_freshness_hours=sla_hours, df=df,
        )

        # ── Step 3: Validate ──────────────────────────────────────────────────
        suite_result: SuiteResult = self.engine.run_suite(df, suite_name, dataset_name)
        logger.info(
            f"   {dataset_name}: score={suite_result.score:.1f} · "
            f"passed={suite_result.passed_checks}/{suite_result.total_checks} · "
            f"critical={suite_result.critical_failures}"
        )

        # ── Step 4: Route rows ────────────────────────────────────────────────
        df_valid, df_rejected = self._route_rows(df, suite_result)

        # ── Step 5: Write outputs ─────────────────────────────────────────────
        valid_path   = self._write_validated(df_valid, dataset_name)
        reject_path  = self._write_quarantine(df_rejected, dataset_name, suite_result)

        # Write to Fabric (if connected)
        fabric = self._get_fabric()
        if fabric:
            try:
                fabric.write_table(
                    df_valid, f"governance_{dataset_name}_validated",
                    partition_cols=["ingestion_date"] if "ingestion_date" in df_valid.columns else None,
                )
            except Exception as e:
                logger.warning(f"Fabric write failed for {dataset_name}: {e}")

        # ── Step 6: Log ───────────────────────────────────────────────────────
        status = (
            "failed"  if suite_result.critical_failures > 0 and
                         self.config.get("validation", {}).get("fail_on_critical", True)
            else "partial" if suite_result.failed_checks > 0
            else "success"
        )

        self.tracker.log_run(
            dataset_name     = dataset_name,
            rows_ingested    = rows_in,
            rows_passed      = len(df_valid),
            rows_rejected    = len(df_rejected),
            quality_score    = suite_result.score,
            duration_seconds = time.time() - t0,
            checks_passed    = suite_result.passed_checks,
            checks_failed    = suite_result.failed_checks,
            critical_failures= suite_result.critical_failures,
            status           = status,
            suite_name       = suite_name,
        )

        self.tracker.log_lineage(
            dataset_name    = dataset_name,
            source          = source_path,
            transformations = ["schema_validation", "quality_checks", "row_routing"],
            sink            = valid_path,
            metadata        = {"quality_score": suite_result.score, "rows_rejected": len(df_rejected)},
        )

        return DatasetResult(
            dataset_name     = dataset_name,
            status           = status,
            quality_score    = suite_result.score,
            rows_ingested    = rows_in,
            rows_passed      = len(df_valid),
            rows_rejected    = len(df_rejected),
            checks_passed    = suite_result.passed_checks,
            checks_failed    = suite_result.failed_checks,
            critical_failures= suite_result.critical_failures,
            duration_seconds = round(time.time() - t0, 3),
            validated_path   = valid_path,
            quarantine_path  = reject_path,
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _load_data(self, source_path: str, dataset_name: str) -> pd.DataFrame:
        """Load data from CSV, Parquet, or use demo data if file missing."""
        path = Path(source_path)
        if path.exists():
            if source_path.endswith(".parquet"):
                return pd.read_parquet(path)
            return pd.read_csv(path)

        # Generate demo data if file doesn't exist
        logger.info(f"Source file not found at {path} — generating demo data for {dataset_name}")
        return _make_demo_dataset(dataset_name)

    def _route_rows(
        self, df: pd.DataFrame, suite_result: SuiteResult
    ) -> tuple:
        """
        Separate rows into validated and quarantine based on check results.

        Rows failing CRITICAL checks on primary key / null checks are quarantined.
        """
        # Identify critical failing columns
        critical_cols = [
            c.column for c in suite_result.checks
            if not c.passed and c.severity == CRITICAL and c.column
        ]

        if not critical_cols:
            # All rows pass
            df_valid = df.copy()
            df_valid["_validation_score"]    = suite_result.score
            df_valid["_ingestion_timestamp"] = pd.Timestamp.now().isoformat()
            return df_valid, pd.DataFrame(columns=df.columns)

        # Build row-level fail mask from critical column checks
        fail_mask = pd.Series(False, index=df.index)
        for col in critical_cols:
            if col in df.columns:
                fail_mask |= df[col].isna()

        df_valid    = df[~fail_mask].copy()
        df_rejected = df[fail_mask].copy()

        df_valid["_validation_score"]    = suite_result.score
        df_valid["_ingestion_timestamp"] = pd.Timestamp.now().isoformat()

        if not df_rejected.empty:
            df_rejected["_rejection_reason"] = "Failed critical validation checks"
            df_rejected["_rejected_at"]       = pd.Timestamp.now().isoformat()

        return df_valid, df_rejected

    def _write_validated(self, df: pd.DataFrame, dataset_name: str) -> str:
        path = self.valid_dir / f"{dataset_name}.parquet"
        df.to_parquet(path, index=False)
        logger.debug(f"Validated → {path} ({len(df):,} rows)")
        return str(path)

    def _write_quarantine(
        self, df: pd.DataFrame, dataset_name: str, suite: SuiteResult
    ) -> str:
        if df.empty:
            return ""
        ts   = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        path = self.reject_dir / f"{dataset_name}_{ts}_quarantine.parquet"
        df.to_parquet(path, index=False)
        self.tracker.log_audit(
            dataset_name, "QUARANTINE", len(df),
            f"{len(df):,} rows quarantined — score={suite.score:.1f}",
            severity="WARNING" if suite.score >= 70 else "CRITICAL",
        )
        return str(path)


# ─────────────────────────────────────────────────────────────────────────────
# Demo data generator
# ─────────────────────────────────────────────────────────────────────────────

def _make_demo_dataset(name: str, n: int = 5000) -> pd.DataFrame:
    """Generate realistic synthetic demo data for each dataset type."""
    import numpy as np
    rng = np.random.default_rng(hash(name) % (2**32))

    base_ids  = [f"ID-{i:07d}" for i in range(n)]
    emails    = [f"user{i}@example{'bad' if i % 30 == 0 else ''}.com" for i in range(n)]
    dates     = pd.date_range("2022-01-01", periods=n, freq="1h")
    amounts   = rng.exponential(500, n).round(2)

    templates = {
        "customer_consent": pd.DataFrame({
            "customer_id":    base_ids,
            "email":          emails,
            "consent_date":   dates.strftime("%Y-%m-%d"),
            "legal_basis":    rng.choice(["consent","legitimate_interest","contract"], n),
            "consent_given":  rng.choice(["true","false"], n, p=[0.85, 0.15]),
            "withdrawal_date": [None] * n,
        }),
        "financial_transactions": pd.DataFrame({
            "transaction_id":   [f"TXN-{i:08d}" for i in range(n)],
            "amount":           amounts,
            "currency":         rng.choice(["USD","EUR","GBP","INR"], n, p=[0.6,0.2,0.1,0.1]),
            "transaction_date": dates.strftime("%Y-%m-%d"),
            "status":           rng.choice(["completed","pending","failed","refunded"], n),
            "transaction_type": rng.choice(["purchase","refund","transfer"], n, p=[0.8,0.1,0.1]),
        }),
        "user_profiles": pd.DataFrame({
            "user_id":    base_ids,
            "email":      emails,
            "created_at": dates.strftime("%Y-%m-%d"),
            "country":    rng.choice(["US","GB","IN","DE","AU","CA"], n),
            "age":        rng.integers(18, 75, n).astype(float),
        }),
    }

    df = templates.get(name)
    if df is None:
        # Generic fallback
        df = pd.DataFrame({
            "id":         base_ids,
            "name":       [f"Record {i}" for i in range(n)],
            "value":      amounts,
            "created_at": dates.strftime("%Y-%m-%d"),
            "status":     rng.choice(["active","inactive"], n),
        })

    # Inject realistic data quality issues for demo
    null_idx = rng.choice(n, size=int(n * 0.02), replace=False)
    df.iloc[null_idx, 1] = None   # ~2% nulls in second column
    return df

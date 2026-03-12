"""
metadata_tracker.py — Data catalog, schema versioning, lineage tracking, and audit logging.

Tracks:
  - Dataset registration and schema versioning
  - Column-level metadata (type, nullable, PII flag, description)
  - Pipeline run history (ingestion events with row counts, scores, duration)
  - Data lineage (source → transformations → targets)
  - Audit log (who touched what data, when, what action)

All metadata persists as JSON files locally; in Fabric environments
these are written to the Lakehouse governance Delta tables.

Usage:
    tracker = MetadataTracker()
    tracker.register_dataset("customer_consent", schema_v="1.0", owner="data-eng@co.com")
    tracker.log_run("customer_consent", rows_in=50000, rows_out=49800, score=94.5)
    audit_df = tracker.get_audit_log()
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

AUDIT_DIR = Path("data/audit")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnMeta:
    name: str
    dtype: str
    nullable: bool = True
    is_pii: bool = False
    description: str = ""
    example: str = ""


@dataclass
class DatasetMeta:
    dataset_id: str
    name: str
    schema_version: str
    owner: str
    sensitivity: str          # PUBLIC / INTERNAL / CONFIDENTIAL / RESTRICTED / PII
    source_path: str
    registered_at: str
    last_updated: str
    columns: list = field(default_factory=list)   # List[ColumnMeta]
    tags: list = field(default_factory=list)
    description: str = ""
    sla_freshness_hours: int = 24
    row_count: int = 0
    active: bool = True


@dataclass
class PipelineRun:
    run_id: str
    dataset_name: str
    run_timestamp: str
    status: str               # success / failed / partial
    rows_ingested: int
    rows_passed: int
    rows_rejected: int
    quality_score: float
    duration_seconds: float
    checks_passed: int
    checks_failed: int
    critical_failures: int
    triggered_by: str = "scheduler"
    error_message: str = ""
    suite_name: str = ""


@dataclass
class AuditEvent:
    event_id: str
    timestamp: str
    dataset_name: str
    action: str               # INGEST / VALIDATE / QUARANTINE / APPROVE / REJECT / EXPORT
    actor: str                # service account or user
    rows_affected: int
    details: str
    severity: str = "INFO"    # INFO / WARNING / CRITICAL


@dataclass
class LineageNode:
    node_id: str
    name: str
    node_type: str            # source / transform / sink
    dataset_name: str
    timestamp: str
    metadata: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# MetadataTracker
# ─────────────────────────────────────────────────────────────────────────────

class MetadataTracker:
    """
    Persistent metadata store for data governance.

    Stores all metadata as append-only JSON-Lines files locally,
    exposing pandas DataFrames for querying and reporting.

    In production, replace _write / _read with Delta table I/O via FabricClient.
    """

    DATASETS_FILE  = AUDIT_DIR / "datasets.jsonl"
    RUNS_FILE      = AUDIT_DIR / "pipeline_runs.jsonl"
    AUDIT_FILE     = AUDIT_DIR / "audit_log.jsonl"
    LINEAGE_FILE   = AUDIT_DIR / "lineage.jsonl"

    def __init__(self):
        for f in [self.DATASETS_FILE, self.RUNS_FILE, self.AUDIT_FILE, self.LINEAGE_FILE]:
            f.touch()

    # ── Dataset registration ──────────────────────────────────────────────────

    def register_dataset(
        self,
        name: str,
        schema_version: str = "1.0",
        owner: str = "data-engineering",
        sensitivity: str = "INTERNAL",
        source_path: str = "",
        description: str = "",
        sla_freshness_hours: int = 24,
        columns: Optional[list] = None,
        tags: Optional[list] = None,
        df: Optional[pd.DataFrame] = None,
    ) -> DatasetMeta:
        """
        Register a dataset in the data catalog.

        If df is provided, auto-infers column metadata (types, nullability).
        """
        now = datetime.now(timezone.utc).isoformat()

        # Auto-infer columns from DataFrame if provided
        inferred_cols = []
        if df is not None:
            for col in df.columns:
                is_pii = any(kw in col.lower() for kw in
                             ["email","phone","name","address","ssn","dob","birth"])
                inferred_cols.append(ColumnMeta(
                    name=col,
                    dtype=str(df[col].dtype),
                    nullable=bool(df[col].isna().any()),
                    is_pii=is_pii,
                ))
        cols = columns or [asdict(c) for c in inferred_cols]

        meta = DatasetMeta(
            dataset_id         = str(uuid.uuid4()),
            name               = name,
            schema_version     = schema_version,
            owner              = owner,
            sensitivity        = sensitivity,
            source_path        = source_path,
            registered_at      = now,
            last_updated       = now,
            columns            = cols,
            tags               = tags or [],
            description        = description,
            sla_freshness_hours= sla_freshness_hours,
            row_count          = len(df) if df is not None else 0,
            active             = True,
        )
        self._append(self.DATASETS_FILE, asdict(meta))
        logger.info(f"Registered dataset: {name} v{schema_version}")
        return meta

    def update_dataset_stats(self, name: str, row_count: int):
        """Update the row count and last_updated timestamp."""
        self._append_event(AuditEvent(
            event_id     = str(uuid.uuid4()),
            timestamp    = datetime.now(timezone.utc).isoformat(),
            dataset_name = name,
            action       = "STATS_UPDATE",
            actor        = "pipeline",
            rows_affected= row_count,
            details      = f"Row count updated to {row_count:,}",
        ))

    # ── Pipeline run logging ──────────────────────────────────────────────────

    def log_run(
        self,
        dataset_name: str,
        rows_ingested: int,
        rows_passed: int,
        rows_rejected: int,
        quality_score: float,
        duration_seconds: float,
        checks_passed: int = 0,
        checks_failed: int = 0,
        critical_failures: int = 0,
        status: str = "success",
        error_message: str = "",
        suite_name: str = "",
        triggered_by: str = "scheduler",
    ) -> PipelineRun:
        """Log a pipeline execution run."""
        run = PipelineRun(
            run_id           = str(uuid.uuid4()),
            dataset_name     = dataset_name,
            run_timestamp    = datetime.now(timezone.utc).isoformat(),
            status           = status if not error_message else "failed",
            rows_ingested    = rows_ingested,
            rows_passed      = rows_passed,
            rows_rejected    = rows_rejected,
            quality_score    = round(quality_score, 1),
            duration_seconds = round(duration_seconds, 3),
            checks_passed    = checks_passed,
            checks_failed    = checks_failed,
            critical_failures= critical_failures,
            triggered_by     = triggered_by,
            error_message    = error_message,
            suite_name       = suite_name,
        )
        self._append(self.RUNS_FILE, asdict(run))

        # Auto-audit
        severity = "CRITICAL" if critical_failures > 0 else "WARNING" if checks_failed > 0 else "INFO"
        self.log_audit(
            dataset_name  = dataset_name,
            action        = "VALIDATE",
            rows_affected = rows_ingested,
            details       = (
                f"Quality score: {quality_score:.1f}/100 · "
                f"Passed: {checks_passed} · Failed: {checks_failed} · "
                f"Rejected rows: {rows_rejected:,}"
            ),
            severity      = severity,
        )
        return run

    # ── Audit logging ─────────────────────────────────────────────────────────

    def log_audit(
        self,
        dataset_name: str,
        action: str,
        rows_affected: int = 0,
        details: str = "",
        severity: str = "INFO",
        actor: str = "pipeline-service",
    ):
        """Append an entry to the audit log."""
        event = AuditEvent(
            event_id     = str(uuid.uuid4()),
            timestamp    = datetime.now(timezone.utc).isoformat(),
            dataset_name = dataset_name,
            action       = action,
            actor        = actor,
            rows_affected= rows_affected,
            details      = details,
            severity     = severity,
        )
        self._append_event(event)

    def _append_event(self, event: AuditEvent):
        self._append(self.AUDIT_FILE, asdict(event))

    # ── Lineage tracking ──────────────────────────────────────────────────────

    def log_lineage(
        self,
        dataset_name: str,
        source: str,
        transformations: list,
        sink: str,
        metadata: Optional[dict] = None,
    ):
        """Record data lineage: source → transformations → sink."""
        now = datetime.now(timezone.utc).isoformat()
        nodes = [
            LineageNode(str(uuid.uuid4()), source, "source", dataset_name, now),
            *[LineageNode(str(uuid.uuid4()), t, "transform", dataset_name, now)
              for t in transformations],
            LineageNode(str(uuid.uuid4()), sink, "sink", dataset_name, now,
                        metadata=metadata or {}),
        ]
        for node in nodes:
            self._append(self.LINEAGE_FILE, asdict(node))
        logger.debug(f"Lineage logged: {source} → {sink} ({len(transformations)} transforms)")

    # ── Query methods ─────────────────────────────────────────────────────────

    def get_datasets(self) -> pd.DataFrame:
        return self._read_jsonl(self.DATASETS_FILE)

    def get_pipeline_runs(self, dataset_name: Optional[str] = None) -> pd.DataFrame:
        df = self._read_jsonl(self.RUNS_FILE)
        if not df.empty and dataset_name:
            df = df[df["dataset_name"] == dataset_name]
        return df

    def get_audit_log(
        self,
        dataset_name: Optional[str] = None,
        since_hours: int = 24,
    ) -> pd.DataFrame:
        df = self._read_jsonl(self.AUDIT_FILE)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=since_hours)
        df     = df[df["timestamp"] >= cutoff]
        if dataset_name:
            df = df[df["dataset_name"] == dataset_name]
        return df.sort_values("timestamp", ascending=False)

    def get_quality_trend(self, dataset_name: str, last_n: int = 30) -> pd.DataFrame:
        """Return quality score trend for a dataset."""
        runs = self.get_pipeline_runs(dataset_name)
        if runs.empty:
            return pd.DataFrame()
        runs["run_timestamp"] = pd.to_datetime(runs["run_timestamp"], utc=True, errors="coerce")
        return (
            runs.sort_values("run_timestamp", ascending=False)
            .head(last_n)[["run_timestamp","quality_score","rows_ingested","rows_rejected","status"]]
            .reset_index(drop=True)
        )

    def summary_stats(self) -> dict:
        """Quick summary for dashboard header tiles."""
        runs = self._read_jsonl(self.RUNS_FILE)
        datasets = self._read_jsonl(self.DATASETS_FILE)
        audit    = self._read_jsonl(self.AUDIT_FILE)

        return {
            "total_datasets":     len(datasets),
            "active_datasets":    int(datasets["active"].sum()) if not datasets.empty and "active" in datasets.columns else 0,
            "total_runs":         len(runs),
            "avg_quality_score":  round(float(runs["quality_score"].mean()), 1) if not runs.empty and "quality_score" in runs.columns else 0.0,
            "total_rows_processed": int(runs["rows_ingested"].sum()) if not runs.empty else 0,
            "total_rows_rejected":  int(runs["rows_rejected"].sum()) if not runs.empty else 0,
            "audit_events_24h":   len(self.get_audit_log(since_hours=24)),
            "failed_runs":        int((runs["status"] == "failed").sum()) if not runs.empty else 0,
        }

    # ── Storage ───────────────────────────────────────────────────────────────

    def _append(self, path: Path, obj: dict):
        with open(path, "a") as f:
            f.write(json.dumps(obj, default=str) + "\n")

    def _read_jsonl(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        try:
            rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"Failed to read {path}: {e}")
            return pd.DataFrame()

    def export_for_powerbi(self, output_dir: str = "data/processed/powerbi") -> dict:
        """Export all metadata tables as Power BI–ready CSVs."""
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        outputs = {}

        for name, getter in [
            ("datasets",       self.get_datasets),
            ("pipeline_runs",  self.get_pipeline_runs),
            ("audit_log",      lambda: self.get_audit_log(since_hours=8760)),
        ]:
            df   = getter()
            path = out / f"governance_{name}.csv"
            df.to_csv(path, index=False)
            outputs[name] = str(path)
            logger.info(f"Exported {name}: {len(df)} rows → {path}")

        return outputs

"""
powerbi_exporter.py — Exports governance data in Power BI–ready format.

Produces 5 linked tables:
  1. governance_datasets.csv         — dataset catalog with metadata
  2. governance_pipeline_runs.csv    — per-run quality scores and row counts
  3. governance_quality_checks.csv   — individual check results per run
  4. governance_audit_log.csv        — all audit events
  5. governance_kpi_summary.csv      — pre-aggregated KPIs for dashboard tiles

Power BI relationships:
  datasets[name] → pipeline_runs[dataset_name] (1:many)
  pipeline_runs[run_id] → quality_checks[run_id] (1:many)
  datasets[name] → audit_log[dataset_name] (1:many)

Usage:
    exporter = PowerBIExporter()
    paths    = exporter.export_all(tracker, suite_results)
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

POWERBI_DATA_DICT = """
POWER BI DATA DICTIONARY — Data Governance & Quality Monitoring Platform
=========================================================================

TABLE: governance_datasets.csv
  name                  — Dataset identifier (primary key)
  schema_version        — Semantic version of the dataset schema
  owner                 — Owning team email
  sensitivity           — Data classification: PUBLIC/INTERNAL/CONFIDENTIAL/RESTRICTED/PII
  row_count             — Latest known row count
  active                — Boolean: is dataset actively monitored
  sla_freshness_hours   — Max acceptable hours between ingestion runs

TABLE: governance_pipeline_runs.csv
  run_id                — Unique run identifier (UUID)
  dataset_name          — FK → governance_datasets[name]
  run_timestamp         — UTC ISO-8601 run start time
  status                — success / partial / failed
  rows_ingested         — Total rows in source file
  rows_passed           — Rows routed to validated output
  rows_rejected         — Rows quarantined due to quality failures
  quality_score         — 0–100 weighted quality score for this run
  checks_passed         — Count of passing quality checks
  checks_failed         — Count of failing quality checks
  critical_failures     — Count of CRITICAL severity failures
  duration_seconds      — Pipeline execution time

TABLE: governance_kpi_summary.csv
  dataset_name          — FK → governance_datasets[name]
  latest_score          — Most recent quality score
  avg_score_30d         — Rolling 30-day average quality score
  score_trend           — 'improving' / 'stable' / 'degrading'
  total_rows_30d        — Total rows processed in last 30 days
  rejection_rate_pct    — % rows rejected in last 30 days
  last_run_timestamp    — Most recent run time
  last_run_status       — Most recent run status
  sla_breach            — Boolean: last run older than sla_freshness_hours

TABLE: governance_audit_log.csv
  event_id              — Unique event identifier
  timestamp             — UTC ISO-8601
  dataset_name          — FK → governance_datasets[name]
  action                — INGEST / VALIDATE / QUARANTINE / APPROVE / REJECT / EXPORT
  actor                 — Service account or user
  rows_affected         — Number of rows affected by this action
  severity              — INFO / WARNING / CRITICAL
  details               — Human-readable description

RECOMMENDED POWER BI MEASURES (DAX)
  Avg Quality Score     = AVERAGE(pipeline_runs[quality_score])
  High Risk Datasets    = COUNTROWS(FILTER(kpi_summary, [latest_score] < 80))
  Total Rows Processed  = SUM(pipeline_runs[rows_ingested])
  Rejection Rate        = DIVIDE(SUM(pipeline_runs[rows_rejected]), SUM(pipeline_runs[rows_ingested]))
  Failed Runs 24h       = COUNTROWS(FILTER(pipeline_runs, [status]="failed" && [run_timestamp] >= NOW()-1))
  Score Sparkline       = Use pipeline_runs[run_timestamp] + [quality_score] per dataset
"""


class PowerBIExporter:
    """
    Exports governance metadata to Power BI–optimised CSV files.

    Handles pre-aggregation (KPI summary table) so Power BI
    can be thin on computation and rich on visualisation.
    """

    def __init__(self, output_dir: str = "data/processed/powerbi"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(
        self,
        tracker,
        extra_check_results: Optional[list] = None,
    ) -> dict:
        """
        Export all Power BI tables.

        Args:
            tracker:             MetadataTracker instance
            extra_check_results: Optional list of SuiteResult objects for check-level detail

        Returns:
            Dict mapping table name → file path
        """
        outputs = {}

        # 1. Datasets
        outputs["datasets"] = self._export_datasets(tracker)

        # 2. Pipeline runs
        outputs["pipeline_runs"] = self._export_runs(tracker)

        # 3. KPI summary
        outputs["kpi_summary"] = self._export_kpi_summary(tracker)

        # 4. Audit log
        outputs["audit_log"] = self._export_audit(tracker)

        # 5. Data dictionary
        dict_path = self.output_dir / "data_dictionary.txt"
        dict_path.write_text(POWERBI_DATA_DICT)
        outputs["data_dictionary"] = str(dict_path)

        logger.info(f"Power BI export complete: {len(outputs)} files → {self.output_dir}")
        return outputs

    def _export_datasets(self, tracker) -> str:
        df   = tracker.get_datasets()
        path = self.output_dir / "governance_datasets.csv"
        if not df.empty:
            # Flatten list columns for Power BI
            if "columns" in df.columns:
                df["column_count"] = df["columns"].apply(
                    lambda x: len(x) if isinstance(x, list) else 0
                )
                df["pii_columns"] = df["columns"].apply(
                    lambda x: sum(1 for c in (x or []) if isinstance(c, dict) and c.get("is_pii"))
                )
                df = df.drop(columns=["columns"], errors="ignore")
            if "tags" in df.columns:
                df["tags"] = df["tags"].apply(lambda x: "|".join(x) if isinstance(x, list) else str(x))
        df.to_csv(path, index=False)
        return str(path)

    def _export_runs(self, tracker) -> str:
        df   = tracker.get_pipeline_runs()
        path = self.output_dir / "governance_pipeline_runs.csv"
        if not df.empty:
            df["run_timestamp"] = pd.to_datetime(df["run_timestamp"], errors="coerce")
            df["pass_rate_pct"] = (
                df["rows_passed"] / df["rows_ingested"].replace(0, 1) * 100
            ).round(1)
        df.to_csv(path, index=False)
        return str(path)

    def _export_kpi_summary(self, tracker) -> str:
        """Pre-aggregate KPIs per dataset for dashboard summary tiles."""
        runs = tracker.get_pipeline_runs()
        path = self.output_dir / "governance_kpi_summary.csv"

        if runs.empty:
            pd.DataFrame().to_csv(path, index=False)
            return str(path)

        runs["run_timestamp"] = pd.to_datetime(runs["run_timestamp"], errors="coerce", utc=True)
        cutoff_30d = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)

        rows = []
        for dataset, grp in runs.groupby("dataset_name"):
            grp_sorted  = grp.sort_values("run_timestamp", ascending=False)
            last_run    = grp_sorted.iloc[0]
            grp_30d     = grp[grp["run_timestamp"] >= cutoff_30d]

            avg_score_30d = grp_30d["quality_score"].mean() if not grp_30d.empty else last_run["quality_score"]

            # Score trend: compare last 5 runs vs prior 5
            if len(grp_sorted) >= 10:
                recent_avg = grp_sorted.head(5)["quality_score"].mean()
                prior_avg  = grp_sorted.iloc[5:10]["quality_score"].mean()
                trend = "improving" if recent_avg > prior_avg + 2 else \
                        "degrading" if recent_avg < prior_avg - 2 else "stable"
            else:
                trend = "stable"

            total_rows_30d   = int(grp_30d["rows_ingested"].sum()) if not grp_30d.empty else 0
            total_rejected_30d = int(grp_30d["rows_rejected"].sum()) if not grp_30d.empty else 0
            rejection_rate   = round(
                total_rejected_30d / total_rows_30d * 100
                if total_rows_30d > 0 else 0.0, 2
            )

            rows.append({
                "dataset_name":        dataset,
                "latest_score":        round(float(last_run["quality_score"]), 1),
                "avg_score_30d":       round(float(avg_score_30d), 1),
                "score_trend":         trend,
                "total_rows_30d":      total_rows_30d,
                "rejection_rate_pct":  rejection_rate,
                "last_run_timestamp":  str(last_run["run_timestamp"]),
                "last_run_status":     str(last_run["status"]),
                "runs_30d":            len(grp_30d),
                "failed_runs_30d":     int((grp_30d["status"] == "failed").sum()) if not grp_30d.empty else 0,
            })

        df = pd.DataFrame(rows).sort_values("latest_score", ascending=True)
        df.to_csv(path, index=False)
        return str(path)

    def _export_audit(self, tracker) -> str:
        df   = tracker.get_audit_log(since_hours=8760)  # 1 year
        path = self.output_dir / "governance_audit_log.csv"
        df.to_csv(path, index=False)
        return str(path)

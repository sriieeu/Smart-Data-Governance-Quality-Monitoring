"""
tests/test_governance.py — Full test suite for the Data Governance Platform.

Run:  pytest tests/ -v
      pytest tests/ -v --cov=src --cov-report=term-missing
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
import pandas as pd
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture
def good_consent_df():
    n = 200
    return pd.DataFrame({
        "customer_id":  [f"C-{i:06d}" for i in range(n)],
        "email":        [f"user{i}@example.com" for i in range(n)],
        "consent_date": pd.date_range("2023-01-01", periods=n, freq="1D").strftime("%Y-%m-%d"),
        "legal_basis":  ["consent"] * n,
        "consent_given":["true"] * n,
    })


@pytest.fixture
def bad_consent_df():
    n = 100
    emails = [f"user{i}@example.com" for i in range(n)]
    emails[10] = "not-an-email"
    emails[20] = "also-bad"
    dates = pd.date_range("2020-01-01", periods=n, freq="1D").strftime("%Y-%m-%d")
    ids   = [f"C-{i:06d}" for i in range(n)]
    ids[5] = None   # null ID
    return pd.DataFrame({
        "customer_id":  ids,
        "email":        emails,
        "consent_date": dates,
        "legal_basis":  ["unknown_basis"] * n,   # invalid value
        "consent_given":["true"] * n,
    })


@pytest.fixture
def good_financial_df():
    n = 300
    return pd.DataFrame({
        "transaction_id": [f"TXN-{i:08d}" for i in range(n)],
        "amount":         np.random.exponential(200, n).round(2),
        "currency":       ["USD"] * n,
        "transaction_date": pd.date_range("2023-01-01", periods=n, freq="1h").strftime("%Y-%m-%d"),
        "status":         ["completed"] * n,
    })


@pytest.fixture
def tracker(tmp_path, monkeypatch):
    """MetadataTracker with temp audit dir."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "audit").mkdir(parents=True)
    from metadata.metadata_tracker import MetadataTracker
    return MetadataTracker()


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality Check Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestCheckPrimitives:

    def test_not_null_passes_clean(self, good_consent_df):
        from validation.data_quality_checks import _check_not_null
        r = _check_not_null(good_consent_df, "customer_id", max_null_pct=0.0)
        assert r.passed is True
        assert r.failing_rows == 0

    def test_not_null_fails_with_nulls(self, bad_consent_df):
        from validation.data_quality_checks import _check_not_null
        r = _check_not_null(bad_consent_df, "customer_id", max_null_pct=0.0)
        assert r.passed is False
        assert r.failing_rows >= 1

    def test_regex_email_valid(self, good_consent_df):
        from validation.data_quality_checks import _check_regex, EMAIL_RE
        r = _check_regex(good_consent_df, "email", EMAIL_RE, "email format")
        assert r.passed is True

    def test_regex_email_invalid(self, bad_consent_df):
        from validation.data_quality_checks import _check_regex, EMAIL_RE
        r = _check_regex(bad_consent_df, "email", EMAIL_RE, "email format")
        assert r.passed is False
        assert r.failing_rows >= 2

    def test_value_set_passes(self, good_consent_df):
        from validation.data_quality_checks import _check_value_set, VALID_LEGAL_BASIS
        r = _check_value_set(good_consent_df, "legal_basis", VALID_LEGAL_BASIS)
        assert r.passed is True

    def test_value_set_fails(self, bad_consent_df):
        from validation.data_quality_checks import _check_value_set, VALID_LEGAL_BASIS
        r = _check_value_set(bad_consent_df, "legal_basis", VALID_LEGAL_BASIS)
        assert r.passed is False

    def test_uniqueness_passes(self, good_consent_df):
        from validation.data_quality_checks import _check_uniqueness
        r = _check_uniqueness(good_consent_df, "customer_id")
        assert r.passed is True

    def test_uniqueness_fails_duplicates(self):
        from validation.data_quality_checks import _check_uniqueness
        df = pd.DataFrame({"id": ["A","A","B","C"]})
        r  = _check_uniqueness(df, "id")
        assert r.passed is False
        assert r.failing_rows >= 1

    def test_numeric_range_passes(self, good_financial_df):
        from validation.data_quality_checks import _check_numeric_range
        r = _check_numeric_range(good_financial_df, "amount", min_val=0.0)
        assert r.passed is True

    def test_numeric_range_fails_negatives(self):
        from validation.data_quality_checks import _check_numeric_range
        df = pd.DataFrame({"amount": [100, -50, 200, -10]})
        r  = _check_numeric_range(df, "amount", min_val=0.0)
        assert r.passed is False
        assert r.failing_rows == 2

    def test_date_recency_recent_passes(self):
        from validation.data_quality_checks import _check_date_recency
        df = pd.DataFrame({"dt": [pd.Timestamp.now().strftime("%Y-%m-%d")] * 10})
        r  = _check_date_recency(df, "dt", max_age_days=30)
        assert r.passed is True

    def test_date_recency_stale_fails(self):
        from validation.data_quality_checks import _check_date_recency
        df = pd.DataFrame({"dt": ["2015-01-01"] * 10})
        r  = _check_date_recency(df, "dt", max_age_days=365)
        assert r.passed is False

    def test_no_future_dates_passes(self):
        from validation.data_quality_checks import _check_no_future_dates
        df = pd.DataFrame({"dt": ["2023-01-01"] * 5})
        r  = _check_no_future_dates(df, "dt")
        assert r.passed is True

    def test_no_future_dates_fails(self):
        from validation.data_quality_checks import _check_no_future_dates
        df = pd.DataFrame({"dt": ["2099-01-01", "2023-01-01"]})
        r  = _check_no_future_dates(df, "dt")
        assert r.passed is False

    def test_row_count_passes(self, good_consent_df):
        from validation.data_quality_checks import _check_row_count
        r = _check_row_count(good_consent_df, min_rows=100)
        assert r.passed is True

    def test_row_count_fails_empty(self):
        from validation.data_quality_checks import _check_row_count
        r = _check_row_count(pd.DataFrame(), min_rows=1)
        assert r.passed is False

    def test_schema_columns_passes(self, good_consent_df):
        from validation.data_quality_checks import _check_schema_columns
        r = _check_schema_columns(good_consent_df, ["customer_id","email","consent_date"])
        assert r.passed is True

    def test_schema_columns_fails_missing(self, good_consent_df):
        from validation.data_quality_checks import _check_schema_columns
        r = _check_schema_columns(good_consent_df, ["customer_id","nonexistent_col"])
        assert r.passed is False

    def test_missing_column_returns_fail(self):
        from validation.data_quality_checks import _check_not_null
        df = pd.DataFrame({"a": [1,2,3]})
        r  = _check_not_null(df, "nonexistent")
        assert r.passed is False


# ─────────────────────────────────────────────────────────────────────────────
# Suite Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestSuites:

    def setup_method(self):
        from validation.data_quality_checks import DataQualityEngine
        self.engine = DataQualityEngine()

    def test_consent_suite_clean_data(self, good_consent_df):
        result = self.engine.run_suite(good_consent_df, "consent_suite", "test")
        assert result.score > 50
        assert result.total_checks == 12

    def test_consent_suite_dirty_data(self, bad_consent_df):
        result = self.engine.run_suite(bad_consent_df, "consent_suite", "test")
        assert result.failed_checks > 0

    def test_financial_suite_clean_data(self, good_financial_df):
        result = self.engine.run_suite(good_financial_df, "financial_suite", "test")
        assert result.score > 50
        assert result.total_checks == 12

    def test_suite_score_bounded(self, good_consent_df):
        result = self.engine.run_suite(good_consent_df, "consent_suite", "test")
        assert 0 <= result.score <= 100

    def test_suite_result_has_all_fields(self, good_consent_df):
        result = self.engine.run_suite(good_consent_df, "consent_suite", "test")
        assert result.dataset_name == "test"
        assert result.run_timestamp != ""
        assert isinstance(result.checks, list)
        assert result.duration_seconds >= 0

    def test_unknown_suite_falls_back_to_generic(self, good_consent_df):
        result = self.engine.run_suite(good_consent_df, "nonexistent_suite", "test")
        assert result.total_checks > 0


# ─────────────────────────────────────────────────────────────────────────────
# Metadata Tracker Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestMetadataTracker:

    def test_register_dataset(self, tracker, good_consent_df):
        meta = tracker.register_dataset(
            "test_ds", schema_version="1.0",
            owner="test@example.com", df=good_consent_df
        )
        assert meta.name == "test_ds"
        assert meta.row_count == len(good_consent_df)

    def test_log_run_persists(self, tracker):
        tracker.log_run(
            "test_ds", rows_ingested=1000, rows_passed=990,
            rows_rejected=10, quality_score=92.0, duration_seconds=1.5,
            checks_passed=10, checks_failed=2, critical_failures=0,
        )
        runs = tracker.get_pipeline_runs("test_ds")
        assert len(runs) >= 1
        assert float(runs.iloc[-1]["quality_score"]) == pytest.approx(92.0)

    def test_audit_log_records(self, tracker):
        tracker.log_audit("test_ds", "INGEST", rows_affected=5000)
        audit = tracker.get_audit_log(since_hours=1)
        assert len(audit) >= 1

    def test_summary_stats(self, tracker):
        tracker.register_dataset("ds1")
        tracker.log_run("ds1", 1000, 990, 10, 85.0, 1.0)
        stats = tracker.summary_stats()
        assert "total_datasets" in stats
        assert "avg_quality_score" in stats

    def test_quality_trend(self, tracker):
        for score in [80, 85, 90, 88, 92]:
            tracker.log_run("ds1", 1000, 990, 10, score, 1.0)
        trend = tracker.get_quality_trend("ds1")
        assert len(trend) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Integration Test
# ─────────────────────────────────────────────────────────────────────────────
class TestIntegration:

    def test_full_governance_pipeline(self, tmp_path, monkeypatch):
        """End-to-end: demo data → validation → metadata → report."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "audit").mkdir(parents=True)
        (tmp_path / "data" / "validated").mkdir(parents=True)
        (tmp_path / "data" / "rejected").mkdir(parents=True)
        (tmp_path / "data" / "processed").mkdir(parents=True)

        from pipeline.ingestion_pipeline import _make_demo_dataset
        from validation.data_quality_checks import DataQualityEngine
        from metadata.metadata_tracker import MetadataTracker

        tracker = MetadataTracker()
        engine  = DataQualityEngine()

        datasets = ["customer_consent", "financial_transactions", "user_profiles"]
        scores   = []

        for ds in datasets:
            df     = _make_demo_dataset(ds, n=500)
            suite  = {"customer_consent": "consent_suite",
                      "financial_transactions": "financial_suite",
                      "user_profiles": "profile_suite"}[ds]
            sr     = engine.run_suite(df, suite, ds)
            scores.append(sr.score)

            tracker.register_dataset(ds, df=df)
            tracker.log_run(ds, len(df), sr.rows_passed, sr.rows_rejected,
                            sr.score, sr.duration_seconds, sr.passed_checks,
                            sr.failed_checks, sr.critical_failures)

        assert len(scores) == 3
        assert all(0 <= s <= 100 for s in scores)

        stats = tracker.summary_stats()
        assert stats["total_datasets"] >= 3
        assert stats["total_runs"] >= 3

        print(f"\n✅ Integration test PASSED")
        print(f"   Datasets: {len(datasets)}")
        print(f"   Scores: {[f'{s:.1f}' for s in scores]}")
        print(f"   Audit events: {stats['audit_events_24h']}")

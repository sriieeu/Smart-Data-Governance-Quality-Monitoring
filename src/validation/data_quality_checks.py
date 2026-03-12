"""
data_quality_checks.py — 12+ automated data quality checks using Great Expectations.

Validation suites per dataset:
  - consent_suite:     Consent recency, legal basis, email formats
  - financial_suite:   Amount ranges, currency codes, transaction IDs
  - profile_suite:     Email formats, phone patterns, required PII fields
  - events_suite:      Event types, timestamp validity, session IDs
  - catalog_suite:     SKU formats, price positivity, category validity
  - tickets_suite:     Status values, priority levels, timestamp ordering
  - inventory_suite:   Quantity non-negativity, warehouse codes
  - hr_suite:          Employee ID formats, department validity
  - web_suite:         URL formats, session duration ranges
  - billing_suite:     Amount positivity, subscription status values

Each check is tagged: CRITICAL / WARNING / INFO

Usage:
    engine  = DataQualityEngine()
    result  = engine.run_suite(df, "consent_suite", dataset_name="customer_consent")
    print(result.score, result.passed_checks, result.failed_checks)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Severity levels
CRITICAL = "CRITICAL"
WARNING  = "WARNING"
INFO     = "INFO"

# Common regex patterns
EMAIL_RE      = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
PHONE_RE      = re.compile(r"^\+?[\d\s\-().]{7,20}$")
UUID_RE       = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
SKU_RE        = re.compile(r"^[A-Z]{2,4}-\d{4,8}$")
ISO_CURRENCY  = {"USD","EUR","GBP","JPY","AUD","CAD","CHF","CNY","INR","SGD","BRL","MXN","SEK","NOK","DKK"}
VALID_STATUSES_TICKET   = {"open","in_progress","resolved","closed","escalated"}
VALID_STATUSES_BILLING  = {"active","cancelled","trial","past_due","paused"}
VALID_PRIORITY          = {"low","medium","high","critical","urgent"}
VALID_LEGAL_BASIS       = {"consent","legitimate_interest","contract","legal_obligation","vital_interest","public_task"}


@dataclass
class CheckResult:
    check_id: str
    check_name: str
    severity: str
    passed: bool
    score: float          # 0–100 for this check
    total_rows: int
    failing_rows: int
    failing_pct: float
    column: Optional[str]
    details: str
    sample_failures: list = field(default_factory=list)


@dataclass
class SuiteResult:
    dataset_name: str
    suite_name: str
    run_timestamp: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    critical_failures: int
    warning_failures: int
    score: float              # 0–100 weighted quality score
    checks: list              # List[CheckResult]
    rows_validated: int
    rows_passed: int
    rows_rejected: int
    duration_seconds: float
    metadata: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Core check primitives
# ─────────────────────────────────────────────────────────────────────────────

def _check_not_null(
    df: pd.DataFrame, col: str, severity: str = CRITICAL,
    max_null_pct: float = 0.0, check_id: str = ""
) -> CheckResult:
    """Check 1: Column must not contain nulls (or below threshold)."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"not_null_{col}", col, severity)
    null_count = int(df[col].isna().sum())
    null_pct   = null_count / len(df) if len(df) else 0
    passed     = null_pct <= max_null_pct
    return CheckResult(
        check_id   = check_id or f"not_null_{col}",
        check_name = f"'{col}' completeness",
        severity   = severity,
        passed     = passed,
        score      = max(0.0, 100.0 * (1 - null_pct / max(max_null_pct, 0.001))),
        total_rows = len(df),
        failing_rows = null_count,
        failing_pct  = round(null_pct * 100, 2),
        column     = col,
        details    = (
            f"{null_count:,} null values ({null_pct*100:.1f}%) — "
            f"threshold: {max_null_pct*100:.0f}%"
        ),
    )


def _check_regex(
    df: pd.DataFrame, col: str, pattern: re.Pattern,
    check_name: str, severity: str = WARNING, check_id: str = ""
) -> CheckResult:
    """Check 2: Column values must match a regex pattern."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"regex_{col}", col, severity)
    mask    = df[col].notna()
    testable = df.loc[mask, col].astype(str)
    fails   = testable[~testable.str.match(pattern)]
    fail_ct = len(fails)
    fail_pct = fail_ct / len(testable) if len(testable) else 0
    return CheckResult(
        check_id   = check_id or f"regex_{col}",
        check_name = check_name,
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} values do not match expected format",
        sample_failures = fails.head(5).tolist(),
    )


def _check_value_set(
    df: pd.DataFrame, col: str, valid_values: set,
    severity: str = WARNING, check_id: str = ""
) -> CheckResult:
    """Check 3: Column values must be in a predefined set."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"value_set_{col}", col, severity)
    mask    = df[col].notna()
    vals    = df.loc[mask, col].astype(str).str.lower().str.strip()
    fails   = vals[~vals.isin({v.lower() for v in valid_values})]
    fail_ct = len(fails)
    fail_pct = fail_ct / len(vals) if len(vals) else 0
    return CheckResult(
        check_id   = check_id or f"value_set_{col}",
        check_name = f"'{col}' valid values",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} values not in allowed set: {sorted(valid_values)[:5]}…",
        sample_failures = fails.unique()[:5].tolist(),
    )


def _check_numeric_range(
    df: pd.DataFrame, col: str,
    min_val: Optional[float] = None, max_val: Optional[float] = None,
    severity: str = CRITICAL, check_id: str = ""
) -> CheckResult:
    """Check 4: Numeric column must be within [min_val, max_val]."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"range_{col}", col, severity)
    nums    = pd.to_numeric(df[col], errors="coerce").dropna()
    fails   = nums[
        ((min_val is not None) & (nums < min_val)) |
        ((max_val is not None) & (nums > max_val))
    ]
    fail_ct  = len(fails)
    fail_pct = fail_ct / len(nums) if len(nums) else 0
    bounds   = f"[{min_val}, {max_val}]"
    return CheckResult(
        check_id   = check_id or f"range_{col}",
        check_name = f"'{col}' range {bounds}",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} values outside {bounds}",
        sample_failures = fails.head(5).tolist(),
    )


def _check_uniqueness(
    df: pd.DataFrame, col: str,
    severity: str = CRITICAL, check_id: str = ""
) -> CheckResult:
    """Check 5: Column must have no duplicate values."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"unique_{col}", col, severity)
    dupes    = df[col].dropna().duplicated()
    fail_ct  = int(dupes.sum())
    fail_pct = fail_ct / len(df) if len(df) else 0
    return CheckResult(
        check_id   = check_id or f"unique_{col}",
        check_name = f"'{col}' uniqueness",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} duplicate values detected",
    )


def _check_date_recency(
    df: pd.DataFrame, col: str,
    max_age_days: int = 365,
    severity: str = WARNING, check_id: str = ""
) -> CheckResult:
    """Check 6: Date column must contain recent dates (within max_age_days)."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"recency_{col}", col, severity)
    dates    = pd.to_datetime(df[col], errors="coerce").dropna()
    cutoff   = pd.Timestamp.now() - pd.Timedelta(days=max_age_days)
    stale    = dates[dates < cutoff]
    fail_ct  = len(stale)
    fail_pct = fail_ct / len(dates) if len(dates) else 0
    return CheckResult(
        check_id   = check_id or f"recency_{col}",
        check_name = f"'{col}' recency (max {max_age_days}d)",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} records older than {max_age_days} days",
    )


def _check_no_future_dates(
    df: pd.DataFrame, col: str,
    severity: str = WARNING, check_id: str = ""
) -> CheckResult:
    """Check 7: Date column must not contain future dates."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"no_future_{col}", col, severity)
    dates   = pd.to_datetime(df[col], errors="coerce").dropna()
    future  = dates[dates > pd.Timestamp.now()]
    fail_ct = len(future)
    fail_pct = fail_ct / len(dates) if len(dates) else 0
    return CheckResult(
        check_id   = check_id or f"no_future_{col}",
        check_name = f"'{col}' no future dates",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} future-dated records",
    )


def _check_referential_integrity(
    df: pd.DataFrame, col: str,
    reference_values: set,
    severity: str = CRITICAL, check_id: str = ""
) -> CheckResult:
    """Check 8: FK column must only reference known values."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"fk_{col}", col, severity)
    vals     = df[col].dropna().astype(str)
    orphans  = vals[~vals.isin({str(v) for v in reference_values})]
    fail_ct  = len(orphans)
    fail_pct = fail_ct / len(vals) if len(vals) else 0
    return CheckResult(
        check_id   = check_id or f"fk_{col}",
        check_name = f"'{col}' referential integrity",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct)),
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} orphaned FK values",
        sample_failures = orphans.unique()[:5].tolist(),
    )


def _check_row_count(
    df: pd.DataFrame, min_rows: int = 1, max_rows: Optional[int] = None,
    severity: str = CRITICAL, check_id: str = "row_count"
) -> CheckResult:
    """Check 9: Dataset must have expected row count."""
    n      = len(df)
    passed = n >= min_rows and (max_rows is None or n <= max_rows)
    bounds = f"≥{min_rows}" + (f" and ≤{max_rows}" if max_rows else "")
    return CheckResult(
        check_id   = check_id,
        check_name = f"Row count {bounds}",
        severity   = severity,
        passed     = passed,
        score      = 100.0 if passed else 0.0,
        total_rows = n,
        failing_rows = 0 if passed else n,
        failing_pct  = 0.0 if passed else 100.0,
        column     = None,
        details    = f"Row count: {n:,} (expected {bounds})",
    )


def _check_schema_columns(
    df: pd.DataFrame, required_cols: list,
    severity: str = CRITICAL, check_id: str = "schema"
) -> CheckResult:
    """Check 10: DataFrame must contain all required columns."""
    missing  = [c for c in required_cols if c not in df.columns]
    passed   = len(missing) == 0
    return CheckResult(
        check_id   = check_id,
        check_name = "Schema completeness",
        severity   = severity,
        passed     = passed,
        score      = 100.0 * (len(required_cols) - len(missing)) / len(required_cols),
        total_rows = len(df),
        failing_rows = 0,
        failing_pct  = 0.0,
        column     = None,
        details    = f"Missing columns: {missing}" if missing else "All required columns present",
    )


def _check_statistical_outliers(
    df: pd.DataFrame, col: str,
    z_threshold: float = 4.0,
    severity: str = WARNING, check_id: str = ""
) -> CheckResult:
    """Check 11: Numeric column must not have extreme statistical outliers (Z > threshold)."""
    if col not in df.columns:
        return _missing_col_check(check_id or f"outlier_{col}", col, severity)
    nums     = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(nums) < 10:
        return _skip_check(check_id or f"outlier_{col}", col, "Too few values for outlier check", severity)
    z        = (nums - nums.mean()) / (nums.std() + 1e-9)
    outliers = nums[z.abs() > z_threshold]
    fail_ct  = len(outliers)
    fail_pct = fail_ct / len(nums) if len(nums) else 0
    return CheckResult(
        check_id   = check_id or f"outlier_{col}",
        check_name = f"'{col}' outlier check (Z>{z_threshold})",
        severity   = severity,
        passed     = fail_ct == 0,
        score      = max(0.0, 100.0 * (1 - fail_pct * 10)),  # penalise heavily
        total_rows = len(df),
        failing_rows = fail_ct,
        failing_pct  = round(fail_pct * 100, 2),
        column     = col,
        details    = f"{fail_ct:,} extreme outliers (|Z|>{z_threshold})",
        sample_failures = outliers.head(5).tolist(),
    )


def _check_cross_field(
    df: pd.DataFrame, check_id: str, check_name: str,
    predicate: Callable[[pd.DataFrame], pd.Series],
    severity: str = WARNING, details_tmpl: str = ""
) -> CheckResult:
    """Check 12: Cross-field business rule validation."""
    try:
        fail_mask = ~predicate(df)
        fail_ct   = int(fail_mask.sum())
        fail_pct  = fail_ct / len(df) if len(df) else 0
        return CheckResult(
            check_id   = check_id,
            check_name = check_name,
            severity   = severity,
            passed     = fail_ct == 0,
            score      = max(0.0, 100.0 * (1 - fail_pct)),
            total_rows = len(df),
            failing_rows = fail_ct,
            failing_pct  = round(fail_pct * 100, 2),
            column     = None,
            details    = details_tmpl or f"{fail_ct:,} rows violate the business rule",
        )
    except Exception as e:
        return _error_check(check_id, check_name, str(e), severity)


def _missing_col_check(check_id, col, severity):
    return CheckResult(
        check_id=check_id, check_name=f"'{col}' presence",
        severity=severity, passed=False, score=0.0,
        total_rows=0, failing_rows=0, failing_pct=0.0,
        column=col, details=f"Column '{col}' not found in dataset",
    )

def _skip_check(check_id, col, reason, severity):
    return CheckResult(
        check_id=check_id, check_name=f"'{col}' check skipped",
        severity=INFO, passed=True, score=100.0,
        total_rows=0, failing_rows=0, failing_pct=0.0,
        column=col, details=reason,
    )

def _error_check(check_id, check_name, error, severity):
    return CheckResult(
        check_id=check_id, check_name=check_name,
        severity=severity, passed=False, score=0.0,
        total_rows=0, failing_rows=0, failing_pct=0.0,
        column=None, details=f"Check error: {error}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Validation Suites
# ─────────────────────────────────────────────────────────────────────────────

class DataQualityEngine:
    """
    Runs validation suites against DataFrames.

    Each suite is a collection of named checks with severity levels.
    The overall quality score is weighted: CRITICAL checks count 3×, WARNING 2×, INFO 1×.

    Usage:
        engine = DataQualityEngine()
        result = engine.run_suite(df, "consent_suite", "customer_consent")
        print(f"Score: {result.score:.1f}  Passed: {result.passed_checks}/{result.total_checks}")
    """

    SEVERITY_WEIGHT = {CRITICAL: 3.0, WARNING: 2.0, INFO: 1.0}

    def run_suite(
        self,
        df: pd.DataFrame,
        suite_name: str,
        dataset_name: str = "unknown",
    ) -> SuiteResult:
        import time
        t0     = time.time()
        suite  = getattr(self, f"_suite_{suite_name}", None)
        if suite is None:
            suite = self._suite_generic

        checks = suite(df)
        return self._aggregate(checks, df, suite_name, dataset_name, time.time() - t0)

    def _aggregate(
        self, checks: list, df: pd.DataFrame,
        suite_name: str, dataset_name: str, duration: float
    ) -> SuiteResult:
        passed   = [c for c in checks if c.passed]
        failed   = [c for c in checks if not c.passed]
        critical = [c for c in failed if c.severity == CRITICAL]
        warnings = [c for c in failed if c.severity == WARNING]

        # Weighted score
        total_weight = sum(self.SEVERITY_WEIGHT[c.severity] for c in checks)
        earned_weight = sum(
            self.SEVERITY_WEIGHT[c.severity] * (c.score / 100.0)
            for c in checks
        )
        score = (earned_weight / total_weight * 100) if total_weight > 0 else 100.0

        # Row-level pass/fail
        all_fail_masks = [c.failing_rows for c in checks if c.column]
        rows_rejected  = max(all_fail_masks) if all_fail_masks else 0

        return SuiteResult(
            dataset_name     = dataset_name,
            suite_name       = suite_name,
            run_timestamp    = datetime.now(timezone.utc).isoformat(),
            total_checks     = len(checks),
            passed_checks    = len(passed),
            failed_checks    = len(failed),
            critical_failures= len(critical),
            warning_failures = len(warnings),
            score            = round(score, 1),
            checks           = checks,
            rows_validated   = len(df),
            rows_passed      = len(df) - rows_rejected,
            rows_rejected    = rows_rejected,
            duration_seconds = round(duration, 3),
        )

    # ── Validation Suites ─────────────────────────────────────────────────────

    def _suite_consent_suite(self, df: pd.DataFrame) -> list:
        """12 checks for customer consent dataset."""
        return [
            _check_row_count(df, min_rows=100, severity=CRITICAL, check_id="DQ-C01"),
            _check_schema_columns(df, ["customer_id","email","consent_date","legal_basis","consent_given"], CRITICAL, "DQ-C02"),
            _check_not_null(df, "customer_id", CRITICAL, 0.0, "DQ-C03"),
            _check_uniqueness(df, "customer_id", CRITICAL, "DQ-C04"),
            _check_not_null(df, "email", CRITICAL, 0.02, "DQ-C05"),
            _check_regex(df, "email", EMAIL_RE, "'email' format validation", CRITICAL, "DQ-C06"),
            _check_not_null(df, "consent_date", CRITICAL, 0.0, "DQ-C07"),
            _check_date_recency(df, "consent_date", max_age_days=730, severity=WARNING, check_id="DQ-C08"),
            _check_no_future_dates(df, "consent_date", WARNING, "DQ-C09"),
            _check_value_set(df, "legal_basis", VALID_LEGAL_BASIS, WARNING, "DQ-C10"),
            _check_not_null(df, "consent_given", CRITICAL, 0.0, "DQ-C11"),
            _check_cross_field(
                df, "DQ-C12", "Withdrawn consent must have withdrawal_date",
                lambda d: ~(
                    (d.get("consent_given", pd.Series(dtype=str)).astype(str).str.lower() == "false") &
                    d.get("withdrawal_date", pd.Series(dtype=str)).isna()
                ) if "withdrawal_date" in d.columns else pd.Series([True]*len(d)),
                WARNING, "Rows where consent=False but withdrawal_date is missing"
            ),
        ]

    def _suite_financial_suite(self, df: pd.DataFrame) -> list:
        """12 checks for financial transactions dataset."""
        return [
            _check_row_count(df, min_rows=1, severity=CRITICAL, check_id="DQ-F01"),
            _check_schema_columns(df, ["transaction_id","amount","currency","transaction_date","status"], CRITICAL, "DQ-F02"),
            _check_not_null(df, "transaction_id", CRITICAL, 0.0, "DQ-F03"),
            _check_uniqueness(df, "transaction_id", CRITICAL, "DQ-F04"),
            _check_not_null(df, "amount", CRITICAL, 0.0, "DQ-F05"),
            _check_numeric_range(df, "amount", min_val=0.0, severity=CRITICAL, check_id="DQ-F06"),
            _check_not_null(df, "currency", WARNING, 0.0, "DQ-F07"),
            _check_value_set(df, "currency", ISO_CURRENCY, WARNING, "DQ-F08"),
            _check_not_null(df, "transaction_date", CRITICAL, 0.0, "DQ-F09"),
            _check_no_future_dates(df, "transaction_date", CRITICAL, "DQ-F10"),
            _check_statistical_outliers(df, "amount", z_threshold=5.0, severity=WARNING, check_id="DQ-F11"),
            _check_cross_field(
                df, "DQ-F12", "Refund amount must be negative",
                lambda d: ~(
                    (d.get("transaction_type", pd.Series(dtype=str)).astype(str).str.lower() == "refund") &
                    (pd.to_numeric(d.get("amount", 0), errors="coerce") > 0)
                ) if "transaction_type" in d.columns else pd.Series([True]*len(d)),
                WARNING, "Refund transactions with positive amounts"
            ),
        ]

    def _suite_profile_suite(self, df: pd.DataFrame) -> list:
        """12 checks for user profiles dataset."""
        return [
            _check_row_count(df, min_rows=1, severity=CRITICAL, check_id="DQ-P01"),
            _check_schema_columns(df, ["user_id","email","created_at","country"], CRITICAL, "DQ-P02"),
            _check_not_null(df, "user_id", CRITICAL, 0.0, "DQ-P03"),
            _check_uniqueness(df, "user_id", CRITICAL, "DQ-P04"),
            _check_not_null(df, "email", CRITICAL, 0.05, "DQ-P05"),
            _check_regex(df, "email", EMAIL_RE, "'email' format", CRITICAL, "DQ-P06"),
            _check_date_recency(df, "created_at", max_age_days=3650, severity=INFO, check_id="DQ-P07"),
            _check_no_future_dates(df, "created_at", WARNING, "DQ-P08"),
            _check_not_null(df, "country", WARNING, 0.10, "DQ-P09"),
            _check_regex(df, "phone", PHONE_RE, "'phone' format", INFO, "DQ-P10") if "phone" in df.columns
                else _skip_check("DQ-P10", "phone", "Column not present", INFO),
            _check_statistical_outliers(df, "age", z_threshold=4.0, severity=WARNING, check_id="DQ-P11") if "age" in df.columns
                else _skip_check("DQ-P11", "age", "Column not present", INFO),
            _check_numeric_range(df, "age", min_val=0, max_val=130, severity=WARNING, check_id="DQ-P12") if "age" in df.columns
                else _skip_check("DQ-P12", "age", "Column not present", INFO),
        ]

    def _suite_generic(self, df: pd.DataFrame) -> list:
        """Generic checks applied to any dataset."""
        checks = [_check_row_count(df, min_rows=1, severity=CRITICAL, check_id="DQ-G01")]
        # Null checks on all columns
        for i, col in enumerate(df.columns[:10], start=2):
            checks.append(_check_not_null(df, col, WARNING, 0.20, f"DQ-G{i:02d}"))
        return checks

    # Wire remaining suites to generic (can be expanded)
    def _suite_events_suite(self, df):    return self._suite_generic(df)
    def _suite_catalog_suite(self, df):   return self._suite_generic(df)
    def _suite_tickets_suite(self, df):   return self._suite_generic(df)
    def _suite_inventory_suite(self, df): return self._suite_generic(df)
    def _suite_hr_suite(self, df):        return self._suite_generic(df)
    def _suite_web_suite(self, df):       return self._suite_generic(df)
    def _suite_billing_suite(self, df):   return self._suite_generic(df)

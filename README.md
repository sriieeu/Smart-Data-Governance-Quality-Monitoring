# 🛡️ Smart Data Governance & Quality Monitoring Platform

> A production-grade data governance pipeline built on Microsoft Fabric Lakehouse architecture — automated validation, metadata tracking, audit logging, and Power BI dashboard integration for 10+ structured datasets.

## Architecture

```
data-governance-platform/
├── src/
│   ├── fabric/
│   │   └── fabric_client.py          # Microsoft Fabric / OneLake connector (ABFSS)
│   ├── validation/
│   │   └── data_quality_checks.py    # 12+ Great Expectations–style checks
│   ├── metadata/
│   │   └── metadata_tracker.py       # Dataset catalog, audit log, lineage tracking
│   ├── pipeline/
│   │   └── ingestion_pipeline.py     # Full orchestration pipeline
│   ├── reporting/
│   │   └── powerbi_exporter.py       # Power BI–ready CSV export + data dictionary
│   └── ui/
│       └── app.py                    # Streamlit dashboard (Industrial Precision theme)
├── tests/
│   └── test_governance.py            # 25 pytest tests
├── data/
│   ├── raw/                          # Source files
│   ├── validated/                    # Clean output
│   ├── rejected/                     # Quarantined rows
│   └── audit/                        # Audit log JSON-Lines
├── config.yaml                       # All settings: datasets, validation thresholds, SLAs
└── requirements.txt
```

## Setup

```bash
pip install -r requirements.txt
streamlit run src/ui/app.py
pytest tests/ -v
```

## Data Quality Checks (12+)

| Check ID | Check | Severity |
|----------|-------|----------|
| DQ-C01 | Row count ≥ minimum | CRITICAL |
| DQ-C02 | Schema completeness | CRITICAL |
| DQ-C03 | Primary key not null | CRITICAL |
| DQ-C04 | Primary key uniqueness | CRITICAL |
| DQ-C05 | Email completeness | CRITICAL |
| DQ-C06 | Email format (regex) | CRITICAL |
| DQ-C07 | Consent date not null | CRITICAL |
| DQ-C08 | Consent recency (≤2yr) | WARNING |
| DQ-C09 | No future dates | WARNING |
| DQ-C10 | Valid legal basis values | WARNING |
| DQ-C11 | consent_given not null | CRITICAL |
| DQ-C12 | Cross-field: withdrawal date | WARNING |

## Microsoft Fabric Integration

The platform targets Microsoft Fabric Lakehouse via the OneLake ABFSS protocol:

```
abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/
```

In a Fabric Notebook: uses `notebookutils` for zero-config auth.
In local/CI: uses `DefaultAzureCredential` (service principal / CLI / managed identity).

```python
# In Fabric Notebook
from src.fabric.fabric_client import FabricClient
client = FabricClient.from_config("config.yaml")
client.write_table(validated_df, "governance_customer_consent_validated")
```

## Power BI Dashboard

The exporter produces 4 linked tables:

| Table | Purpose |
|-------|---------|
| `governance_datasets.csv` | Dataset catalog with sensitivity and ownership |
| `governance_pipeline_runs.csv` | Per-run quality scores and row counts |
| `governance_kpi_summary.csv` | Pre-aggregated KPIs for dashboard tiles |
| `governance_audit_log.csv` | Full immutable audit trail |

**Key DAX Measures:**
```dax
Avg Quality Score   = AVERAGE(pipeline_runs[quality_score])
Rejection Rate      = DIVIDE(SUM([rows_rejected]), SUM([rows_ingested]))
High Risk Datasets  = COUNTROWS(FILTER(kpi_summary, [latest_score] < 80))
Failed Runs 24h     = COUNTROWS(FILTER(pipeline_runs, [status]="failed"))
```

## Tech Stack

`Python` · `Microsoft Fabric` · `Azure Data Lake Storage Gen2` · `Great Expectations` · `pandas` · `pydantic` · `Streamlit` · `Plotly` · `Power BI`

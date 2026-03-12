#  Smart Data Governance & Quality Monitoring Platform

Modern organizations generate and store large volumes of data from multiple sources such as business applications, operational systems, and analytics platforms. As the amount of data grows, ensuring that the data remains accurate, reliable, and well-managed becomes increasingly important. Poor data quality can lead to incorrect business decisions, reporting errors, and compliance issues.

A Smart Data Governance and Quality Monitoring Platform is designed to manage and monitor datasets in a structured data environment. The platform ensures that datasets follow predefined quality standards, maintains metadata about the data, tracks data lineage, and records system activities through audit logs. It also integrates reporting tools so that stakeholders can monitor data quality and governance metrics in real time.

The system is implemented using the Microsoft Fabric Lakehouse architecture, which combines the capabilities of a data lake and a data warehouse. This architecture allows organizations to store large datasets efficiently while also supporting structured analytics and governance processes.

The main objectives of the Smart Data Governance and Quality Monitoring Platform include:
Ensure high data quality across multiple structured datasets.
Maintain metadata information such as dataset ownership and schema details.
Monitor and validate incoming data automatically.
Track data processing activities through audit logs.
Provide dashboards that visualize data quality metrics.

Data governance refers to the policies, processes, and technologies used to manage data availability, usability, integrity, and security.

A data governance framework typically includes:
Data ownership definitions ,Data quality standards , Metadata management , Data lineage tracking  and Compliance monitoring



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

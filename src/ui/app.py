"""
app.py — Smart Data Governance & Quality Monitoring Platform
Streamlit Dashboard — Industrial Precision Theme

Run:  streamlit run src/ui/app.py
"""

import sys, logging
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Data Governance Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme: Industrial Precision ───────────────────────────────────────────────
# Dark graphite base · Electric cyan accent · Sharp monospace typography
# Inspired by Bloomberg Terminal + industrial control room dashboards
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@600;700;800&display=swap');

:root {
    --bg:           #0d0f12;
    --bg-panel:     #141720;
    --bg-card:      #1a1e27;
    --bg-hover:     #1f2433;
    --border:       #252a38;
    --border-bright:#2e3548;
    --cyan:         #00d4ff;
    --cyan-dim:     #0099cc;
    --cyan-glow:    rgba(0,212,255,0.12);
    --amber:        #ffb347;
    --amber-glow:   rgba(255,179,71,0.10);
    --green:        #00e676;
    --green-glow:   rgba(0,230,118,0.10);
    --red:          #ff4757;
    --red-glow:     rgba(255,71,87,0.10);
    --text:         #e8eaf0;
    --text-dim:     #8892a4;
    --text-faint:   #3d4456;
    --critical:     #ff4757;
    --high:         #ff6b35;
    --medium:       #ffb347;
    --low:          #00e676;
    --none:         #3d4456;
}

html, body, [class*="css"] {
    font-family: 'Space Mono', monospace !important;
    background: var(--bg) !important;
    color: var(--text) !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: var(--bg-panel) !important;
    border-right: 1px solid var(--border-bright) !important;
    width: 240px !important;
}
section[data-testid="stSidebar"] * { color: var(--text-dim) !important; }

.brand {
    padding: 28px 20px 20px;
    border-bottom: 1px solid var(--border-bright);
    margin-bottom: 6px;
}
.brand-icon { font-size: 22px; margin-bottom: 8px; }
.brand-name {
    font-family: 'Syne', sans-serif;
    font-size: 13px; font-weight: 800;
    color: var(--cyan) !important;
    letter-spacing: 0.08em; text-transform: uppercase;
    line-height: 1.3;
}
.brand-sub {
    font-family: 'Space Mono', monospace;
    font-size: 9px; color: var(--text-faint) !important;
    letter-spacing: 0.14em; text-transform: uppercase; margin-top: 4px;
}

section[data-testid="stSidebar"] .stRadio > label { display: none; }
section[data-testid="stSidebar"] .stRadio label {
    padding: 9px 20px !important; border-radius: 0 !important;
    font-family: 'Space Mono', monospace !important; font-size: 11px !important;
    color: var(--text-dim) !important; cursor: pointer;
    border-left: 2px solid transparent !important;
    transition: all 0.12s;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    background: var(--bg-card) !important; color: var(--text) !important;
    border-left-color: var(--border-bright) !important;
}
section[data-testid="stSidebar"] label:has(input:checked) {
    background: var(--cyan-glow) !important; color: var(--cyan) !important;
    border-left-color: var(--cyan) !important;
}

.sidebar-section {
    padding: 16px 20px 4px;
    font-family: 'Space Mono', monospace;
    font-size: 8.5px; letter-spacing: 0.2em;
    text-transform: uppercase; color: var(--text-faint) !important;
    border-top: 1px solid var(--border); margin-top: 8px;
}
.sidebar-stat {
    padding: 6px 20px; display: flex; justify-content: space-between;
    align-items: center;
}
.sidebar-stat-label { font-size: 10px; color: var(--text-dim) !important; }
.sidebar-stat-val   { font-size: 11px; color: var(--cyan) !important; font-weight: 700; }

/* ── Main ── */
.main { background: var(--bg) !important; }
.main .block-container { padding: 0 44px 60px; max-width: 1400px; }

/* ── Page header ── */
.page-header {
    padding: 32px 0 24px; border-bottom: 1px solid var(--border);
    margin-bottom: 32px;
    display: flex; justify-content: space-between; align-items: flex-end;
}
.page-title {
    font-family: 'Syne', sans-serif; font-size: 26px; font-weight: 800;
    color: var(--text); letter-spacing: -0.01em; line-height: 1;
    margin-bottom: 6px;
}
.page-title .accent { color: var(--cyan); }
.page-sub {
    font-family: 'Space Mono', monospace; font-size: 10.5px;
    color: var(--text-dim); line-height: 1.75; letter-spacing: 0.02em;
}
.page-timestamp {
    font-family: 'Space Mono', monospace; font-size: 10px;
    color: var(--text-faint); text-align: right; line-height: 1.75;
}

/* ── Stat tiles ── */
.kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 28px; }
.kpi-tile {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 4px; padding: 16px 18px; position: relative; overflow: hidden;
}
.kpi-tile::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--accent-color, var(--cyan));
}
.kpi-val {
    font-family: 'Syne', sans-serif; font-size: 30px; font-weight: 800;
    line-height: 1; margin-bottom: 4px; color: var(--accent-color, var(--cyan));
}
.kpi-label {
    font-family: 'Space Mono', monospace; font-size: 9px;
    color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.14em;
}
.kpi-sub {
    font-family: 'Space Mono', monospace; font-size: 9px;
    color: var(--text-faint); margin-top: 6px;
}

/* ── Section label ── */
.sec-label {
    font-family: 'Space Mono', monospace; font-size: 9px; font-weight: 700;
    letter-spacing: 0.2em; text-transform: uppercase; color: var(--text-faint);
    margin-bottom: 10px; display: flex; align-items: center; gap: 12px;
}
.sec-label::after { content: ''; flex: 1; height: 1px; background: var(--border); }

/* ── Score badge ── */
.score-badge {
    display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px;
    border-radius: 2px; font-family: 'Space Mono', monospace; font-size: 10px;
    font-weight: 700; border: 1px solid; text-transform: uppercase; letter-spacing: 0.06em;
}
.score-critical { background: var(--red-glow);   color: var(--red);   border-color: var(--red);   }
.score-high     { background: var(--amber-glow);  color: var(--amber); border-color: var(--amber); }
.score-medium   { background: var(--amber-glow);  color: var(--amber); border-color: var(--amber); }
.score-low      { background: var(--green-glow);  color: var(--green); border-color: var(--green); }
.score-none     { background: var(--bg-hover);    color: var(--text-faint); border-color: var(--border); }

/* ── Check row ── */
.check-row {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 3px; padding: 10px 14px; margin: 4px 0;
    display: flex; justify-content: space-between; align-items: center;
    font-family: 'Space Mono', monospace; font-size: 11px;
}
.check-row:hover { border-color: var(--border-bright); background: var(--bg-hover); }
.check-row.fail-critical { border-left: 3px solid var(--red); }
.check-row.fail-warning  { border-left: 3px solid var(--amber); }
.check-row.pass          { border-left: 3px solid var(--green); }
.check-id    { color: var(--cyan); font-size: 9.5px; min-width: 70px; }
.check-name  { color: var(--text); flex: 1; padding: 0 12px; }
.check-score { color: var(--text-dim); font-size: 10px; min-width: 50px; text-align: right; }
.check-fail  { color: var(--text-faint); font-size: 9.5px; min-width: 60px; text-align: right; }
.check-dot   { width: 7px; height: 7px; border-radius: 50%; min-width: 7px; }
.dot-pass     { background: var(--green); }
.dot-critical { background: var(--red); box-shadow: 0 0 6px var(--red); }
.dot-warning  { background: var(--amber); }
.dot-info     { background: var(--cyan-dim); }

/* ── Dataset card ── */
.ds-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 4px; padding: 14px 16px; margin: 6px 0;
    cursor: pointer; transition: all 0.12s;
}
.ds-card:hover { border-color: var(--border-bright); background: var(--bg-hover); }
.ds-card-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
.ds-name { font-family: 'Syne', sans-serif; font-size: 13px; font-weight: 700; color: var(--text); }
.ds-meta { font-size: 9.5px; color: var(--text-faint); }
.ds-bar-track { background: var(--border); border-radius: 1px; height: 3px; margin-top: 8px; }
.ds-bar-fill  { height: 3px; border-radius: 1px; transition: width 0.3s; }

/* ── Audit row ── */
.audit-row {
    border-bottom: 1px solid var(--border); padding: 8px 0;
    font-family: 'Space Mono', monospace; font-size: 10.5px;
    display: grid; grid-template-columns: 140px 80px 120px 1fr;
    gap: 12px; align-items: center; color: var(--text-dim);
}
.audit-row:hover { color: var(--text); }
.audit-ts { color: var(--text-faint); font-size: 9.5px; }
.audit-action { color: var(--cyan); font-size: 9.5px; font-weight: 700; letter-spacing: 0.06em; }
.audit-sev-INFO     { color: var(--text-faint); }
.audit-sev-WARNING  { color: var(--amber); }
.audit-sev-CRITICAL { color: var(--red); }

/* ── Streamlit overrides ── */
.stButton > button {
    background: var(--cyan) !important; color: var(--bg) !important;
    border: none !important; border-radius: 2px !important;
    font-family: 'Space Mono', monospace !important; font-size: 11px !important;
    font-weight: 700 !important; padding: 9px 22px !important;
    letter-spacing: 0.08em !important; text-transform: uppercase !important;
}
.stButton > button:hover { background: var(--cyan-dim) !important; }
.stSelectbox > div > div, .stTextInput input, .stMultiSelect > div > div {
    background: var(--bg-card) !important; border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-family: 'Space Mono', monospace !important; font-size: 11px !important;
}
.stAlert { border-radius: 3px !important; font-family: 'Space Mono', monospace !important; font-size: 11px !important; }
div[data-testid="stMetricValue"] { font-family: 'Syne', sans-serif !important; font-size: 28px !important; color: var(--cyan) !important; }
div[data-testid="stMetricLabel"] { font-family: 'Space Mono', monospace !important; font-size: 9px !important; letter-spacing: 0.14em !important; text-transform: uppercase !important; color: var(--text-dim) !important; }
#MainMenu, footer, header { visibility: hidden; }
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg-panel); }
::-webkit-scrollbar-thumb { background: var(--border-bright); border-radius: 2px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Session state + pipeline bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def init():
    for k, v in {
        "pipeline_ran": False,
        "report": None,
        "tracker": None,
        "suite_results": {},
        "selected_dataset": None,
    }.items():
        if k not in st.session_state:
            st.session_state[k] = v
init()


@st.cache_resource(show_spinner=False)
def get_pipeline():
    from pipeline.ingestion_pipeline import GovernancePipeline
    from metadata.metadata_tracker   import MetadataTracker
    return GovernancePipeline(), MetadataTracker()


def score_color(s):
    if s >= 90: return "var(--green)"
    if s >= 75: return "var(--cyan)"
    if s >= 60: return "var(--amber)"
    return "var(--red)"

def risk_label(s):
    if s >= 90: return ("low",    "✓ HEALTHY")
    if s >= 75: return ("medium", "⚠ MONITOR")
    if s >= 60: return ("high",   "⚡ DEGRADED")
    return ("critical", "✕ CRITICAL")


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="brand">
        <div class="brand-icon">🛡️</div>
        <div class="brand-name">Data<br>Governance</div>
        <div class="brand-sub">Quality Platform v2.0</div>
    </div>
    """, unsafe_allow_html=True)

    nav = st.radio("", [
        "Pipeline Control",
        "Dataset Catalog",
        "Quality Checks",
        "Audit Log",
        "Power BI Export",
    ])

    # Live stats
    tracker = st.session_state.tracker
    if tracker:
        stats = tracker.summary_stats()
        st.markdown('<div class="sidebar-section">Live Stats</div>', unsafe_allow_html=True)
        for label, key in [
            ("Datasets",    "total_datasets"),
            ("Runs",        "total_runs"),
            ("Avg Score",   "avg_quality_score"),
            ("Audit 24h",   "audit_events_24h"),
        ]:
            val = stats.get(key, "—")
            if isinstance(val, float):
                val = f"{val:.1f}"
            st.markdown(f"""
            <div class="sidebar-stat">
                <span class="sidebar-stat-label">{label}</span>
                <span class="sidebar-stat-val">{val}</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div class="sidebar-section">Run Pipeline to Begin</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(val, label, sub="", accent="var(--cyan)"):
    st.markdown(f"""
    <div class="kpi-tile" style="--accent-color:{accent};">
        <div class="kpi-val">{val}</div>
        <div class="kpi-label">{label}</div>
        {"<div class='kpi-sub'>"+sub+"</div>" if sub else ""}
    </div>""", unsafe_allow_html=True)

def sec_label(text):
    st.markdown(f'<div class="sec-label">{text}</div>', unsafe_allow_html=True)

def divider():
    st.markdown('<div style="height:1px;background:var(--border);margin:24px 0;"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1: Pipeline Control
# ─────────────────────────────────────────────────────────────────────────────
if nav == "Pipeline Control":
    from datetime import datetime, timezone
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    st.markdown(f"""
    <div class="page-header">
        <div>
            <div class="page-title">Pipeline <span class="accent">Control</span></div>
            <div class="page-sub">Orchestrate governance pipeline · 10+ datasets · Microsoft Fabric Lakehouse</div>
        </div>
        <div class="page-timestamp">{now_str}<br>All times UTC</div>
    </div>
    """, unsafe_allow_html=True)

    # Config
    col_l, col_r = st.columns([3, 2], gap="large")
    with col_l:
        sec_label("Dataset Selection")
        DATASETS = [
            "customer_consent", "financial_transactions", "user_profiles",
            "campaign_events", "product_catalog", "support_tickets",
            "inventory_levels", "employee_records", "web_analytics",
            "subscription_billing",
        ]
        selected = st.multiselect(
            "datasets", DATASETS, default=DATASETS[:5],
            label_visibility="collapsed",
        )
        use_demo = st.checkbox("Use built-in demo data (no files needed)", value=True)

    with col_r:
        st.markdown("""
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:4px;padding:18px 20px;">
            <div style="font-family:'Syne',sans-serif;font-size:13px;font-weight:700;color:var(--text);margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border);">
                Validation Config
            </div>
        """, unsafe_allow_html=True)
        fail_on_critical = st.checkbox("Fail on CRITICAL checks", value=True)
        alert_threshold  = st.slider("Alert threshold (score)", 50, 95, 80)
        st.markdown("</div>", unsafe_allow_html=True)

    divider()
    run_btn = st.button("▶  Run Pipeline", type="primary")

    if run_btn and selected:
        pipeline, tracker_obj = get_pipeline()
        st.session_state.tracker = tracker_obj

        suite_map = {
            "customer_consent":        "consent_suite",
            "financial_transactions":  "financial_suite",
            "user_profiles":           "profile_suite",
        }

        results = []
        all_suite_results = {}

        progress = st.progress(0)
        status_box = st.empty()

        for i, ds_name in enumerate(selected):
            status_box.info(f"Processing: **{ds_name}** ({i+1}/{len(selected)})…")
            progress.progress((i + 0.5) / len(selected))

            from pipeline.ingestion_pipeline import _make_demo_dataset
            from validation.data_quality_checks import DataQualityEngine

            df = _make_demo_dataset(ds_name, n=3000)
            engine = DataQualityEngine()
            suite  = suite_map.get(ds_name, "generic")
            sr     = engine.run_suite(df, suite, ds_name)

            all_suite_results[ds_name] = sr

            # Log to tracker
            tracker_obj.register_dataset(
                name=ds_name, schema_version="1.0",
                owner=f"{ds_name.split('_')[0]}@company.com",
                sensitivity="INTERNAL", df=df,
            )
            tracker_obj.log_run(
                dataset_name=ds_name, rows_ingested=len(df),
                rows_passed=sr.rows_passed, rows_rejected=sr.rows_rejected,
                quality_score=sr.score, duration_seconds=sr.duration_seconds,
                checks_passed=sr.passed_checks, checks_failed=sr.failed_checks,
                critical_failures=sr.critical_failures, suite_name=suite,
            )
            results.append({
                "dataset": ds_name, "score": sr.score,
                "passed": sr.passed_checks, "failed": sr.failed_checks,
                "critical": sr.critical_failures, "rows": len(df),
                "rejected": sr.rows_rejected,
            })
            progress.progress((i + 1) / len(selected))

        progress.empty()
        status_box.empty()

        st.session_state.pipeline_ran   = True
        st.session_state.suite_results  = all_suite_results
        st.session_state.results_list   = results
        st.session_state.tracker        = tracker_obj

        # KPI row
        scores = [r["score"] for r in results]
        avg_s  = sum(scores) / len(scores)
        healthy   = sum(1 for s in scores if s >= 80)
        degraded  = sum(1 for s in scores if s < 80)
        tot_rows  = sum(r["rows"] for r in results)
        tot_rej   = sum(r["rejected"] for r in results)

        cols = st.columns(4)
        with cols[0]: render_kpi(len(results), "Datasets Run", f"{len(selected)} selected")
        with cols[1]: render_kpi(f"{avg_s:.1f}", "Avg Quality Score", f"/{100}", score_color(avg_s))
        with cols[2]: render_kpi(healthy, "Healthy (≥80)", f"{degraded} degraded", "var(--green)")
        with cols[3]: render_kpi(f"{tot_rej:,}", "Rows Quarantined", f"of {tot_rows:,}", "var(--amber)")

        divider()
        sec_label("Dataset Results")

        for r in sorted(results, key=lambda x: x["score"]):
            risk, label = risk_label(r["score"])
            color = score_color(r["score"])
            pct   = r["score"]
            st.markdown(f"""
            <div class="ds-card">
                <div class="ds-card-top">
                    <span class="ds-name">{r['dataset']}</span>
                    <span class="score-badge score-{risk}">{label}</span>
                </div>
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span class="ds-meta">Checks: {r['passed']}✓ {r['failed']}✗ &nbsp;·&nbsp; Critical: {r['critical']} &nbsp;·&nbsp; Rows: {r['rows']:,} &nbsp;·&nbsp; Rejected: {r['rejected']:,}</span>
                    <span style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:{color};">{r['score']:.1f}</span>
                </div>
                <div class="ds-bar-track"><div class="ds-bar-fill" style="width:{pct}%;background:{color};"></div></div>
            </div>""", unsafe_allow_html=True)

    elif not st.session_state.pipeline_ran:
        st.markdown("""
        <div style="background:var(--bg-card);border:1px dashed var(--border-bright);border-radius:4px;
             padding:48px 40px;text-align:center;margin-top:20px;">
            <div style="font-family:'Syne',sans-serif;font-size:18px;font-weight:700;color:var(--text-dim);margin-bottom:10px;">
                No pipeline runs yet
            </div>
            <div style="font-family:'Space Mono',monospace;font-size:11px;color:var(--text-faint);">
                Select datasets above and click Run Pipeline to begin governance validation.
            </div>
        </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2: Dataset Catalog
# ─────────────────────────────────────────────────────────────────────────────
elif nav == "Dataset Catalog":
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Dataset <span class="accent">Catalog</span></div>
            <div class="page-sub">Registered datasets · Schema versions · Ownership · Data lineage</div>
        </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.pipeline_ran:
        st.info("Run the pipeline first to populate the catalog.")
        st.stop()

    tracker = st.session_state.tracker
    ds_df   = tracker.get_datasets()
    runs_df = tracker.get_pipeline_runs()

    sensitivity_colors = {
        "PII": "var(--red)", "RESTRICTED": "var(--amber)",
        "CONFIDENTIAL": "var(--amber)", "INTERNAL": "var(--cyan-dim)",
        "PUBLIC": "var(--green)",
    }

    sec_label(f"{len(ds_df)} Registered Datasets")

    if not ds_df.empty:
        for _, row in ds_df.drop_duplicates("name").iterrows():
            name = row["name"]

            # Get latest run score
            if not runs_df.empty and "dataset_name" in runs_df.columns:
                latest = runs_df[runs_df["dataset_name"] == name]
                score  = float(latest["quality_score"].iloc[-1]) if not latest.empty else 0.0
            else:
                score = 0.0

            color    = score_color(score)
            risk, lbl= risk_label(score)
            sens     = str(row.get("sensitivity", "INTERNAL"))
            sens_col = sensitivity_colors.get(sens, "var(--text-dim)")

            st.markdown(f"""
            <div class="ds-card">
                <div class="ds-card-top">
                    <span class="ds-name">{name}</span>
                    <div style="display:flex;gap:8px;align-items:center;">
                        <span style="font-family:'Space Mono',monospace;font-size:9px;
                              background:rgba(0,0,0,0.3);border:1px solid {sens_col};
                              color:{sens_col};padding:2px 8px;border-radius:2px;
                              text-transform:uppercase;letter-spacing:0.08em;">{sens}</span>
                        <span class="score-badge score-{risk}">{score:.1f} / 100</span>
                    </div>
                </div>
                <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:6px;">
                    <div class="ds-meta">Owner: {row.get('owner','—')}</div>
                    <div class="ds-meta">Schema: v{row.get('schema_version','—')}</div>
                    <div class="ds-meta">Rows: {row.get('row_count',0):,}</div>
                    <div class="ds-meta">SLA: {row.get('sla_freshness_hours','—')}h</div>
                </div>
                <div class="ds-bar-track"><div class="ds-bar-fill" style="width:{score}%;background:{color};"></div></div>
            </div>""", unsafe_allow_html=True)

    divider()
    sec_label("Quality Score History")

    if not runs_df.empty and "dataset_name" in runs_df.columns:
        import pandas as pd
        import plotly.express as px
        runs_df["run_timestamp"] = pd.to_datetime(runs_df["run_timestamp"], errors="coerce")
        fig = px.line(
            runs_df.sort_values("run_timestamp"),
            x="run_timestamp", y="quality_score",
            color="dataset_name",
            title="Quality Score Over Time",
            labels={"quality_score": "Score", "run_timestamp": "Run Time"},
        )
        fig.update_layout(
            height=320, margin=dict(t=44, b=20, l=0, r=0),
            font=dict(family="Space Mono", size=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#141720",
            title_font=dict(family="Syne", size=14, color="#e8eaf0"),
            legend=dict(font=dict(family="Space Mono", size=9), bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(gridcolor="#252a38", color="#8892a4"),
            yaxis=dict(gridcolor="#252a38", color="#8892a4", range=[0,100]),
        )
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3: Quality Checks
# ─────────────────────────────────────────────────────────────────────────────
elif nav == "Quality Checks":
    import pandas as pd

    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Quality <span class="accent">Checks</span></div>
            <div class="page-sub">12+ automated checks per dataset · CRITICAL / WARNING / INFO severity</div>
        </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.pipeline_ran or not st.session_state.suite_results:
        st.info("Run the pipeline first.")
        st.stop()

    suite_results = st.session_state.suite_results
    datasets_avail = list(suite_results.keys())

    col_l, col_r = st.columns([2, 3], gap="large")
    with col_l:
        sec_label("Select Dataset")
        sel_ds = st.selectbox("ds", datasets_avail, label_visibility="collapsed")
        sr     = suite_results[sel_ds]

        # Score gauge
        color = score_color(sr.score)
        risk, lbl = risk_label(sr.score)
        st.markdown(f"""
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:4px;
             padding:24px 20px;text-align:center;margin-top:8px;">
            <div style="font-family:'Syne',sans-serif;font-size:52px;font-weight:800;
                 color:{color};line-height:1;">{sr.score:.1f}</div>
            <div style="font-family:'Space Mono',monospace;font-size:9px;color:var(--text-faint);
                 letter-spacing:0.2em;text-transform:uppercase;margin:4px 0 12px;">Quality Score</div>
            <span class="score-badge score-{risk}">{lbl}</span>
            <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:16px;padding-top:14px;border-top:1px solid var(--border);">
                <div><div style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:var(--green);">{sr.passed_checks}</div><div style="font-size:9px;color:var(--text-faint);">PASSED</div></div>
                <div><div style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:var(--amber);">{sr.failed_checks}</div><div style="font-size:9px;color:var(--text-faint);">FAILED</div></div>
                <div><div style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:var(--red);">{sr.critical_failures}</div><div style="font-size:9px;color:var(--text-faint);">CRITICAL</div></div>
            </div>
            <div style="font-size:9.5px;color:var(--text-faint);margin-top:10px;">{sr.rows_validated:,} rows validated · {sr.duration_seconds:.2f}s</div>
        </div>""", unsafe_allow_html=True)

    with col_r:
        sec_label(f"{sr.total_checks} checks — {sel_ds}")
        for check in sr.checks:
            if check.passed:
                status_class = "pass"
                dot_class    = "dot-pass"
            elif check.severity == "CRITICAL":
                status_class = "fail-critical"
                dot_class    = "dot-critical"
            else:
                status_class = "fail-warning"
                dot_class    = "dot-warning"

            score_txt = f"{check.score:.0f}%"
            fail_txt  = f"{check.failing_pct:.1f}% fail" if check.failing_pct > 0 else "0% fail"

            st.markdown(f"""
            <div class="check-row {status_class}">
                <div class="check-dot {dot_class}"></div>
                <span class="check-id">{check.check_id}</span>
                <span class="check-name">{check.check_name}</span>
                <span class="check-score">{score_txt}</span>
                <span class="check-fail">{fail_txt}</span>
                <span style="font-size:9px;color:var(--text-faint);min-width:60px;text-align:right;">
                    {check.severity}
                </span>
            </div>""", unsafe_allow_html=True)

            if not check.passed:
                st.markdown(f"""
                <div style="background:var(--bg);border:1px solid var(--border);
                     border-top:none;padding:8px 14px 8px 30px;
                     font-family:'Space Mono',monospace;font-size:10px;color:var(--text-dim);
                     margin-bottom:2px;">
                    {check.details}
                    {f"<br><span style='color:var(--text-faint)'>Sample: {check.sample_failures[:3]}</span>" if check.sample_failures else ""}
                </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4: Audit Log
# ─────────────────────────────────────────────────────────────────────────────
elif nav == "Audit Log":
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Audit <span class="accent">Log</span></div>
            <div class="page-sub">Complete immutable record of all pipeline and governance actions</div>
        </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.pipeline_ran:
        st.info("Run the pipeline first.")
        st.stop()

    tracker = st.session_state.tracker
    audit   = tracker.get_audit_log(since_hours=8760)

    if audit.empty:
        st.warning("No audit events found.")
        st.stop()

    # Summary
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_kpi(len(audit), "Total Events")
    with c2: render_kpi(int((audit.get("severity","") == "CRITICAL").sum() if "severity" in audit.columns else 0), "Critical Events", accent="var(--red)")
    with c3: render_kpi(int((audit.get("severity","") == "WARNING").sum() if "severity" in audit.columns else 0), "Warnings", accent="var(--amber)")
    with c4: render_kpi(audit["dataset_name"].nunique() if "dataset_name" in audit.columns else 0, "Datasets Touched")

    divider()

    # Filters
    col_l, col_r = st.columns([2, 3], gap="large")
    with col_l:
        sec_label("Filters")
        ds_filter  = st.selectbox("Dataset", ["All"] + sorted(audit["dataset_name"].unique().tolist() if "dataset_name" in audit.columns else []), label_visibility="collapsed")
        sev_filter = st.multiselect("Severity", ["INFO","WARNING","CRITICAL"], default=["WARNING","CRITICAL"], label_visibility="collapsed")

    filtered = audit.copy()
    if ds_filter != "All" and "dataset_name" in filtered.columns:
        filtered = filtered[filtered["dataset_name"] == ds_filter]
    if sev_filter and "severity" in filtered.columns:
        filtered = filtered[filtered["severity"].isin(sev_filter)]

    with col_r:
        sec_label(f"{len(filtered)} events")

    st.markdown("""
    <div class="audit-row" style="color:var(--text-faint);font-size:9px;letter-spacing:0.1em;text-transform:uppercase;border-color:var(--border-bright);">
        <span>Timestamp</span><span>Action</span><span>Dataset</span><span>Details</span>
    </div>""", unsafe_allow_html=True)

    for _, row in filtered.head(50).iterrows():
        ts_str = str(row.get("timestamp", ""))[:19].replace("T", " ")
        sev    = str(row.get("severity", "INFO"))
        action = str(row.get("action", ""))
        dataset= str(row.get("dataset_name", ""))
        detail = str(row.get("details", ""))[:80]
        st.markdown(f"""
        <div class="audit-row">
            <span class="audit-ts">{ts_str}</span>
            <span class="audit-action">{action}</span>
            <span style="color:var(--cyan-dim);font-size:10.5px;">{dataset}</span>
            <span class="audit-sev-{sev}">{detail}</span>
        </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5: Power BI Export
# ─────────────────────────────────────────────────────────────────────────────
elif nav == "Power BI Export":
    st.markdown("""
    <div class="page-header">
        <div>
            <div class="page-title">Power BI <span class="accent">Export</span></div>
            <div class="page-sub">Export governance KPIs as linked CSV tables for Power BI dashboard integration</div>
        </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.pipeline_ran:
        st.info("Run the pipeline first.")
        st.stop()

    import plotly.graph_objects as go
    import plotly.express as px

    tracker  = st.session_state.tracker
    results  = st.session_state.get("results_list", [])

    # ── Pre-export visualisation ──
    if results:
        sec_label("KPI Preview")
        cols = st.columns(2)

        with cols[0]:
            sorted_r = sorted(results, key=lambda x: x["score"])
            fig = go.Figure(go.Bar(
                x=[r["score"] for r in sorted_r],
                y=[r["dataset"] for r in sorted_r],
                orientation="h",
                marker_color=[score_color(r["score"]) for r in sorted_r],
                marker_line=dict(color="rgba(0,0,0,0)", width=0),
            ))
            fig.add_vline(x=80, line_dash="dash", line_color="rgba(0,212,255,0.3)",
                          annotation_text="Alert threshold",
                          annotation_font=dict(family="Space Mono", size=9, color="#00d4ff"))
            fig.update_layout(
                height=max(280, len(results)*34),
                margin=dict(t=10, b=10, l=0, r=0),
                font=dict(family="Space Mono", size=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#141720",
                xaxis=dict(range=[0,100], gridcolor="#252a38", color="#8892a4"),
                yaxis=dict(color="#8892a4"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        with cols[1]:
            cats    = ["Critical","High","Medium","Low"]
            counts  = [
                sum(1 for r in results if r["score"] < 60),
                sum(1 for r in results if 60 <= r["score"] < 75),
                sum(1 for r in results if 75 <= r["score"] < 90),
                sum(1 for r in results if r["score"] >= 90),
            ]
            colors = ["#ff4757","#ff6b35","#ffb347","#00e676"]
            fig2 = go.Figure(go.Pie(
                labels=cats, values=counts, hole=0.6,
                marker_colors=colors,
                marker_line=dict(color="#0d0f12", width=3),
                textfont=dict(family="Space Mono", size=10),
            ))
            fig2.update_layout(
                height=260, margin=dict(t=10,b=10,l=0,r=0),
                font=dict(family="Space Mono"),
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
                legend=dict(font=dict(family="Space Mono",size=9), bgcolor="rgba(0,0,0,0)", font_color="#8892a4"),
            )
            st.plotly_chart(fig2, use_container_width=True)

    divider()

    col_btn, col_info = st.columns([2, 3])
    with col_btn:
        sec_label("Export")
        if st.button("⬇  Export Power BI Files"):
            from reporting.powerbi_exporter import PowerBIExporter
            exporter = PowerBIExporter()
            outputs  = exporter.export_all(tracker)

            st.success(f"✓ Exported {len(outputs)} files")
            for name, path in outputs.items():
                st.code(path, language="")

    with col_info:
        sec_label("Power BI Schema")
        st.markdown("""
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:4px;padding:16px 18px;font-family:'Space Mono',monospace;font-size:10.5px;color:var(--text-dim);line-height:1.9;">
            <strong style="color:var(--cyan);display:block;margin-bottom:8px;font-size:9.5px;letter-spacing:0.12em;text-transform:uppercase;">4 Linked Tables</strong>
            📊 governance_datasets.csv<br>
            📈 governance_pipeline_runs.csv<br>
            🎯 governance_kpi_summary.csv<br>
            📋 governance_audit_log.csv<br>
            <br>
            <strong style="color:var(--cyan);display:block;margin-bottom:6px;font-size:9.5px;letter-spacing:0.12em;text-transform:uppercase;">Relationships</strong>
            datasets[name] → pipeline_runs[dataset_name]<br>
            pipeline_runs[run_id] → quality_checks[run_id]<br>
            datasets[name] → audit_log[dataset_name]<br>
            <br>
            <strong style="color:var(--cyan);display:block;margin-bottom:6px;font-size:9.5px;letter-spacing:0.12em;text-transform:uppercase;">Key DAX Measures</strong>
            Avg Quality Score = AVERAGE(pipeline_runs[quality_score])<br>
            Rejection Rate = DIVIDE(SUM([rows_rejected]),SUM([rows_ingested]))
        </div>""", unsafe_allow_html=True)
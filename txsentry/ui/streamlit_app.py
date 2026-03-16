"""Module 9: Streamlit Dashboard.

4-page dashboard: Alert Queue, Case Detail, Model Monitoring, Graph Explorer.

Usage:
    streamlit run txsentry/ui/streamlit_app.py
"""

import json
import sys
from pathlib import Path

# Ensure project root is on path for imports
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
import streamlit as st

ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"
MODELS_DIR = "data/models"
CASES_DIR = "data/cases/memos"

st.set_page_config(page_title="TxSentry", page_icon="🛡️", layout="wide")


@st.cache_data(ttl=60)
def load_alerts():
    path = f"{ALERTS_DIR}/alert_events.parquet"
    if not Path(path).exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@st.cache_data(ttl=60)
def load_metrics():
    path = f"{MODELS_DIR}/metrics.json"
    if not Path(path).exists():
        return {}
    return json.loads(Path(path).read_text())


def load_case(case_id: str) -> dict | None:
    path = Path(CASES_DIR) / f"{case_id}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


# --- Sidebar navigation ---
page = st.sidebar.radio("Navigation", ["Alert Queue", "Case Detail", "Model Monitoring", "Graph Explorer"])

# ============================================================
# PAGE 1: Alert Queue
# ============================================================
if page == "Alert Queue":
    st.title("🛡️ TxSentry — Alert Queue")

    alerts = load_alerts()
    if alerts.empty:
        st.warning("No alert data found.")
        st.stop()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        risk_filter = st.multiselect("Risk Band", ["CRITICAL", "HIGH", "ELEVATED", "MEDIUM", "LOW"],
                                      default=["CRITICAL", "HIGH", "ELEVATED"])
    with col2:
        action_filter = st.multiselect("Action", alerts["action"].unique().tolist(),
                                        default=["BLOCK", "QUEUE_FOR_REVIEW"])
    with col3:
        top_n = st.slider("Show top N", 10, 500, 100)

    # Filter
    filtered = alerts[
        alerts["risk_band"].isin(risk_filter) &
        alerts["action"].isin(action_filter)
    ].nlargest(top_n, "final_risk_score")

    # Summary metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Alerts", f"{len(filtered):,}")
    m2.metric("Fraud in Selection", f"{filtered['is_fraud'].sum():,}")
    m3.metric("Avg Risk Score", f"{filtered['final_risk_score'].mean():.3f}")
    m4.metric("Precision (Fraud %)", f"{filtered['is_fraud'].mean():.1%}")

    # Color-coded table
    def color_action(val):
        colors = {
            "BLOCK": "background-color: #dc2626; color: white",
            "QUEUE_FOR_REVIEW": "background-color: #d97706; color: white",
            "STEP_UP_AUTH": "background-color: #2563eb; color: white",
            "ALLOW_WITH_MONITORING": "background-color: #7c3aed; color: white",
            "ALLOW": "background-color: #16a34a; color: white",
        }
        return colors.get(val, "")

    display_cols = ["alert_id", "timestamp", "account_id", "amount",
                    "final_risk_score", "risk_band", "action", "is_fraud"]
    display = filtered[display_cols].copy()
    display["final_risk_score"] = display["final_risk_score"].round(4)

    st.dataframe(
        display.style.map(color_action, subset=["action"]),
        width="stretch",
        height=500,
    )

# ============================================================
# PAGE 2: Case Detail
# ============================================================
elif page == "Case Detail":
    st.title("🔍 Case Investigation Detail")

    cases_dir = Path(CASES_DIR)
    case_files = sorted(cases_dir.glob("*.json")) if cases_dir.exists() else []

    if not case_files:
        st.warning("No case memos found. Run the LangGraph agent first.")
        st.stop()

    case_names = [f.stem for f in case_files]
    selected = st.selectbox("Select Case", case_names)

    case = load_case(selected)
    if case is None:
        st.error("Case not found.")
        st.stop()

    # Header
    action = case.get("recommended_action", "N/A")
    action_colors = {
        "BLOCK": "🔴", "QUEUE_FOR_REVIEW": "🟠",
        "STEP_UP_AUTH": "🔵", "ALLOW_WITH_MONITORING": "🟣", "ALLOW": "🟢",
    }
    st.markdown(f"### {action_colors.get(action, '⚪')} {selected} — {action}")

    col1, col2, col3 = st.columns(3)
    col1.metric("Confidence", f"{case.get('confidence', 0):.2%}")
    col2.metric("Priority", case.get("priority", "N/A"))
    col3.metric("Alert ID", case.get("alert_id", "N/A"))

    # Summary
    st.markdown("#### Summary")
    st.info(case.get("summary", "No summary available."))

    # Reason codes
    st.markdown("#### Reason Codes")
    codes = case.get("reason_codes", [])
    st.write(" | ".join([f"`{c}`" for c in codes]) if codes else "None")

    # Supporting evidence
    st.markdown("#### Supporting Evidence")
    for ev in case.get("supporting_evidence", []):
        st.markdown(f"- {ev}")

    # Tools called
    st.markdown("#### Tools Called")
    tools = case.get("tools_called", [])
    st.write(" → ".join(tools) if tools else "No tools recorded")

    # Next steps
    st.markdown("#### Recommended Next Steps")
    for step in case.get("next_steps", []):
        st.markdown(f"1. {step}")

    # Entities involved
    st.markdown("#### Entities Involved")
    entities = case.get("entities_involved", {})
    if entities:
        st.json(entities)

# ============================================================
# PAGE 3: Model Monitoring
# ============================================================
elif page == "Model Monitoring":
    st.title("📊 Model Monitoring")

    alerts = load_alerts()
    metrics = load_metrics()

    if alerts.empty:
        st.warning("No alert data found.")
        st.stop()

    # Model metrics
    st.markdown("### Model Performance Metrics")
    if metrics:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("PR-AUC (Temporal)", f"{metrics.get('pr_auc_temporal', 0):.4f}")
        m2.metric("PR-AUC (Random)", f"{metrics.get('pr_auc_random', 0):.4f}")
        m3.metric("Precision @Budget", f"{metrics.get('precision_at_500_alerts', 0):.2%}")
        m4.metric("Recall @Budget", f"{metrics.get('recall_at_500_alerts', 0):.2%}")

    # Monthly alert volume
    st.markdown("### Alert Volume by Month")
    alerts["month"] = alerts["timestamp"].dt.to_period("M").astype(str)
    monthly_vol = alerts.groupby("month").size().reset_index(name="count")
    st.bar_chart(monthly_vol.set_index("month"))

    # Action distribution by month
    st.markdown("### Action Distribution by Month")
    action_monthly = alerts.groupby(["month", "action"]).size().unstack(fill_value=0)
    st.bar_chart(action_monthly)

    # Risk score distribution
    st.markdown("### Risk Score Distribution")
    hist_data = alerts["final_risk_score"].dropna()
    hist_counts, hist_edges = np.histogram(hist_data, bins=20)
    hist_df = pd.DataFrame({
        "score_bin": [f"{hist_edges[i]:.2f}" for i in range(len(hist_counts))],
        "count": hist_counts,
    }).set_index("score_bin")
    st.bar_chart(hist_df)

    # Fraud rate by month
    st.markdown("### Fraud Rate by Month")
    fraud_monthly = alerts.groupby("month")["is_fraud"].mean().reset_index()
    fraud_monthly.columns = ["month", "fraud_rate"]
    st.line_chart(fraud_monthly.set_index("month"))

# ============================================================
# PAGE 4: Graph Explorer
# ============================================================
elif page == "Graph Explorer":
    st.title("🕸️ Graph Explorer")

    st.markdown("Enter an account ID to explore its graph neighborhood.")
    account_id = st.text_input("Account ID", "ACC_AML_152")

    if st.button("Explore") and account_id:
        from txsentry.services.mcp_server.tools.graph_tools import get_graph_neighborhood

        with st.spinner("Loading graph neighborhood..."):
            result = get_graph_neighborhood(account_id, "account", 2)

        if "error" in result:
            st.error(result["error"])
        else:
            col1, col2 = st.columns(2)
            col1.metric("Total Neighbors", result.get("total_neighbors", 0))
            col2.metric("Shared Device Accounts", len(result.get("shared_device_accounts", [])))

            st.markdown("#### Connected Devices")
            st.write(result.get("connected_devices", []))

            st.markdown("#### Connected Beneficiaries")
            st.write(result.get("connected_beneficiaries", [])[:20])

            st.markdown("#### Shared Device Accounts")
            shared = result.get("shared_device_accounts", [])
            if shared:
                st.warning(f"⚠️ {len(shared)} other accounts share a device with this account")
                st.write(shared[:20])
            else:
                st.success("No shared devices detected")

            st.markdown("#### Graph Features")
            gf = result.get("graph_features", {})
            if gf:
                st.json(gf)
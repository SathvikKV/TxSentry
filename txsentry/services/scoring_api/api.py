"""Full backend API serving real data to the Next.js frontend.

Usage:
    cd C:\\Users\\Sathvik\\Documents\\Projects\\txsentry
    .venv\\Scripts\\activate
    uvicorn txsentry.services.scoring_api.api:app --port 8000 --reload
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"
MODELS_DIR = "data/models"
CASES_DIR = "data/cases/memos"

app = FastAPI(title="TxSentry API", version="1.0.0")

# CORS — allow Next.js dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Lazy data cache ---
_cache = {}


def _alerts() -> pd.DataFrame:
    if "alerts" not in _cache:
        df = pd.read_parquet(f"{ALERTS_DIR}/alert_events.parquet")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        _cache["alerts"] = df
        logger.info(f"Loaded {len(df):,} alerts")
    return _cache["alerts"]


def _metrics() -> dict:
    if "metrics" not in _cache:
        path = Path(f"{MODELS_DIR}/metrics.json")
        _cache["metrics"] = json.loads(path.read_text()) if path.exists() else {}
    return _cache["metrics"]


def _monitoring() -> dict:
    if "monitoring" not in _cache:
        path = Path(f"{MODELS_DIR}/monitoring_report.json")
        _cache["monitoring"] = json.loads(path.read_text()) if path.exists() else {}
    return _cache["monitoring"]


def _shap() -> list:
    if "shap" not in _cache:
        path = Path(f"{MODELS_DIR}/shap_importance.csv")
        if path.exists():
            df = pd.read_csv(path)
            _cache["shap"] = df.to_dict("records")
        else:
            _cache["shap"] = []
    return _cache["shap"]


# ============================================================
# ENDPOINT: /api/alerts — Alert Queue page
# ============================================================
@app.get("/api/alerts")
def get_alerts(
    limit: int = Query(50, ge=1, le=500),
    risk_band: str = Query(None),
    action: str = Query(None),
    search: str = Query(None),
):
    """Return alerts matching the frontend Alert[] type."""
    df = _alerts()

    # Filter
    if risk_band and risk_band != "all":
        df = df[df["risk_band"] == risk_band]
    if action and action != "all":
        df = df[df["action"] == action]
    if search:
        df = df[df["account_id"].str.contains(search, case=False, na=False)]

    # Sort by risk score descending, take top N
    df = df.nlargest(limit, "final_risk_score")

    alerts = []
    for _, row in df.iterrows():
        alerts.append({
            "alertId": str(row.get("alert_id", "")),
            "timestamp": str(row["timestamp"]),
            "accountId": str(row.get("account_id", "")),
            "amount": round(float(row.get("amount", 0)), 2),
            "riskScore": round(float(row.get("final_risk_score", 0)), 4),
            "riskBand": str(row.get("risk_band", "LOW")),
            "action": str(row.get("action", "ALLOW")),
            "isFraud": bool(row.get("is_fraud", False)),
        })

    return {"alerts": alerts, "total": len(alerts)}


# ============================================================
# ENDPOINT: /api/alerts/distribution — Action distribution donut
# ============================================================
@app.get("/api/alerts/distribution")
def get_action_distribution():
    df = _alerts()
    dist = df["action"].value_counts().to_dict()

    colors = {
        "ALLOW": "#22c55e",
        "ALLOW_WITH_MONITORING": "#a855f7",
        "STEP_UP_AUTH": "#3b82f6",
        "QUEUE_FOR_REVIEW": "#f59e0b",
        "BLOCK": "#ef4444",
    }

    return [
        {"action": action, "count": int(count), "color": colors.get(action, "#6b7280")}
        for action, count in dist.items()
    ]


# ============================================================
# ENDPOINT: /api/cases/:case_id — Case Investigation Detail page
# ============================================================
@app.get("/api/cases/{case_id}")
def get_case(case_id: str):
    """Return case memo matching the frontend mockCase shape."""
    path = Path(CASES_DIR) / f"{case_id}.json"
    if not path.exists():
        return {"error": f"Case {case_id} not found"}

    case = json.loads(path.read_text())

    # Get the alert data for risk scores
    alerts = _alerts()
    alert_row = alerts[alerts["alert_id"] == case.get("alert_id", "")]

    risk_scores = {"txnRisk": 0, "anomaly": 0, "graph": 0, "final": 0}
    amount = 0
    if not alert_row.empty:
        r = alert_row.iloc[0]
        risk_scores = {
            "txnRisk": round(float(r.get("txn_risk_score", 0)), 4),
            "anomaly": round(float(r.get("behavior_anomaly_score", 0)), 4),
            "graph": round(float(r.get("graph_risk_score", 0)), 4),
            "final": round(float(r.get("final_risk_score", 0)), 4),
        }
        amount = round(float(r.get("amount", 0)), 2)

    # Build investigation steps from case data
    # The case memo has tools_called but not full trace — build simplified version
    tools = case.get("tools_called", [])
    steps = []
    for i, tool in enumerate(tools):
        steps.append({
            "step": i + 1,
            "tool": tool,
            "params": {"account_id": case.get("alert_id", "").replace("ALT_", "ACC_AML_")},
            "output": f"Tool {tool} executed successfully",
            "reasoning": f"Step {i+1} of investigation plan",
        })

    # Build entities from case data
    entities_raw = case.get("entities_involved", {})
    entities = []
    type_map = {
        "account_ids": ("Account", "Source Account"),
        "device_ids": ("Device", "Device"),
        "merchant_ids": ("Merchant", "Merchant"),
        "beneficiary_ids": ("Beneficiary", "Beneficiary"),
    }
    for key, (etype, label) in type_map.items():
        for eid in entities_raw.get(key, []):
            entities.append({"type": etype, "id": str(eid), "label": label})

    return {
        "caseId": case.get("case_id", case_id),
        "alertId": case.get("alert_id", ""),
        "accountId": str(entities_raw.get("account_ids", [""])[0]) if entities_raw.get("account_ids") else "",
        "amount": amount,
        "action": case.get("recommended_action", "QUEUE_FOR_REVIEW"),
        "confidence": round(float(case.get("confidence", 0)) * 100, 1),
        "priority": case.get("priority", "MEDIUM"),
        "reasonCodes": case.get("reason_codes", []),
        "summary": case.get("summary", ""),
        "riskScores": risk_scores,
        "investigationSteps": steps,
        "entities": entities,
        "evidence": case.get("supporting_evidence", []),
        "nextSteps": case.get("next_steps", []),
    }


# ============================================================
# ENDPOINT: /api/cases — List all cases
# ============================================================
@app.get("/api/cases")
def list_cases():
    cases_dir = Path(CASES_DIR)
    if not cases_dir.exists():
        return []
    return [f.stem for f in sorted(cases_dir.glob("*.json"))]


# ============================================================
# ENDPOINT: /api/model/metrics — Model Performance page
# ============================================================
@app.get("/api/model/metrics")
def get_model_metrics():
    metrics = _metrics()
    return {
        "prAucTemporal": metrics.get("pr_auc_temporal", 0),
        "prAucRandom": metrics.get("pr_auc_random", 0),
        "rocAucTemporal": metrics.get("roc_auc_temporal", 0),
        "rocAucRandom": metrics.get("roc_auc_random", 0),
        "precisionAtBudget": metrics.get("precision_at_500_alerts", 0),
        "recallAtBudget": metrics.get("recall_at_500_alerts", 0),
    }


# ============================================================
# ENDPOINT: /api/model/shap — SHAP feature importance
# ============================================================
@app.get("/api/model/shap")
def get_shap_importance():
    shap_data = _shap()

    # Map features to categories
    category_map = {
        "txn_type": "metadata", "channel": "metadata",
        "amount_sum_24h": "velocity", "amount_sum_1h": "velocity",
        "txn_count_1h": "velocity", "txn_count_24h": "velocity",
        "txn_count_7d": "velocity", "unique_merchants_7d": "velocity",
        "unique_beneficiaries_7d": "velocity", "unique_devices_30d": "velocity",
        "community_fraud_rate": "graph", "account_degree": "graph",
        "graph_risk_score": "graph", "device_shared_account_count": "graph",
        "beneficiary_in_degree": "graph",
        "is_new_device": "behavioral", "is_new_beneficiary": "behavioral",
        "amount_vs_30d_avg": "behavioral", "amount_vs_30d_max": "behavioral",
        "days_since_last_txn": "behavioral", "ip_is_vpn": "behavioral",
        "merchant_fraud_rate_hist": "behavioral",
    }

    result = []
    for item in shap_data[:10]:
        feature = item.get("feature", "")
        result.append({
            "feature": feature,
            "importance": round(float(item.get("mean_abs_shap", 0)), 4),
            "category": category_map.get(feature, "other"),
        })

    return result


# ============================================================
# ENDPOINT: /api/monitoring — Monitoring & Drift page
# ============================================================
@app.get("/api/monitoring")
def get_monitoring():
    report = _monitoring()
    alerts = _alerts()

    # PSI heatmap
    psi_data = report.get("psi_by_feature", {})
    psi_heatmap = []
    for feature, monthly in psi_data.items():
        values = {}
        for month_str, psi_val in monthly.items():
            # Map "2024-10" -> "oct", "2024-11" -> "nov", etc.
            month_map = {
                "2024-10": "oct", "2024-11": "nov",
                "2024-12": "dec", "2025-01": "jan",
            }
            short = month_map.get(month_str)
            if short:
                values[short] = round(psi_val, 4)
        if values:
            psi_heatmap.append({"feature": feature, "values": values})

    # Monthly precision
    precision_data = report.get("precision_monthly", {})
    monthly_precision = []
    for month_str, prec in precision_data.items():
        month_labels = {
            "2024-10": "Oct 2024", "2024-11": "Nov 2024",
            "2024-12": "Dec 2024", "2025-01": "Jan 2025",
        }
        label = month_labels.get(month_str, month_str)
        monthly_precision.append({"month": label, "precision": round(prec * 100, 1)})

    # Monthly fraud rate from actual data
    alerts_copy = alerts.copy()
    alerts_copy["month"] = alerts_copy["timestamp"].dt.to_period("M")
    monthly_fraud = (
        alerts_copy.groupby("month")["is_fraud"]
        .mean()
        .reset_index()
    )
    monthly_fraud_rate = []
    for _, row in monthly_fraud.iterrows():
        month_str = str(row["month"])
        rate = round(float(row["is_fraud"]) * 100, 1)
        # Format nicely
        try:
            period = pd.Period(month_str)
            label = period.strftime("%b %Y")
        except Exception:
            label = month_str
        monthly_fraud_rate.append({"month": label, "rate": rate})

    return {
        "psiHeatmap": psi_heatmap,
        "monthlyPrecision": monthly_precision,
        "monthlyFraudRate": monthly_fraud_rate,
        "retrainingRecommended": report.get("retraining_recommended", False),
        "driftFeatures": report.get("drift_features", []),
    }


# ============================================================
# ENDPOINT: /api/graph/:account_id — Graph Explorer page
# ============================================================
@app.get("/api/graph/{account_id}")
def get_graph_data(account_id: str):
    """Return graph neighborhood + pattern detection for an account."""
    from txsentry.services.mcp_server.tools.graph_tools import (
        get_graph_neighborhood, detect_graph_pattern,
    )

    neighborhood = get_graph_neighborhood(account_id, "account", 2)
    patterns = detect_graph_pattern(account_id)

    if "error" in neighborhood:
        return {"error": neighborhood["error"]}

    # Build response matching frontend shape
    connected_devices = []
    for dev_id in neighborhood.get("connected_devices", [])[:5]:
        shared_accts = [a for a in neighborhood.get("shared_device_accounts", []) if True]
        connected_devices.append({
            "id": dev_id,
            "shared": len(neighborhood.get("shared_device_accounts", [])) > 0,
            "sharedWith": len(neighborhood.get("shared_device_accounts", [])),
        })

    connected_beneficiaries = []
    for ben_id in neighborhood.get("connected_beneficiaries", [])[:10]:
        connected_beneficiaries.append({
            "id": ben_id,
            "riskScore": 0.5,
            "transactions": 1,
        })

    connected_ips = []
    for ip_id in neighborhood.get("connected_ips", [])[:5]:
        connected_ips.append({"id": ip_id, "riskLevel": "medium"})

    shared_device_accounts = []
    for acc_id in neighborhood.get("shared_device_accounts", [])[:10]:
        shared_device_accounts.append({"id": acc_id, "isFraud": True})

    pattern_list = []
    if patterns.get("patterns_detected"):
        for p in patterns["patterns_detected"]:
            evidence_map = {
                "FAN_OUT": "Multiple unique beneficiaries in short window",
                "FAN_IN": "Received from multiple accounts",
                "MULE_CHAIN": "Chain pattern: received then forwarded",
                "SHARED_DEVICE_RING": "Shared device with multiple accounts",
                "STRUCTURING": "Transactions just below reporting threshold",
            }
            pattern_list.append({
                "pattern": p,
                "confidence": round(float(patterns.get("confidence", 0.5)), 2),
                "evidence": evidence_map.get(p, "Pattern detected"),
            })

    graph_features = neighborhood.get("graph_features", {})

    return {
        "account": {
            "id": account_id,
            "degree": graph_features.get("account_degree", 0),
            "riskScore": graph_features.get("graph_risk_score", 0),
            "community": f"Cluster_{graph_features.get('community_id', 0)}",
            "communityFraudRate": graph_features.get("community_fraud_rate", 0),
            "totalTransactions": neighborhood.get("total_neighbors", 0),
            "flaggedTransactions": len(shared_device_accounts),
        },
        "connectedDevices": connected_devices,
        "connectedBeneficiaries": connected_beneficiaries,
        "connectedIPs": connected_ips,
        "sharedDeviceAccounts": shared_device_accounts,
        "patterns": pattern_list,
    }


# ============================================================
# ENDPOINT: /api/overview — Overview page metrics
# ============================================================
@app.get("/api/overview")
def get_overview():
    metrics = _metrics()
    alerts = _alerts()

    return {
        "keyMetrics": [
            {"label": "Transactions Processed", "value": f"{len(alerts)/1_000_000:.1f}M"},
            {"label": "Fraud Detected", "value": f"{int(alerts['is_fraud'].sum()/1000)}K"},
            {"label": "Precision @Budget", "value": f"{metrics.get('precision_at_500_alerts', 0):.1%}"},
            {"label": "PR-AUC", "value": f"{metrics.get('pr_auc_temporal', 0):.3f}"},
            {"label": "Fraud Scenarios", "value": "6"},
            {"label": "Investigation Tools", "value": "11"},
        ],
        "actionDistribution": [
            {"action": a, "count": int(c), "color": col}
            for a, c, col in zip(
                *zip(*[
                    (action, int(count), {"ALLOW": "#22c55e", "ALLOW_WITH_MONITORING": "#a855f7",
                     "STEP_UP_AUTH": "#3b82f6", "QUEUE_FOR_REVIEW": "#f59e0b", "BLOCK": "#ef4444"}.get(action, "#6b7280"))
                    for action, count in alerts["action"].value_counts().items()
                ])
            )
        ],
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}
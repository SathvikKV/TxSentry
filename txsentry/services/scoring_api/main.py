"""Module 8: FastAPI Scoring API.

Real-time transaction scoring endpoint and alert queue.

Usage:
    uvicorn txsentry.services.scoring_api.main:app --reload --port 8000
"""

import time
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODELS_DIR = "data/models"
ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"

app = FastAPI(
    title="TxSentry Scoring API",
    description="Real-time payment fraud detection scoring endpoint",
    version="1.0.0",
)

# --- Lazy model loading ---
_models = {}


def _load_models():
    if "lgbm" not in _models:
        logger.info("Loading LightGBM model...")
        _models["lgbm"] = lgb.Booster(model_file=f"{MODELS_DIR}/lgbm_txn_risk.txt")
    if "iso" not in _models:
        logger.info("Loading Isolation Forest model...")
        _models["iso"] = joblib.load(f"{MODELS_DIR}/isolation_forest.joblib")
    return _models


# --- Request/Response schemas ---

class TransactionEvent(BaseModel):
    txn_id: str
    account_id: str
    merchant_id: str | None = None
    beneficiary_id: str | None = None
    device_id: str | None = None
    ip_id: str | None = None
    amount: float
    currency: str = "USD"
    txn_type: str = "TRANSFER"
    channel: str = "ONLINE"
    timestamp: str | None = None


class AlertResponse(BaseModel):
    alert_id: str
    txn_id: str
    account_id: str
    txn_risk_score: float
    behavior_anomaly_score: float
    final_risk_score: float
    risk_band: str
    action: str
    reason_codes: list[str]
    latency_ms: float


class CaseMemoResponse(BaseModel):
    case_id: str
    alert_id: str
    recommended_action: str
    confidence: float
    priority: str
    reason_codes: list[str]
    summary: str
    supporting_evidence: list[str]
    next_steps: list[str]


# --- In-memory alert queue ---
_alert_queue: list[dict] = []
_alert_counter = 0


# --- Endpoints ---

@app.on_event("startup")
async def startup():
    _load_models()
    logger.info("TxSentry Scoring API ready")


@app.post("/score", response_model=AlertResponse)
async def score_transaction(event: TransactionEvent):
    """Score a single transaction event. Returns alert with risk scores and action."""
    global _alert_counter
    start = time.time()

    models = _load_models()

    # Build feature vector (simplified — uses defaults for missing features)
    features = {
        "txn_count_1h": 1, "txn_count_24h": 1, "txn_count_7d": 1,
        "amount_sum_1h": event.amount, "amount_sum_24h": event.amount,
        "unique_merchants_7d": 1, "unique_beneficiaries_7d": 1, "unique_devices_30d": 1,
        "amount_vs_30d_avg": 1.0, "amount_vs_30d_max": 1.0, "days_since_last_txn": 0.0,
        "merchant_fraud_rate_hist": 0.0,
        "account_degree": 0, "device_shared_account_count": 0,
        "beneficiary_in_degree": 0, "num_devices": 1, "num_beneficiaries": 1,
        "community_size": 1, "community_fraud_rate": 0.0, "graph_risk_score": 0.0,
        "is_new_device": 0, "is_new_beneficiary": 0, "ip_is_vpn": 0,
        "is_fan_out_source": 0, "is_fan_in_target": 0, "is_shared_device": 0,
        "structuring_flag": 0, "is_mule_chain_member": 0,
        "mcc_risk_tier": "LOW", "channel": event.channel, "txn_type": event.txn_type,
    }

    feature_df = pd.DataFrame([features])
    for col in ["mcc_risk_tier", "channel", "txn_type"]:
        feature_df[col] = feature_df[col].astype("category")

    # Score with LightGBM
    txn_risk_score = float(models["lgbm"].predict(feature_df)[0])

    # Score with Isolation Forest (simplified)
    anomaly_features = ["txn_count_7d", "amount_sum_24h", "unique_merchants_7d",
                        "unique_beneficiaries_7d", "unique_devices_30d",
                        "days_since_last_txn", "amount_vs_30d_avg"]
    iso_input = feature_df[anomaly_features].fillna(0)
    raw_anomaly = models["iso"].score_samples(iso_input)[0]
    behavior_anomaly_score = float(np.clip((0.5 - raw_anomaly) / 1.0, 0, 1))

    # Fusion (simplified)
    final_score = 0.50 * txn_risk_score + 0.25 * behavior_anomaly_score + 0.25 * 0.0
    final_score = float(np.clip(final_score, 0, 1))

    # Action assignment
    if final_score >= 0.85:
        action, risk_band = "BLOCK", "CRITICAL"
    elif final_score >= 0.70:
        action, risk_band = "QUEUE_FOR_REVIEW", "HIGH"
    elif final_score >= 0.55:
        action, risk_band = "STEP_UP_AUTH", "ELEVATED"
    elif final_score >= 0.35:
        action, risk_band = "ALLOW_WITH_MONITORING", "MEDIUM"
    else:
        action, risk_band = "ALLOW", "LOW"

    # Reason codes
    reason_codes = []
    if txn_risk_score > 0.7:
        reason_codes.append("ELEVATED_RISK_SCORE")
    if event.amount > 9000:
        reason_codes.append("STRUCTURING_PATTERN")

    latency_ms = (time.time() - start) * 1000
    _alert_counter += 1
    alert_id = f"ALT_RT_{_alert_counter:08d}"

    alert = {
        "alert_id": alert_id,
        "txn_id": event.txn_id,
        "account_id": event.account_id,
        "txn_risk_score": round(txn_risk_score, 4),
        "behavior_anomaly_score": round(behavior_anomaly_score, 4),
        "final_risk_score": round(final_score, 4),
        "risk_band": risk_band,
        "action": action,
        "reason_codes": reason_codes,
        "latency_ms": round(latency_ms, 2),
        "timestamp": event.timestamp or datetime.utcnow().isoformat(),
    }

    _alert_queue.append(alert)
    if len(_alert_queue) > 1000:
        _alert_queue.pop(0)

    return AlertResponse(**alert)


@app.get("/alerts")
async def get_alert_queue(
    limit: int = Query(50, ge=1, le=500),
    risk_band: str = Query(None),
):
    """Return current alert queue, optionally filtered by risk band."""
    alerts = _alert_queue[-limit:]
    if risk_band:
        alerts = [a for a in alerts if a["risk_band"] == risk_band]
    return {"alerts": alerts, "total": len(alerts)}


@app.get("/cases/{case_id}")
async def get_case(case_id: str):
    """Return completed case memo for a given case."""
    import json
    case_path = Path(f"data/cases/memos/{case_id}.json")
    if not case_path.exists():
        return {"error": f"Case {case_id} not found"}
    return json.loads(case_path.read_text())


@app.get("/health")
async def health():
    models = _load_models()
    return {
        "status": "ok",
        "models_loaded": list(models.keys()),
        "alert_queue_size": len(_alert_queue),
    }
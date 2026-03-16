"""Transaction and account investigation tools."""

import pandas as pd
import numpy as np
from pathlib import Path

ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"
CANONICAL_DIR = "data/canonical"
SYNTHETIC_DIR = "data/synthetic"

# Lazy-loaded data cache
_cache = {}


def _load(key: str, path: str, columns: list[str] = None) -> pd.DataFrame:
    if key not in _cache:
        if not Path(path).exists():
            # Try directory (Spark output)
            if Path(path).is_dir():
                _cache[key] = pd.read_parquet(path, columns=columns)
            else:
                return pd.DataFrame()
        else:
            _cache[key] = pd.read_parquet(path, columns=columns)
    return _cache[key]


def _alerts() -> pd.DataFrame:
    return _load("alerts", f"{ALERTS_DIR}/alert_events.parquet")


def _txn_features() -> pd.DataFrame:
    return _load("txn_features", f"{FEATURES_DIR}/txn_features.parquet")


def _graph_features() -> pd.DataFrame:
    return _load("graph_features", f"{FEATURES_DIR}/graph_features.parquet")


def _account_profiles() -> pd.DataFrame:
    return _load("account_profiles", f"{FEATURES_DIR}/account_profiles.parquet")


def get_transaction_detail(txn_id: str) -> dict:
    """Full transaction record including device, IP, merchant, risk scores, SHAP top features."""
    alerts = _alerts()
    features = _txn_features()

    alert_row = alerts[alerts["txn_id"] == txn_id]
    feat_row = features[features["txn_id"] == txn_id]

    if alert_row.empty:
        return {"error": f"Transaction {txn_id} not found"}

    a = alert_row.iloc[0]
    result = {
        "txn_id": txn_id,
        "account_id": str(a.get("account_id", "")),
        "amount": float(a.get("amount", 0)),
        "timestamp": str(a.get("timestamp", "")),
        "txn_risk_score": float(a.get("txn_risk_score", 0)),
        "behavior_anomaly_score": float(a.get("behavior_anomaly_score", 0)),
        "final_risk_score": float(a.get("final_risk_score", 0)),
        "action": str(a.get("action", "")),
        "risk_band": str(a.get("risk_band", "")),
        "reason_codes": a.get("reason_codes", []),
        "is_fraud": bool(a.get("is_fraud", False)),
        "fraud_scenario": str(a.get("fraud_scenario", "")),
    }

    if not feat_row.empty:
        f = feat_row.iloc[0]
        result.update({
            "device_id": str(f.get("device_id", "")),
            "ip_id": str(f.get("ip_id", "")),
            "merchant_id": str(f.get("merchant_id", "")),
            "beneficiary_id": str(f.get("beneficiary_id", "")),
            "channel": str(f.get("channel", "")),
            "txn_type": str(f.get("txn_type", "")),
            "amount_vs_30d_avg": float(f.get("amount_vs_30d_avg", 0)),
            "is_new_device": bool(f.get("is_new_device", False)),
            "is_new_beneficiary": bool(f.get("is_new_beneficiary", False)),
        })

    return result


def get_account_history(account_id: str, window_days: int = 30) -> dict:
    """Transaction history summary for the account over the given window."""
    alerts = _alerts()
    acct_txns = alerts[alerts["account_id"] == account_id].copy()

    if acct_txns.empty:
        return {"error": f"No transactions found for account {account_id}"}

    acct_txns["timestamp"] = pd.to_datetime(acct_txns["timestamp"])
    latest = acct_txns["timestamp"].max()
    cutoff = latest - pd.Timedelta(days=window_days)
    window_txns = acct_txns[acct_txns["timestamp"] >= cutoff]

    return {
        "account_id": account_id,
        "window_days": window_days,
        "total_transactions": len(window_txns),
        "total_amount": round(float(window_txns["amount"].sum()), 2),
        "avg_amount": round(float(window_txns["amount"].mean()), 2),
        "max_amount": round(float(window_txns["amount"].max()), 2),
        "min_amount": round(float(window_txns["amount"].min()), 2),
        "fraud_count": int(window_txns["is_fraud"].sum()),
        "avg_risk_score": round(float(window_txns["final_risk_score"].mean()), 4),
        "max_risk_score": round(float(window_txns["final_risk_score"].max()), 4),
        "actions": window_txns["action"].value_counts().to_dict(),
        "date_range": f"{cutoff.date()} to {latest.date()}",
    }


def get_velocity_features(account_id: str) -> dict:
    """Current velocity signals for the account."""
    features = _txn_features()
    acct = features[features["account_id"] == account_id]

    if acct.empty:
        return {"error": f"No features found for account {account_id}"}

    # Get latest transaction's features
    latest = acct.sort_values("timestamp").iloc[-1]

    return {
        "account_id": account_id,
        "as_of_txn": str(latest.get("txn_id", "")),
        "txn_count_1h": int(latest.get("txn_count_1h", 0)),
        "txn_count_24h": int(latest.get("txn_count_24h", 0)),
        "txn_count_7d": int(latest.get("txn_count_7d", 0)),
        "amount_sum_1h": round(float(latest.get("amount_sum_1h", 0)), 2),
        "amount_sum_24h": round(float(latest.get("amount_sum_24h", 0)), 2),
        "unique_merchants_7d": int(latest.get("unique_merchants_7d", 0)),
        "unique_beneficiaries_7d": int(latest.get("unique_beneficiaries_7d", 0)),
        "unique_devices_30d": int(latest.get("unique_devices_30d", 0)),
    }
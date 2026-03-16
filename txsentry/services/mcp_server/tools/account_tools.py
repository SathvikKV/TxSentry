"""Account, merchant, and watchlist investigation tools."""

import pandas as pd
import numpy as np
from pathlib import Path

FEATURES_DIR = "data/features"
SYNTHETIC_DIR = "data/synthetic"
ALERTS_DIR = "data/alerts"

_cache = {}


def _load(key, path):
    if key not in _cache:
        if Path(path).exists() or Path(path).is_dir():
            _cache[key] = pd.read_parquet(path)
        else:
            return pd.DataFrame()
    return _cache[key]


def get_behavioral_baseline(account_id: str, window_days: int = 30) -> dict:
    """Account behavioral profile: avg/max/std amounts, typical patterns."""
    profiles = _load("profiles", f"{FEATURES_DIR}/account_profiles.parquet")

    row = profiles[profiles["account_id"] == account_id]
    if row.empty:
        return {"error": f"No profile found for account {account_id}"}

    p = row.iloc[0]
    return {
        "account_id": account_id,
        "total_txn_count": int(p.get("total_txn_count", 0)),
        "avg_amount": round(float(p.get("avg_amount", 0)), 2),
        "max_amount": round(float(p.get("max_amount", 0)), 2),
        "std_amount": round(float(p.get("std_amount", 0)), 2),
        "unique_merchants": int(p.get("unique_merchants", 0)),
        "unique_beneficiaries": int(p.get("unique_beneficiaries", 0)),
        "unique_devices": int(p.get("unique_devices", 0)),
        "avg_txn_count_7d": round(float(p.get("avg_txn_count_7d", 0)), 2),
        "avg_amount_sum_24h": round(float(p.get("avg_amount_sum_24h", 0)), 2),
        "has_prior_fraud": bool(p.get("has_fraud", False)),
    }


def get_merchant_risk_profile(merchant_id: str) -> dict:
    """Merchant category, historical fraud rate, risk tier."""
    merchants = _load("merchants", f"{SYNTHETIC_DIR}/merchant.parquet")

    row = merchants[merchants["merchant_id"] == merchant_id]
    if row.empty:
        return {"error": f"Merchant {merchant_id} not found"}

    m = row.iloc[0]
    fraud_rate = float(m.get("fraud_rate_hist", 0))
    if fraud_rate > 0.04:
        risk_tier = "HIGH"
    elif fraud_rate > 0.015:
        risk_tier = "MEDIUM"
    else:
        risk_tier = "LOW"

    return {
        "merchant_id": merchant_id,
        "name": str(m.get("name", "")),
        "category_code": str(m.get("category_code", "")),
        "country": str(m.get("country", "")),
        "fraud_rate_hist": round(fraud_rate, 4),
        "risk_tier": risk_tier,
    }


def check_watchlist(entity_id: str, entity_type: str = "INDIVIDUAL") -> dict:
    """Check entity against simulated watchlist."""
    watchlist = _load("watchlist", f"{SYNTHETIC_DIR}/watchlist_entity.parquet")

    if watchlist.empty:
        return {"hit": False, "entity_id": entity_id, "reason": None}

    # Simple name/ID matching simulation
    # In practice, check both by ID and fuzzy name match
    hits = watchlist[
        (watchlist["entity_id"] == entity_id) |
        (watchlist["name"].str.contains(entity_id, case=False, na=False))
    ]

    if hits.empty:
        return {"hit": False, "entity_id": entity_id, "reason": None}

    h = hits.iloc[0]
    return {
        "hit": True,
        "entity_id": entity_id,
        "matched_watchlist_id": str(h["entity_id"]),
        "matched_name": str(h["name"]),
        "reason": str(h["reason"]),
        "source": str(h["source"]),
        "listed_at": str(h["listed_at"]),
    }


def run_anomaly_score(account_id: str) -> dict:
    """Get the Isolation Forest anomaly score for an account's recent activity."""
    alerts = _load("alerts", f"{ALERTS_DIR}/alert_events.parquet")

    acct_txns = alerts[alerts["account_id"] == account_id]
    if acct_txns.empty:
        return {"error": f"No transactions found for account {account_id}"}

    latest = acct_txns.sort_values("timestamp").iloc[-1]
    avg_score = float(acct_txns["behavior_anomaly_score"].mean())

    return {
        "account_id": account_id,
        "latest_anomaly_score": round(float(latest.get("behavior_anomaly_score", 0)), 4),
        "avg_anomaly_score": round(avg_score, 4),
        "total_transactions_scored": len(acct_txns),
        "interpretation": (
            "HIGHLY_ANOMALOUS" if avg_score > 0.6
            else "MODERATELY_ANOMALOUS" if avg_score > 0.3
            else "NORMAL"
        ),
    }
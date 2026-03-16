"""Graph neighborhood and pattern detection tools."""

import pandas as pd
import numpy as np
from pathlib import Path

FEATURES_DIR = "data/features"
CANONICAL_DIR = "data/canonical"
ALERTS_DIR = "data/alerts"

_cache = {}


def _load(key, path):
    if key not in _cache:
        if Path(path).exists() or Path(path).is_dir():
            _cache[key] = pd.read_parquet(path)
        else:
            return pd.DataFrame()
    return _cache[key]


def get_graph_neighborhood(entity_id: str, entity_type: str = "account", hops: int = 2) -> dict:
    """Return connected entities within N hops."""
    graph_feats = _load("graph", f"{FEATURES_DIR}/graph_features.parquet")
    txn_feats = _load("txn_features", f"{FEATURES_DIR}/txn_features.parquet")

    if entity_type == "account":
        # Find devices and beneficiaries directly connected
        acct_txns = txn_feats[txn_feats["account_id"] == entity_id]
        if acct_txns.empty:
            return {"error": f"Entity {entity_id} not found in transaction data"}

        devices = acct_txns["device_id"].dropna().unique().tolist()[:20]
        beneficiaries = acct_txns["beneficiary_id"].dropna().unique().tolist()[:20]
        ips = acct_txns["ip_id"].dropna().unique().tolist()[:20]
        merchants = acct_txns["merchant_id"].dropna().unique().tolist()[:10]

        # Hop 2: find other accounts sharing those devices
        shared_accounts = []
        if hops >= 2 and devices:
            for dev in devices[:5]:
                dev_txns = txn_feats[txn_feats["device_id"] == dev]
                other_accts = dev_txns["account_id"].unique()
                other_accts = [a for a in other_accts if a != entity_id][:10]
                shared_accounts.extend(other_accts)
            shared_accounts = list(set(shared_accounts))[:20]

        # Get graph features for this account
        acct_graph = graph_feats[graph_feats["account_id"] == entity_id]
        graph_info = {}
        if not acct_graph.empty:
            g = acct_graph.iloc[0]
            graph_info = {
                "account_degree": int(g.get("account_degree", 0)),
                "device_shared_account_count": int(g.get("device_shared_account_count", 0)),
                "beneficiary_in_degree": int(g.get("beneficiary_in_degree", 0)),
                "community_id": int(g.get("community_id", 0)),
                "community_size": int(g.get("community_size", 0)),
                "community_fraud_rate": round(float(g.get("community_fraud_rate", 0)), 4),
                "graph_risk_score": round(float(g.get("graph_risk_score", 0)), 4),
            }

        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "hops": hops,
            "connected_devices": devices,
            "connected_beneficiaries": beneficiaries,
            "connected_ips": ips,
            "connected_merchants": merchants,
            "shared_device_accounts": shared_accounts,
            "graph_features": graph_info,
            "total_neighbors": len(devices) + len(beneficiaries) + len(ips),
        }

    elif entity_type == "device":
        dev_txns = txn_feats[txn_feats["device_id"] == entity_id]
        if dev_txns.empty:
            return {"error": f"Device {entity_id} not found"}

        accounts = dev_txns["account_id"].unique().tolist()[:20]
        return {
            "entity_id": entity_id,
            "entity_type": "device",
            "accounts_using_device": accounts,
            "account_count": len(accounts),
            "is_shared": len(accounts) > 1,
        }

    return {"error": f"Unsupported entity_type: {entity_type}"}


def detect_graph_pattern(account_id: str) -> dict:
    """Check if account's graph substructure matches known AML topologies."""
    topo = _load("topo", f"{FEATURES_DIR}/topology_flags.parquet")
    txn_feats = _load("txn_features", f"{FEATURES_DIR}/txn_features.parquet")
    graph_feats = _load("graph", f"{FEATURES_DIR}/graph_features.parquet")

    # Get topology flags for this account's transactions
    acct_txns = txn_feats[txn_feats["account_id"] == account_id]
    if acct_txns.empty:
        return {"error": f"Account {account_id} not found"}

    txn_ids = set(acct_txns["txn_id"].values)
    acct_topo = topo[topo["txn_id"].isin(txn_ids)]

    patterns_detected = []
    evidence = []
    confidence = 0.0

    if not acct_topo.empty:
        if acct_topo["is_fan_out_source"].any():
            patterns_detected.append("FAN_OUT")
            n = acct_topo["is_fan_out_source"].sum()
            evidence.append(f"Account is source of fan-out pattern ({n} flagged txns)")
            confidence = max(confidence, 0.8)

        if acct_topo["is_fan_in_target"].any():
            patterns_detected.append("FAN_IN")
            n = acct_topo["is_fan_in_target"].sum()
            evidence.append(f"Account's beneficiaries show fan-in pattern ({n} flagged txns)")
            confidence = max(confidence, 0.7)

        if acct_topo["is_shared_device"].any():
            patterns_detected.append("SHARED_DEVICE_RING")
            n = acct_topo["is_shared_device"].sum()
            evidence.append(f"Account uses shared device ({n} flagged txns)")
            confidence = max(confidence, 0.6)

        if acct_topo["structuring_flag"].any():
            patterns_detected.append("STRUCTURING")
            n = acct_topo["structuring_flag"].sum()
            evidence.append(f"Structuring pattern detected ({n} flagged txns)")
            confidence = max(confidence, 0.85)

        if acct_topo["is_mule_chain_member"].any():
            patterns_detected.append("MULE_CHAIN")
            n = acct_topo["is_mule_chain_member"].sum()
            evidence.append(f"Account appears in mule chain pattern ({n} flagged txns)")
            confidence = max(confidence, 0.75)

    # Add graph-level info
    acct_graph = graph_feats[graph_feats["account_id"] == account_id]
    if not acct_graph.empty:
        g = acct_graph.iloc[0]
        if g.get("device_shared_account_count", 0) > 3:
            evidence.append(f"Device shared with {int(g['device_shared_account_count'])} other accounts")
        if g.get("community_fraud_rate", 0) > 0.05:
            evidence.append(f"Community fraud rate: {g['community_fraud_rate']:.2%}")

    return {
        "account_id": account_id,
        "patterns_detected": patterns_detected if patterns_detected else None,
        "confidence": round(confidence, 2),
        "supporting_evidence": evidence,
        "total_transactions_checked": len(acct_topo),
    }
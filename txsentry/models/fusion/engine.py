"""Module 5: Fusion Engine.

Combines LightGBM risk score, Isolation Forest anomaly score, and graph risk score
with rule-based boosters into a final risk decision with reason codes.

Usage:
    python -m txsentry.models.fusion.engine
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"


# --- Input Schema ---

@dataclass
class FusionInput:
    txn_id: str
    account_id: str
    txn_risk_score: float           # 0-1 from LightGBM
    behavior_anomaly_score: float   # 0-1 from Isolation Forest
    graph_risk_score: float         # 0-1 from graph features
    # Rule signal flags
    is_new_device: bool = False
    is_new_beneficiary: bool = False
    watchlist_hit: bool = False
    is_fan_out_source: bool = False
    is_fan_in_target: bool = False
    is_shared_device: bool = False
    structuring_flag: bool = False
    is_mule_chain_member: bool = False
    amount_vs_30d_avg: float = 1.0
    merchant_fraud_rate_hist: float = 0.0
    ip_is_vpn: bool = False


# --- Core Fusion Logic ---

def compute_final_score(inp: FusionInput) -> float:
    """Weighted combination of model scores with rule-based boosters."""
    score = (
        0.50 * inp.txn_risk_score +
        0.25 * inp.behavior_anomaly_score +
        0.25 * inp.graph_risk_score
    )
    # Hard boosters for high-confidence signals
    if inp.watchlist_hit:
        score = min(1.0, score + 0.30)
    if inp.structuring_flag:
        score = min(1.0, score + 0.20)
    if inp.is_shared_device and inp.is_new_beneficiary:
        score = min(1.0, score + 0.15)
    if inp.is_fan_out_source:
        score = min(1.0, score + 0.10)
    if inp.is_mule_chain_member and inp.amount_vs_30d_avg > 3.0:
        score = min(1.0, score + 0.10)
    if inp.ip_is_vpn and inp.is_new_device:
        score = min(1.0, score + 0.10)
    return float(np.clip(score, 0.0, 1.0))


def assign_action(score: float) -> str:
    """Map final risk score to decision action."""
    if score >= 0.85:
        return "BLOCK"
    if score >= 0.70:
        return "QUEUE_FOR_REVIEW"
    if score >= 0.55:
        return "STEP_UP_AUTH"
    if score >= 0.35:
        return "ALLOW_WITH_MONITORING"
    return "ALLOW"


def assign_risk_band(score: float) -> str:
    """Map final risk score to a risk band for display."""
    if score >= 0.85:
        return "CRITICAL"
    if score >= 0.70:
        return "HIGH"
    if score >= 0.55:
        return "ELEVATED"
    if score >= 0.35:
        return "MEDIUM"
    return "LOW"


def compute_reason_codes(inp: FusionInput) -> list[str]:
    """Generate list of reason codes explaining why a transaction was flagged."""
    codes = []
    if inp.is_new_device and inp.amount_vs_30d_avg > 2.0:
        codes.append("NEW_DEVICE_HIGH_VALUE_TXN")
    if inp.is_shared_device:
        codes.append("SHARED_DEVICE_CLUSTER")
    if inp.structuring_flag:
        codes.append("STRUCTURING_PATTERN")
    if inp.is_fan_out_source:
        codes.append("FAN_OUT_PATTERN")
    if inp.is_fan_in_target:
        codes.append("FAN_IN_PATTERN")
    if inp.amount_vs_30d_avg > 4.0:
        codes.append("AMOUNT_4X_BASELINE")
    if inp.watchlist_hit:
        codes.append("WATCHLIST_HIT")
    if inp.ip_is_vpn and inp.is_new_beneficiary:
        codes.append("IP_COUNTRY_MISMATCH")
    if inp.merchant_fraud_rate_hist > 0.05:
        codes.append("HIGH_RISK_MERCHANT")
    if inp.is_mule_chain_member:
        codes.append("MULE_CHAIN_DETECTED")
    if inp.is_new_beneficiary and inp.amount_vs_30d_avg > 2.0:
        codes.append("NEW_PAYEE_LARGE_TXN")
    if inp.amount_vs_30d_avg > 1.0 and inp.behavior_anomaly_score > 0.5:
        # Account was dormant or shows unusual velocity
        if inp.txn_risk_score > 0.3:
            codes.append("BURST_VELOCITY")
    return codes


# --- Vectorized Batch Processing ---

def run_fusion_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Apply fusion logic to a DataFrame with all required columns.

    Vectorized where possible, falls back to row-level for reason codes.
    """
    logger.info(f"Running fusion on {len(df):,} transactions...")

    # --- Vectorized score computation ---
    base_score = (
        0.50 * df["txn_risk_score"].fillna(0) +
        0.25 * df["behavior_anomaly_score"].fillna(0) +
        0.25 * df["graph_risk_score"].fillna(0)
    )

    # Apply boosters vectorized
    boost = pd.Series(0.0, index=df.index)

    if "structuring_flag" in df.columns:
        boost += df["structuring_flag"].astype(float) * 0.20
    if "is_shared_device" in df.columns and "is_new_beneficiary" in df.columns:
        boost += (df["is_shared_device"] & df["is_new_beneficiary"]).astype(float) * 0.15
    if "is_fan_out_source" in df.columns:
        boost += df["is_fan_out_source"].astype(float) * 0.10
    if "is_mule_chain_member" in df.columns and "amount_vs_30d_avg" in df.columns:
        boost += (df["is_mule_chain_member"] & (df["amount_vs_30d_avg"] > 3.0)).astype(float) * 0.10
    if "ip_is_vpn" in df.columns and "is_new_device" in df.columns:
        boost += (df["ip_is_vpn"] & df["is_new_device"]).astype(float) * 0.10

    df = df.copy()
    df["final_risk_score"] = np.clip(base_score + boost, 0.0, 1.0)

    # --- Vectorized action assignment ---
    conditions = [
        df["final_risk_score"] >= 0.85,
        df["final_risk_score"] >= 0.70,
        df["final_risk_score"] >= 0.55,
        df["final_risk_score"] >= 0.35,
    ]
    choices = ["BLOCK", "QUEUE_FOR_REVIEW", "STEP_UP_AUTH", "ALLOW_WITH_MONITORING"]
    df["action"] = np.select(conditions, choices, default="ALLOW")

    # --- Vectorized risk band ---
    band_conditions = [
        df["final_risk_score"] >= 0.85,
        df["final_risk_score"] >= 0.70,
        df["final_risk_score"] >= 0.55,
        df["final_risk_score"] >= 0.35,
    ]
    band_choices = ["CRITICAL", "HIGH", "ELEVATED", "MEDIUM"]
    df["risk_band"] = np.select(band_conditions, band_choices, default="LOW")

    # --- Reason codes (row-level, only for non-ALLOW transactions) ---
    logger.info("  Computing reason codes for flagged transactions...")
    non_allow_mask = df["action"] != "ALLOW"
    n_flagged = non_allow_mask.sum()
    logger.info(f"  {n_flagged:,} transactions need reason codes")

    reason_codes_list = [[] for _ in range(len(df))]
    if n_flagged > 0:
        flagged_idx = df.index[non_allow_mask]
        for idx in flagged_idx:
            row = df.loc[idx]
            codes = []
            if row.get("is_new_device", False) and row.get("amount_vs_30d_avg", 1.0) > 2.0:
                codes.append("NEW_DEVICE_HIGH_VALUE_TXN")
            if row.get("is_shared_device", False):
                codes.append("SHARED_DEVICE_CLUSTER")
            if row.get("structuring_flag", False):
                codes.append("STRUCTURING_PATTERN")
            if row.get("is_fan_out_source", False):
                codes.append("FAN_OUT_PATTERN")
            if row.get("is_fan_in_target", False):
                codes.append("FAN_IN_PATTERN")
            if row.get("amount_vs_30d_avg", 1.0) > 4.0:
                codes.append("AMOUNT_4X_BASELINE")
            if row.get("ip_is_vpn", False) and row.get("is_new_beneficiary", False):
                codes.append("IP_COUNTRY_MISMATCH")
            if row.get("merchant_fraud_rate_hist", 0.0) > 0.05:
                codes.append("HIGH_RISK_MERCHANT")
            if row.get("is_mule_chain_member", False):
                codes.append("MULE_CHAIN_DETECTED")
            if not codes:
                codes.append("ELEVATED_RISK_SCORE")
            reason_codes_list[idx] = codes

    df["reason_codes"] = reason_codes_list

    return df


def run():
    """Run fusion engine on scored transactions."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting Fusion Engine")
    logger.info("=" * 60)

    # Load scored transactions
    scored_path = f"{ALERTS_DIR}/scored_transactions.parquet"
    logger.info(f"Loading scored transactions from {scored_path}...")
    scored_df = pd.read_parquet(scored_path)
    logger.info(f"  Loaded {len(scored_df):,} transactions")

    # Load feature tables for rule signals
    logger.info("Loading feature tables for rule signals...")

    # Transaction features (for amount_vs_30d_avg, is_new_device, etc.)
    txn_features = pd.read_parquet(
        f"{FEATURES_DIR}/txn_features.parquet",
        columns=[
            "txn_id", "is_new_device", "is_new_beneficiary",
            "amount_vs_30d_avg", "merchant_fraud_rate_hist", "ip_is_vpn",
        ]
    )

    # Graph features (for graph_risk_score)
    graph_features = pd.read_parquet(
        f"{FEATURES_DIR}/graph_features.parquet",
        columns=["account_id", "graph_risk_score"]
    )

    # Topology flags
    topo_flags = pd.read_parquet(
        f"{FEATURES_DIR}/topology_flags.parquet"
    )

    # Merge everything
    logger.info("Merging all signals...")
    df = scored_df.merge(txn_features, on="txn_id", how="left")
    df = df.merge(graph_features, on="account_id", how="left")
    df = df.merge(topo_flags, on="txn_id", how="left")

    # Fill missing values
    df["graph_risk_score"] = df["graph_risk_score"].fillna(0.0)
    df["amount_vs_30d_avg"] = df["amount_vs_30d_avg"].fillna(1.0)
    df["merchant_fraud_rate_hist"] = df["merchant_fraud_rate_hist"].fillna(0.0)
    for col in ["is_new_device", "is_new_beneficiary", "ip_is_vpn",
                 "is_fan_out_source", "is_fan_in_target", "is_shared_device",
                 "structuring_flag", "is_mule_chain_member"]:
        if col in df.columns:
            df[col] = df[col].fillna(False)

    logger.info(f"  Merged dataset: {len(df):,} rows")

    # Run fusion
    df = run_fusion_batch(df)

    # --- Build alert table ---
    logger.info("\nBuilding alert table...")
    alert_df = df[[
        "txn_id", "account_id", "timestamp", "amount",
        "is_fraud", "fraud_scenario",
        "txn_risk_score", "behavior_anomaly_score", "graph_risk_score",
        "final_risk_score", "risk_band", "action", "reason_codes",
    ]].copy()

    alert_df["alert_id"] = ["ALT_" + str(i).zfill(8) for i in range(len(alert_df))]
    alert_df["triggered_at"] = alert_df["timestamp"]

    # Save full alert table
    Path(ALERTS_DIR).mkdir(parents=True, exist_ok=True)
    alert_df.to_parquet(
        f"{ALERTS_DIR}/alert_events.parquet",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    # --- Summary ---
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("FUSION ENGINE REPORT")
    logger.info("=" * 60)

    logger.info(f"Total transactions processed: {len(df):,}")

    action_dist = df["action"].value_counts()
    logger.info(f"\nAction distribution:")
    for action, count in action_dist.items():
        pct = count / len(df) * 100
        logger.info(f"  {action:25s} {count:>10,} ({pct:.2f}%)")

    risk_band_dist = df["risk_band"].value_counts()
    logger.info(f"\nRisk band distribution:")
    for band, count in risk_band_dist.items():
        pct = count / len(df) * 100
        logger.info(f"  {band:25s} {count:>10,} ({pct:.2f}%)")

    # Precision of each action tier (among actual fraud)
    logger.info(f"\nPrecision by action (fraud rate within tier):")
    for action in ["BLOCK", "QUEUE_FOR_REVIEW", "STEP_UP_AUTH", "ALLOW_WITH_MONITORING", "ALLOW"]:
        tier = df[df["action"] == action]
        if len(tier) > 0:
            fraud_rate = tier["is_fraud"].mean()
            fraud_count = tier["is_fraud"].sum()
            logger.info(f"  {action:25s} {fraud_rate:.2%} fraud ({fraud_count:,}/{len(tier):,})")

    # Reason code frequency
    all_codes = [code for codes in df["reason_codes"] for code in codes]
    if all_codes:
        code_counts = pd.Series(all_codes).value_counts()
        logger.info(f"\nTop reason codes:")
        for code, count in code_counts.head(10).items():
            logger.info(f"  {code:35s} {count:>10,}")

    logger.info(f"\nFinal risk score distribution:")
    score = df["final_risk_score"]
    logger.info(f"  mean={score.mean():.4f}, p50={score.median():.4f}, "
                f"p95={score.quantile(0.95):.4f}, p99={score.quantile(0.99):.4f}")

    logger.info(f"\nOutput: {ALERTS_DIR}/alert_events.parquet ({len(alert_df):,} rows)")
    size_mb = Path(f"{ALERTS_DIR}/alert_events.parquet").stat().st_size / (1024 * 1024)
    logger.info(f"  Size: {size_mb:.1f} MB")

    logger.info(f"\nPipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
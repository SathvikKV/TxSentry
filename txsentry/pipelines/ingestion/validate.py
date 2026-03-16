"""Step 4: Validate canonical data and produce an ingestion report."""

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Columns that must have zero nulls for downstream feature engineering
CRITICAL_COLUMNS = ["txn_id", "account_id", "amount", "timestamp", "is_fraud"]

# Columns where some nulls are acceptable but should be flagged if > 5%
MONITORED_COLUMNS = [
    "txn_id", "account_id", "merchant_id", "beneficiary_id",
    "device_id", "ip_id", "amount", "currency", "txn_type",
    "channel", "timestamp", "is_fraud",
]


def validate_canonical(
    txn_df: pd.DataFrame,
    device_df: pd.DataFrame,
    ip_df: pd.DataFrame,
    output_path: str = "data/canonical/ingestion_report.json",
) -> dict:
    """Run validation checks on canonical data and write a report.

    Raises ValueError if any critical column has nulls.
    Logs warnings if monitored columns exceed 5% null rate.
    """
    report = {}

    # --- Record counts ---
    report["record_counts"] = {
        "transactions": len(txn_df),
        "unique_accounts": txn_df["account_id"].nunique(),
        "devices": len(device_df),
        "ips": len(ip_df),
    }

    # --- Null rates ---
    null_rates = {}
    errors = []
    warnings = []

    for col in MONITORED_COLUMNS:
        if col not in txn_df.columns:
            continue
        null_rate = txn_df[col].isna().mean()
        null_rates[col] = round(float(null_rate), 4)

        if col in CRITICAL_COLUMNS and null_rate > 0:
            errors.append(f"CRITICAL: Column '{col}' has {null_rate:.2%} nulls — must be zero")
        elif null_rate > 0.05:
            warnings.append(f"WARNING: Column '{col}' has {null_rate:.2%} nulls (> 5% threshold)")

    report["null_rates"] = null_rates

    # --- Fraud distribution ---
    fraud_counts = txn_df.groupby("source")["is_fraud"].agg(["sum", "count"])
    fraud_dist = {}
    for source, row in fraud_counts.iterrows():
        fraud_dist[source] = {
            "fraud_count": int(row["sum"]),
            "total_count": int(row["count"]),
            "fraud_rate": round(float(row["sum"] / row["count"]), 4),
        }
    report["fraud_distribution_by_source"] = fraud_dist

    # --- Fraud by scenario ---
    scenario_counts = txn_df[txn_df["is_fraud"]]["fraud_scenario"].value_counts().to_dict()
    report["fraud_by_scenario"] = {k: int(v) for k, v in scenario_counts.items()}

    # --- Timestamp range ---
    report["timestamp_range"] = {
        "min": str(txn_df["timestamp"].min()),
        "max": str(txn_df["timestamp"].max()),
    }

    # --- Device/IP coverage ---
    device_coverage = 1 - txn_df["device_id"].isna().mean()
    ip_coverage = 1 - txn_df["ip_id"].isna().mean()
    report["device_assignment_coverage"] = round(float(device_coverage), 4)
    report["ip_assignment_coverage"] = round(float(ip_coverage), 4)

    # --- Uniqueness check ---
    txn_id_unique = txn_df["txn_id"].is_unique
    report["txn_id_unique"] = bool(txn_id_unique)
    if not txn_id_unique:
        errors.append("CRITICAL: Duplicate txn_ids found")

    report["errors"] = errors
    report["warnings"] = warnings

    # --- Log results ---
    logger.info("=" * 60)
    logger.info("INGESTION VALIDATION REPORT")
    logger.info("=" * 60)
    logger.info(f"Transactions:    {report['record_counts']['transactions']:,}")
    logger.info(f"Unique accounts: {report['record_counts']['unique_accounts']:,}")
    logger.info(f"Devices:         {report['record_counts']['devices']:,}")
    logger.info(f"IPs:             {report['record_counts']['ips']:,}")
    logger.info(f"Timestamp range: {report['timestamp_range']['min']} to {report['timestamp_range']['max']}")
    logger.info(f"Device coverage: {device_coverage:.1%}")
    logger.info(f"IP coverage:     {ip_coverage:.1%}")
    logger.info(f"txn_id unique:   {txn_id_unique}")

    logger.info("\nFraud distribution by source:")
    for source, stats in fraud_dist.items():
        logger.info(f"  {source}: {stats['fraud_count']:,} / {stats['total_count']:,} ({stats['fraud_rate']:.2%})")

    if warnings:
        logger.info("\nWarnings:")
        for w in warnings:
            logger.warning(f"  {w}")

    if errors:
        logger.info("\nErrors:")
        for e in errors:
            logger.error(f"  {e}")

    logger.info("=" * 60)

    # --- Write report ---
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Report written to {output_path}")

    # Raise on critical errors
    if errors:
        raise ValueError(f"Ingestion validation failed with {len(errors)} error(s):\n" + "\n".join(errors))

    return report
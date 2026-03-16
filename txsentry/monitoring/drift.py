"""Module 10: Model Monitoring — PSI drift detection.

Computes Population Stability Index (PSI) monthly for key features
and generates a monitoring report.

Usage:
    python -m txsentry.monitoring.drift
"""

import logging
import time
import json
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
MODELS_DIR = "data/models"

TRAIN_CUTOFF = pd.Timestamp("2024-10-01")

# Features to monitor
MONITORED_FEATURES = [
    "txn_risk_score",
    "behavior_anomaly_score",
    "final_risk_score",
    "amount",
]

PSI_THRESHOLD = 0.2  # >0.2 = significant drift


def compute_psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Compute Population Stability Index between two distributions.

    PSI < 0.1: stable
    0.1 - 0.2: moderate shift
    > 0.2: significant drift — consider retraining
    """
    # Bin the expected distribution
    breakpoints = np.linspace(0, 1, bins + 1)

    # Use percentiles if data isn't 0-1 range
    if expected.max() > 1 or expected.min() < 0:
        breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
        breakpoints[0] = -np.inf
        breakpoints[-1] = np.inf

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    # Convert to proportions, avoid zeros
    expected_pct = (expected_counts + 1) / (expected_counts.sum() + bins)
    actual_pct = (actual_counts + 1) / (actual_counts.sum() + bins)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


def run():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting Model Monitoring — PSI Drift Detection")
    logger.info("=" * 60)

    # Load alert data with scores
    alerts = pd.read_parquet(f"{ALERTS_DIR}/alert_events.parquet")
    alerts["timestamp"] = pd.to_datetime(alerts["timestamp"])
    logger.info(f"Loaded {len(alerts):,} scored transactions")

    # Split into training reference and production windows
    train_ref = alerts[alerts["timestamp"] < TRAIN_CUTOFF]
    production = alerts[alerts["timestamp"] >= TRAIN_CUTOFF]

    logger.info(f"Training reference: {len(train_ref):,} transactions")
    logger.info(f"Production window:  {len(production):,} transactions")

    if production.empty:
        logger.warning("No production data found. Cannot compute drift.")
        return

    # Compute monthly PSI
    production["month"] = production["timestamp"].dt.to_period("M")
    months = sorted(production["month"].unique())

    psi_results = {}
    logger.info(f"\nMonthly PSI (threshold = {PSI_THRESHOLD}):")
    logger.info("-" * 70)

    for feature in MONITORED_FEATURES:
        if feature not in alerts.columns:
            logger.warning(f"  Feature '{feature}' not found in data — skipping")
            continue

        ref_values = train_ref[feature].dropna().values
        if len(ref_values) < 100:
            logger.warning(f"  Insufficient reference data for '{feature}'")
            continue

        feature_psi = {}
        for month in months:
            month_data = production[production["month"] == month][feature].dropna().values
            if len(month_data) < 50:
                continue
            psi = compute_psi(ref_values, month_data)
            feature_psi[str(month)] = round(psi, 4)

        psi_results[feature] = feature_psi

        # Log results
        logger.info(f"\n  {feature}:")
        for month, psi in feature_psi.items():
            flag = " ⚠️  DRIFT" if psi > PSI_THRESHOLD else ""
            logger.info(f"    {month}: PSI = {psi:.4f}{flag}")

    # Precision degradation over time
    logger.info(f"\nPrecision by month (fraud rate in top 500 scores/month):")
    precision_monthly = {}
    for month in months:
        month_df = production[production["month"] == month]
        if len(month_df) < 500:
            continue
        top_500 = month_df.nlargest(500, "final_risk_score")
        precision = float(top_500["is_fraud"].mean())
        precision_monthly[str(month)] = round(precision, 4)
        logger.info(f"  {month}: {precision:.2%}")

    # Fraud rate by month
    fraud_rate_monthly = {}
    for month in months:
        month_df = production[production["month"] == month]
        rate = float(month_df["is_fraud"].mean())
        fraud_rate_monthly[str(month)] = round(rate, 4)

    # Retraining recommendation
    drift_features = []
    for feature, monthly_psi in psi_results.items():
        high_psi_months = [m for m, p in monthly_psi.items() if p > PSI_THRESHOLD]
        if len(high_psi_months) >= 2:
            drift_features.append(feature)

    precision_drop = False
    if precision_monthly:
        values = list(precision_monthly.values())
        if len(values) >= 2 and values[-1] < values[0] * 0.9:
            precision_drop = True

    needs_retrain = len(drift_features) >= 2 or precision_drop

    # Write report
    report = {
        "psi_by_feature": psi_results,
        "precision_monthly": precision_monthly,
        "fraud_rate_monthly": fraud_rate_monthly,
        "drift_features": drift_features,
        "precision_degradation": precision_drop,
        "retraining_recommended": needs_retrain,
        "psi_threshold": PSI_THRESHOLD,
    }

    report_path = f"{MODELS_DIR}/monitoring_report.json"
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    # Summary
    elapsed = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info("MONITORING REPORT SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Features monitored: {len(MONITORED_FEATURES)}")
    logger.info(f"Features with drift: {drift_features if drift_features else 'None'}")
    logger.info(f"Precision degradation: {'Yes' if precision_drop else 'No'}")
    logger.info(f"Retraining recommended: {'YES ⚠️' if needs_retrain else 'No'}")
    logger.info(f"\nReport saved to {report_path}")
    logger.info(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
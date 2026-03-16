"""Apply temporal drift to fraud patterns across 12-month synthetic window.

Drift affects: fraud scenario mix, fraud rate, and average fraud amounts.
This enables the model monitoring module to detect degradation.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Drift schedule per quarter
DRIFT_SCHEDULE = {
    "Q1": {
        "months": (2, 3, 4),  # Feb-Apr 2024
        "base_fraud_rate": 0.018,
        "dominant_scenarios": ["MULE_CHAIN", "FAN_OUT"],
        "scenario_weights": {
            "MULE_CHAIN": 0.30,
            "FAN_OUT": 0.25,
            "FAN_IN": 0.10,
            "ACCOUNT_TAKEOVER_BURST": 0.10,
            "SHARED_DEVICE_RING": 0.10,
            "STRUCTURING": 0.15,
        },
        "amount_multiplier": 1.0,
    },
    "Q2": {
        "months": (5, 6, 7),  # May-Jul 2024
        "base_fraud_rate": 0.022,
        "dominant_scenarios": ["ACCOUNT_TAKEOVER_BURST", "SHARED_DEVICE_RING"],
        "scenario_weights": {
            "MULE_CHAIN": 0.10,
            "FAN_OUT": 0.10,
            "FAN_IN": 0.15,
            "ACCOUNT_TAKEOVER_BURST": 0.30,
            "SHARED_DEVICE_RING": 0.25,
            "STRUCTURING": 0.10,
        },
        "amount_multiplier": 1.2,
    },
    "Q3": {
        "months": (8, 9, 10),  # Aug-Oct 2024
        "base_fraud_rate": 0.025,
        "dominant_scenarios": ["STRUCTURING", "FAN_IN"],
        "scenario_weights": {
            "MULE_CHAIN": 0.10,
            "FAN_OUT": 0.10,
            "FAN_IN": 0.25,
            "ACCOUNT_TAKEOVER_BURST": 0.10,
            "SHARED_DEVICE_RING": 0.10,
            "STRUCTURING": 0.35,
        },
        "amount_multiplier": 1.4,
    },
    "Q4": {
        "months": (11, 12, 1),  # Nov 2024 - Jan 2025
        "base_fraud_rate": 0.030,
        "dominant_scenarios": ["MULE_CHAIN", "ACCOUNT_TAKEOVER_BURST"],
        "scenario_weights": {
            "MULE_CHAIN": 0.30,
            "FAN_OUT": 0.05,
            "FAN_IN": 0.10,
            "ACCOUNT_TAKEOVER_BURST": 0.30,
            "SHARED_DEVICE_RING": 0.10,
            "STRUCTURING": 0.15,
        },
        "amount_multiplier": 1.6,
    },
}


def get_quarter_for_month(month: int) -> str:
    for qname, qdata in DRIFT_SCHEDULE.items():
        if month in qdata["months"]:
            return qname
    return "Q1"


def get_monthly_fraud_config(month: int) -> dict:
    """Get fraud generation parameters for a given month."""
    quarter = get_quarter_for_month(month)
    config = DRIFT_SCHEDULE[quarter]
    return {
        "fraud_rate": config["base_fraud_rate"],
        "scenario_weights": config["scenario_weights"],
        "amount_multiplier": config["amount_multiplier"],
        "dominant_scenarios": config["dominant_scenarios"],
        "quarter": quarter,
    }


def get_month_boundaries(month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Get start and end timestamps for a given month (1-12 mapped to Feb 2024 - Jan 2025)."""
    if month <= 11:
        year = 2024
        cal_month = month + 1  # month 1 -> Feb, month 11 -> Dec
    else:
        year = 2025
        cal_month = 1  # month 12 -> Jan 2025

    start = pd.Timestamp(year=year, month=cal_month, day=1)
    if cal_month == 12:
        end = pd.Timestamp(year=year + 1, month=1, day=1) - pd.Timedelta(seconds=1)
    else:
        end = pd.Timestamp(year=year, month=cal_month + 1, day=1) - pd.Timedelta(seconds=1)

    return start, end


def log_drift_schedule():
    """Log the full drift schedule for visibility."""
    logger.info("Temporal drift schedule:")
    for qname, qdata in DRIFT_SCHEDULE.items():
        months_str = ", ".join(str(m) for m in qdata["months"])
        dominant = ", ".join(qdata["dominant_scenarios"])
        logger.info(
            f"  {qname} (months {months_str}): "
            f"fraud_rate={qdata['base_fraud_rate']:.1%}, "
            f"amount_mult={qdata['amount_multiplier']:.1f}x, "
            f"dominant=[{dominant}]"
        )
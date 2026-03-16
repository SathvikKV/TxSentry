"""Transform staged PaySim data into TxSentry canonical schema."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_paysim(df: pd.DataFrame) -> pd.DataFrame:
    """Map PaySim columns to the canonical transaction_event schema.

    PaySim has no device, IP, or merchant data — those fields are left null
    and filled by the synthetic device/IP assignment step.
    """
    logger.info(f"Transforming {len(df)} PaySim records to canonical schema")

    canonical = pd.DataFrame({
        "txn_id": ["TXN_PS_" + str(i) for i in range(len(df))],
        "account_id": "ACC_" + df["nameOrig"].astype(str),
        "merchant_id": None,
        "beneficiary_id": "ACC_" + df["nameDest"].astype(str),
        "device_id": None,
        "ip_id": None,
        "amount": df["amount"].values,
        "currency": "USD",
        "txn_type": df["type"].values,
        "channel": "MOBILE",
        "timestamp": pd.Timestamp("2024-01-01") + pd.to_timedelta(df["step"], unit="h"),
        "is_fraud": df["isFraud"].astype(bool).values,
        "fraud_scenario": df["isFraud"].map({1: "PAYSIM_FRAUD", 0: None}).values,
        "source": "PAYSIM",
    })

    fraud_count = canonical["is_fraud"].sum()
    fraud_rate = fraud_count / len(canonical) * 100
    logger.info(
        f"PaySim transform complete: {len(canonical)} txns, "
        f"{fraud_count} fraud ({fraud_rate:.2f}%)"
    )

    ts_range = canonical["timestamp"]
    logger.info(f"Timestamp range: {ts_range.min()} to {ts_range.max()}")

    return canonical
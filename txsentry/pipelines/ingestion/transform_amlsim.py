"""Transform staged AMLSim data into TxSentry canonical schema."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Map AMLSim alert reasons to TxSentry fraud scenarios
REASON_TO_SCENARIO = {
    "fan_in": "FAN_IN",
    "fan_out": "FAN_OUT",
    "cycle": "MULE_CHAIN",
}


def transform_amlsim_transactions(
    txn_df: pd.DataFrame,
    alert_accounts_df: pd.DataFrame,
) -> pd.DataFrame:
    """Map AMLSim transaction log to canonical transaction_event schema.

    AMLSim has no device, IP, or merchant data — those are filled
    by the synthetic device/IP assignment step.
    """
    logger.info(f"Transforming {len(txn_df)} AMLSim records to canonical schema")

    # Build set of SAR account IDs from alert_accounts
    sar_accounts = set(
        alert_accounts_df.loc[
            alert_accounts_df["isSAR"] == True, "accountID"
        ].astype(str).values
    )
    logger.info(f"Found {len(sar_accounts)} SAR-flagged accounts")

    # Build account -> alert reason mapping for fraud scenario labels
    acct_to_reason = {}
    for _, row in alert_accounts_df[alert_accounts_df["isSAR"] == True].iterrows():
        acct_id = str(row["accountID"])
        reason = str(row["reason"]).lower()
        acct_to_reason[acct_id] = REASON_TO_SCENARIO.get(reason, "AMLSIM_AML")

    # Build fraud labels per transaction
    orig_accounts = txn_df["nameOrig"].astype(str)
    is_sar_col = txn_df["isSAR"].astype(int)

    # A transaction is fraud if isSAR=1 OR originator is a SAR account
    is_fraud = (is_sar_col == 1) | orig_accounts.isin(sar_accounts)

    # Assign fraud scenario based on originator's alert reason
    fraud_scenarios = orig_accounts.map(acct_to_reason)
    # For isSAR=1 transactions without a mapped reason, default to AMLSIM_AML
    fraud_scenarios = fraud_scenarios.where(is_fraud, other=None)
    fraud_scenarios = fraud_scenarios.fillna("AMLSIM_AML").where(is_fraud, other=None)

    # Convert step (integer hours from base date) to timestamps
    base_date = pd.Timestamp("2024-01-01")
    timestamps = base_date + pd.to_timedelta(txn_df["step"], unit="h")

    canonical = pd.DataFrame({
        "txn_id": ["TXN_AML_" + str(i) for i in range(len(txn_df))],
        "account_id": "ACC_AML_" + orig_accounts,
        "merchant_id": None,
        "beneficiary_id": "ACC_AML_" + txn_df["nameDest"].astype(str),
        "device_id": None,
        "ip_id": None,
        "amount": txn_df["amount"].values,
        "currency": "USD",
        "txn_type": txn_df["type"].values,
        "channel": "ONLINE",
        "timestamp": timestamps.values,
        "is_fraud": is_fraud.values,
        "fraud_scenario": fraud_scenarios.values,
        "source": "AMLSIM",
    })

    fraud_count = canonical["is_fraud"].sum()
    fraud_rate = fraud_count / len(canonical) * 100
    logger.info(
        f"AMLSim transform complete: {len(canonical)} txns, "
        f"{fraud_count} fraud ({fraud_rate:.2f}%)"
    )
    logger.info(f"Timestamp range: {timestamps.min()} to {timestamps.max()}")

    scenario_dist = canonical[canonical["is_fraud"]]["fraud_scenario"].value_counts()
    logger.info(f"Fraud scenario distribution:\n{scenario_dist.to_string()}")

    return canonical
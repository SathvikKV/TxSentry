"""Generate full 12-month synthetic dataset."""

import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from txsentry.data_gen.entity_generator import generate_all_entities
from txsentry.data_gen.scenario_injection import ALL_SCENARIOS
from txsentry.data_gen.drift_injection import (
    get_monthly_fraud_config,
    get_month_boundaries,
    log_drift_schedule,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = "data/synthetic"

# Target: ~2M transactions over 12 months
LEGIT_TXNS_PER_MONTH = 155_000  # ~1.86M legitimate total


def generate_legitimate_transactions(
    accounts: pd.DataFrame,
    merchants: pd.DataFrame,
    devices: pd.DataFrame,
    ips: pd.DataFrame,
    beneficiaries: pd.DataFrame,
    month: int,
    n_txns: int,
    seed: int,
) -> pd.DataFrame:
    """Generate legitimate (non-fraud) transactions for a single month."""
    rng = np.random.RandomState(seed + month)
    start, end = get_month_boundaries(month)

    account_ids = accounts["account_id"].values
    merchant_ids = merchants["merchant_id"].values
    device_ids = devices["device_id"].values
    ip_ids = ips["ip_id"].values
    bene_ids = beneficiaries["beneficiary_id"].values

    # Vectorized generation
    timestamps = start + pd.to_timedelta(
        rng.randint(0, max(1, int((end - start).total_seconds())), size=n_txns),
        unit="s",
    )

    txn_types = rng.choice(
        ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"],
        size=n_txns,
        p=[0.35, 0.25, 0.15, 0.15, 0.10],
    )
    channels = rng.choice(
        ["MOBILE", "ONLINE", "BRANCH", "ATM"],
        size=n_txns,
        p=[0.40, 0.35, 0.15, 0.10],
    )

    # Lognormal amount distribution — realistic: most txns small, some large
    amounts = np.round(rng.lognormal(mean=4.5, sigma=1.5, size=n_txns), 2)
    amounts = np.clip(amounts, 1.0, 50000.0)

    df = pd.DataFrame({
        "txn_id": [f"TXN_SYN_{month:02d}_{i}" for i in range(n_txns)],
        "account_id": rng.choice(account_ids, n_txns),
        "merchant_id": rng.choice(merchant_ids, n_txns),
        "beneficiary_id": np.where(
            np.isin(txn_types, ["TRANSFER", "CASH_OUT"]),
            rng.choice(bene_ids, n_txns),
            None,
        ),
        "device_id": rng.choice(device_ids, n_txns),
        "ip_id": rng.choice(ip_ids, n_txns),
        "amount": amounts,
        "currency": "USD",
        "txn_type": txn_types,
        "channel": channels,
        "timestamp": timestamps,
        "is_fraud": False,
        "fraud_scenario": None,
        "source": "SYNTHETIC",
    })

    return df


def generate_fraud_transactions(
    entities: dict[str, pd.DataFrame],
    month: int,
    n_legit_txns: int,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate fraud transactions for a month based on drift schedule."""
    config = get_monthly_fraud_config(month)
    start, end = get_month_boundaries(month)

    # Number of fraud txns this month
    n_fraud_instances = int(n_legit_txns * config["fraud_rate"])

    scenario_weights = config["scenario_weights"]
    amount_mult = config["amount_multiplier"]

    all_fraud_records = []
    scenario_classes = {cls.name: cls for cls in ALL_SCENARIOS}

    for scenario_name, weight in scenario_weights.items():
        n_instances = max(1, int(n_fraud_instances * weight))
        cls = scenario_classes.get(scenario_name)
        if cls is None:
            continue

        scenario = cls(
            accounts=entities["account"],
            devices=entities["device"],
            ips=entities["ip_address"],
            beneficiaries=entities["beneficiary"],
            merchants=entities["merchant"],
            seed=seed + month + hash(scenario_name) % 10000,
        )

        records = scenario.generate(n_instances, start, end)

        # Apply amount multiplier for drift
        for r in records:
            r["amount"] = round(r["amount"] * amount_mult, 2)

        all_fraud_records.extend(records)

    if not all_fraud_records:
        return pd.DataFrame()

    fraud_df = pd.DataFrame(all_fraud_records)
    return fraud_df


def run():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting TxSentry Synthetic Data Generator")
    logger.info("=" * 60)

    log_drift_schedule()

    # --- Step 1: Generate entities ---
    logger.info("\nStep 1: Generating entity tables...")
    entities = generate_all_entities(output_dir=OUTPUT_DIR)

    # --- Step 2: Generate transactions month by month ---
    logger.info("\nStep 2: Generating transactions (12 months)...")
    all_dfs = []

    for month in range(1, 13):
        config = get_monthly_fraud_config(month)
        start, end = get_month_boundaries(month)
        logger.info(
            f"\n  Month {month:2d} ({start.strftime('%b %Y')}): "
            f"quarter={config['quarter']}, fraud_rate={config['fraud_rate']:.1%}"
        )

        # Legitimate transactions
        legit_df = generate_legitimate_transactions(
            accounts=entities["account"],
            merchants=entities["merchant"],
            devices=entities["device"],
            ips=entities["ip_address"],
            beneficiaries=entities["beneficiary"],
            month=month,
            n_txns=LEGIT_TXNS_PER_MONTH,
            seed=42,
        )
        logger.info(f"    Legit: {len(legit_df):,} transactions")

        # Fraud transactions
        fraud_df = generate_fraud_transactions(
            entities=entities,
            month=month,
            n_legit_txns=LEGIT_TXNS_PER_MONTH,
            seed=42,
        )
        logger.info(f"    Fraud: {len(fraud_df):,} transactions")

        if len(fraud_df) > 0:
            scenario_counts = fraud_df["fraud_scenario"].value_counts()
            for scenario, count in scenario_counts.items():
                logger.info(f"      {scenario}: {count}")

        month_df = pd.concat([legit_df, fraud_df], ignore_index=True)
        all_dfs.append(month_df)

    # --- Step 3: Combine and sort ---
    logger.info("\nStep 3: Combining all months...")
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values("timestamp").reset_index(drop=True)

    # Ensure unique txn_ids
    assert full_df["txn_id"].is_unique, "Duplicate txn_ids in synthetic data!"

    # --- Step 4: Write output ---
    output_path = f"{OUTPUT_DIR}/transaction_event.parquet"
    full_df.to_parquet(output_path, index=False)
    logger.info(f"\nWrote {len(full_df):,} transactions to {output_path}")

    # --- Summary report ---
    logger.info("\n" + "=" * 60)
    logger.info("SYNTHETIC DATA GENERATION REPORT")
    logger.info("=" * 60)
    logger.info(f"Total transactions:  {len(full_df):,}")
    logger.info(f"Fraud transactions:  {full_df['is_fraud'].sum():,}")
    logger.info(f"Fraud rate:          {full_df['is_fraud'].mean():.2%}")
    logger.info(f"Date range:          {full_df['timestamp'].min()} to {full_df['timestamp'].max()}")
    logger.info(f"Unique accounts:     {full_df['account_id'].nunique():,}")

    logger.info("\nTransactions by month:")
    full_df["month"] = full_df["timestamp"].dt.to_period("M")
    monthly = full_df.groupby("month").agg(
        total=("txn_id", "count"),
        fraud=("is_fraud", "sum"),
    )
    monthly["fraud_rate"] = (monthly["fraud"] / monthly["total"] * 100).round(2)
    for period, row in monthly.iterrows():
        logger.info(f"  {period}: {row['total']:,} txns, {row['fraud']:,} fraud ({row['fraud_rate']:.2f}%)")

    logger.info("\nFraud by scenario:")
    scenario_counts = full_df[full_df["is_fraud"]]["fraud_scenario"].value_counts()
    for scenario, count in scenario_counts.items():
        logger.info(f"  {scenario}: {count:,}")

    logger.info("\nEntity table sizes:")
    for name, df in entities.items():
        logger.info(f"  {name}: {len(df):,}")

    # File sizes
    logger.info("\nOutput file sizes:")
    for f in sorted(Path(OUTPUT_DIR).glob("*.parquet")):
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name}: {size_mb:.1f} MB")

    elapsed = time.time() - start_time
    logger.info(f"\nGeneration complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
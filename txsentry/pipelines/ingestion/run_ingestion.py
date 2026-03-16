"""Orchestrate the full ingestion pipeline."""

import logging
import time
from pathlib import Path

import pandas as pd

from txsentry.pipelines.ingestion.ingest_raw import ingest_paysim, ingest_amlsim
from txsentry.pipelines.ingestion.transform_paysim import transform_paysim
from txsentry.pipelines.ingestion.transform_amlsim import transform_amlsim_transactions
from txsentry.pipelines.ingestion.assign_devices_ips import assign_synthetic_devices_and_ips
from txsentry.pipelines.ingestion.validate import validate_canonical

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# --- Paths ---
RAW_PAYSIM = "data/raw/paysim/paysim.csv"
RAW_AMLSIM_DIR = "data/raw/amlsim"
STAGING_DIR = "data/staging"
CANONICAL_DIR = "data/canonical"


def run():
    start = time.time()
    logger.info("Starting TxSentry ingestion pipeline")

    canonical_dfs = []

    # Data Ingestion
    logger.info("Ingesting raw data to staging")

    # PaySim
    if Path(RAW_PAYSIM).exists():
        ingest_paysim(RAW_PAYSIM, f"{STAGING_DIR}/paysim.parquet")
    else:
        logger.warning(f"PaySim raw data not found at {RAW_PAYSIM} — skipping")

    # AMLSim
    amlsim_txn_path = Path(RAW_AMLSIM_DIR) / "transactions.csv"
    if amlsim_txn_path.exists():
        ingest_amlsim(RAW_AMLSIM_DIR, STAGING_DIR)
    else:
        logger.info("AMLSim raw data not found — skipping")

    # Canonical Transformation
    logger.info("Transforming to canonical schema")

    # PaySim transform
    paysim_staging = Path(f"{STAGING_DIR}/paysim.parquet")
    if paysim_staging.exists():
        logger.info("Transforming PaySim...")
        paysim_staged = pd.read_parquet(str(paysim_staging))
        paysim_canonical = transform_paysim(paysim_staged)
        canonical_dfs.append(paysim_canonical)
        del paysim_staged  # free memory
    else:
        logger.warning("PaySim staging file not found — skipping transform")

    # AMLSim transform
    amlsim_txn_staging = Path(f"{STAGING_DIR}/amlsim_transactions.parquet")
    amlsim_alert_staging = Path(f"{STAGING_DIR}/amlsim_alert_accounts.parquet")
    if amlsim_txn_staging.exists():
        logger.info("Transforming AMLSim...")
        amlsim_txn = pd.read_parquet(str(amlsim_txn_staging))
        if amlsim_alert_staging.exists():
            amlsim_alerts = pd.read_parquet(str(amlsim_alert_staging))
        else:
            logger.warning("AMLSim alert_accounts not found — using empty DataFrame")
            amlsim_alerts = pd.DataFrame(columns=["alertID", "reason", "accountID", "isSAR"])
        amlsim_canonical = transform_amlsim_transactions(amlsim_txn, amlsim_alerts)
        canonical_dfs.append(amlsim_canonical)
        del amlsim_txn  # free memory
    else:
        logger.info("AMLSim staging file not found — skipping transform")

    # Merge all sources
    if not canonical_dfs:
        raise RuntimeError("No data sources were successfully transformed!")

    logger.info(f"Merging {len(canonical_dfs)} source(s)...")
    all_txns = pd.concat(canonical_dfs, ignore_index=True)
    del canonical_dfs

    # Synthetic Assignment
    logger.info(f"Assigning synthetic devices and IPs to {len(all_txns)} transactions")

    all_txns, device_df, ip_df, acc_device_edges, device_ip_edges = (
        assign_synthetic_devices_and_ips(all_txns)
    )

    # Sorting and Deduplication
    logger.info("Sorting and checking uniqueness")
    all_txns = all_txns.sort_values("timestamp").reset_index(drop=True)
    assert all_txns["txn_id"].is_unique, "Duplicate txn_ids found after merge!"

    # ---- Write canonical Parquet files ----
    logger.info("Writing canonical Parquet files...")
    Path(CANONICAL_DIR).mkdir(parents=True, exist_ok=True)

    all_txns.to_parquet(f"{CANONICAL_DIR}/transaction_event.parquet", index=False)
    device_df.to_parquet(f"{CANONICAL_DIR}/device.parquet", index=False)
    ip_df.to_parquet(f"{CANONICAL_DIR}/ip_address.parquet", index=False)
    acc_device_edges.to_parquet(f"{CANONICAL_DIR}/account_used_by_device.parquet", index=False)
    device_ip_edges.to_parquet(f"{CANONICAL_DIR}/device_seen_on_ip.parquet", index=False)

    # Write account table (derived from unique accounts in transactions)
    accounts = all_txns[["account_id"]].drop_duplicates()
    accounts["customer_id"] = "CUST_" + accounts["account_id"].str.replace("ACC_", "")
    accounts["account_type"] = "CHECKING"
    accounts["balance"] = 0.0
    accounts["status"] = "ACTIVE"
    accounts["created_at"] = pd.Timestamp("2024-01-01")
    accounts.to_parquet(f"{CANONICAL_DIR}/account.parquet", index=False)

    logger.info(f"Canonical files written to {CANONICAL_DIR}/")

    # Validation
    logger.info("Running validation")
    report = validate_canonical(all_txns, device_df, ip_df)

    elapsed = time.time() - start
    logger.info(f"\nIngestion pipeline complete in {elapsed:.1f}s")

    # Print file sizes
    logger.info("\nCanonical file sizes:")
    for f in sorted(Path(CANONICAL_DIR).glob("*.parquet")):
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name}: {size_mb:.1f} MB")

    return report


if __name__ == "__main__":
    run()
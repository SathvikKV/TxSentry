"""Ingest raw CSVs into staging Parquet files.

Perform minimal validation and write to staging directory.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_paysim(raw_path: str, staging_path: str) -> pd.DataFrame:
    """Read raw PaySim CSV, validate required columns, write to staging Parquet."""
    logger.info(f"Ingesting PaySim from {raw_path}")
    df = pd.read_csv(raw_path)

    required_cols = {"step", "type", "amount", "nameOrig", "nameDest", "isFraud"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"PaySim CSV missing required columns: {missing}")

    initial_count = len(df)
    df = df.dropna(subset=["amount", "nameOrig", "nameDest"])
    dropped = initial_count - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with null amount/nameOrig/nameDest")

    Path(staging_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(staging_path, index=False)
    logger.info(f"PaySim staged: {len(df)} rows -> {staging_path}")
    return df


def ingest_amlsim(raw_dir: str, staging_dir: str) -> dict[str, pd.DataFrame]:
    """Read raw AMLSim CSVs, write each to staging Parquet."""
    logger.info(f"Ingesting AMLSim from {raw_dir}")
    Path(staging_dir).mkdir(parents=True, exist_ok=True)

    results = {}
    for fname in ["transactions.csv", "accounts.csv", "alert_accounts.csv"]:
        fpath = Path(raw_dir) / fname
        if not fpath.exists():
            logger.warning(f"AMLSim file not found, skipping: {fpath}")
            continue
        df = pd.read_csv(str(fpath))
        out_name = f"amlsim_{fname.replace('.csv', '.parquet')}"
        out_path = Path(staging_dir) / out_name
        df.to_parquet(str(out_path), index=False)
        results[fname] = df
        logger.info(f"AMLSim staged: {fname} -> {len(df)} rows")

    return results
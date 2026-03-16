"""Assign synthetic devices and IPs to transactions.

Perform vectorized generation of device and IP entities and map them to transaction events.
"""

import logging
import uuid

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()
Faker.seed(42)

DEVICE_TYPES = ["MOBILE", "DESKTOP", "TABLET"]
DEVICE_TYPE_WEIGHTS = [0.6, 0.25, 0.15]
OS_BY_DEVICE = {
    "MOBILE": ["iOS 17", "Android 14", "Android 13", "iOS 16"],
    "DESKTOP": ["Windows 11", "macOS 14", "Linux", "Windows 10"],
    "TABLET": ["iPadOS 17", "Android 14", "Android 13"],
}
COUNTRIES = ["US", "US", "US", "US", "GB", "CA", "DE", "IN", "BR", "NG"]


def assign_synthetic_devices_and_ips(
    txn_df: pd.DataFrame,
    shared_device_rate: float = 0.05,
    new_device_rate: float = 0.03,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Assign synthetic devices and IPs to all transactions (vectorized).

    Returns:
        txn_df, device_df, ip_df, acc_device_edges, device_ip_edges
    """
    np.random.seed(seed)
    logger.info("Assigning synthetic devices and IPs (vectorized)...")

    accounts = txn_df["account_id"].unique()
    n_accounts = len(accounts)
    logger.info(f"Processing {n_accounts} unique accounts")

    # --- Phase 1: Generate devices per account ---
    logger.info("Phase 1: Generating devices...")
    device_records = []
    ip_records = []
    account_to_devices = {}  # account_id -> list of device_ids
    device_to_ips = {}       # device_id -> list of ip_ids

    for i, acc_id in enumerate(accounts):
        if i % 500_000 == 0 and i > 0:
            logger.info(f"  ...generated devices for {i:,}/{n_accounts:,} accounts")

        n_devices = np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1])
        acc_devs = []

        for _ in range(n_devices):
            dev_type = np.random.choice(DEVICE_TYPES, p=DEVICE_TYPE_WEIGHTS)
            dev_id = f"DEV_{uuid.uuid4().hex[:12]}"
            device_records.append({
                "device_id": dev_id,
                "device_type": dev_type,
                "os": np.random.choice(OS_BY_DEVICE[dev_type]),
                "fingerprint_hash": uuid.uuid4().hex,
                "first_seen_at": pd.Timestamp("2024-01-01"),
            })
            acc_devs.append(dev_id)

            # 1-2 IPs per device
            n_ips = np.random.choice([1, 2], p=[0.7, 0.3])
            dev_ips = []
            for _ in range(n_ips):
                ip_id = f"IP_{uuid.uuid4().hex[:12]}"
                ip_records.append({
                    "ip_id": ip_id,
                    "ip_addr": f"{np.random.randint(1,255)}.{np.random.randint(0,255)}.{np.random.randint(0,255)}.{np.random.randint(1,255)}",
                    "country": np.random.choice(COUNTRIES),
                    "isp": f"ISP_{np.random.randint(1, 500)}",
                    "is_vpn": np.random.random() < 0.05,
                    "is_datacenter": np.random.random() < 0.03,
                })
                dev_ips.append(ip_id)
            device_to_ips[dev_id] = dev_ips

        account_to_devices[acc_id] = acc_devs

    logger.info(f"  Created {len(device_records)} devices, {len(ip_records)} IPs")

    # --- Phase 2: Shared devices for fraud rings ---
    logger.info("Phase 2: Creating shared device clusters...")
    n_shared = int(n_accounts * shared_device_rate)
    shared_indices = np.random.choice(n_accounts, size=min(n_shared, n_accounts), replace=False)
    shared_accounts = accounts[shared_indices]

    shared_count = 0
    i = 0
    while i < len(shared_accounts):
        cluster_size = np.random.randint(2, 5)
        cluster = shared_accounts[i:i + cluster_size]
        if len(cluster) < 2:
            break
        # Create one shared device
        dev_id = f"DEV_SHARED_{uuid.uuid4().hex[:10]}"
        device_records.append({
            "device_id": dev_id,
            "device_type": "MOBILE",
            "os": "Android 14",
            "fingerprint_hash": uuid.uuid4().hex,
            "first_seen_at": pd.Timestamp("2024-01-01"),
        })
        ip_id = f"IP_SHARED_{uuid.uuid4().hex[:10]}"
        ip_records.append({
            "ip_id": ip_id,
            "ip_addr": f"{np.random.randint(1,255)}.{np.random.randint(0,255)}.{np.random.randint(0,255)}.{np.random.randint(1,255)}",
            "country": np.random.choice(COUNTRIES),
            "isp": f"ISP_{np.random.randint(1, 500)}",
            "is_vpn": True,
            "is_datacenter": np.random.random() < 0.5,
        })
        device_to_ips[dev_id] = [ip_id]

        for acc_id in cluster:
            account_to_devices[acc_id].append(dev_id)
        shared_count += 1
        i += cluster_size

    logger.info(f"  Created {shared_count} shared device clusters")

    # --- Phase 3: Vectorized assignment to transactions ---
    logger.info("Phase 3: Assigning devices/IPs to transactions (vectorized)...")

    # Build a mapping: account_id -> primary device_id (most common)
    primary_device = {acc: devs[0] for acc, devs in account_to_devices.items()}
    # Build primary IP for each device
    primary_ip = {dev: ips[0] for dev, ips in device_to_ips.items()}

    # Assign primary device to all transactions via map
    txn_df = txn_df.copy()
    txn_df["device_id"] = txn_df["account_id"].map(primary_device)

    # ~3% get a random (possibly new) device from their account's device list
    n_txns = len(txn_df)
    new_device_mask = np.random.random(n_txns) < new_device_rate
    n_new = new_device_mask.sum()
    logger.info(f"  Reassigning {n_new:,} transactions ({n_new/n_txns*100:.1f}%) to alternate devices")

    if n_new > 0:
        new_device_indices = txn_df.index[new_device_mask]
        for idx in new_device_indices:
            acc_id = txn_df.at[idx, "account_id"]
            devs = account_to_devices.get(acc_id, [])
            if len(devs) > 1:
                txn_df.at[idx, "device_id"] = np.random.choice(devs)

    # Assign IP based on device
    txn_df["ip_id"] = txn_df["device_id"].map(primary_ip)

    # Fill any remaining nulls (shouldn't happen but safety net)
    null_device = txn_df["device_id"].isna().sum()
    null_ip = txn_df["ip_id"].isna().sum()
    if null_device > 0 or null_ip > 0:
        logger.warning(f"  Null devices: {null_device}, null IPs: {null_ip} — filling with defaults")
        fallback_dev = device_records[0]["device_id"]
        fallback_ip = ip_records[0]["ip_id"]
        txn_df["device_id"] = txn_df["device_id"].fillna(fallback_dev)
        txn_df["ip_id"] = txn_df["ip_id"].fillna(fallback_ip)

    logger.info(f"  Device/IP assignment complete")

    # --- Build output DataFrames ---
    logger.info("Building entity and edge tables...")
    device_df = pd.DataFrame(device_records).drop_duplicates(subset=["device_id"])
    ip_df = pd.DataFrame(ip_records).drop_duplicates(subset=["ip_id"])

    # Account-device edges (aggregated from transactions)
    acc_dev_agg = (
        txn_df.groupby(["account_id", "device_id"])
        .agg(
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
            txn_count=("txn_id", "count"),
        )
        .reset_index()
    )

    # Device-IP edges (aggregated from transactions)
    dev_ip_agg = (
        txn_df.groupby(["device_id", "ip_id"])
        .agg(
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
        )
        .reset_index()
    )

    logger.info(
        f"Done: {len(device_df)} devices, {len(ip_df)} IPs, "
        f"{len(acc_dev_agg)} account-device edges, {len(dev_ip_agg)} device-IP edges"
    )

    return txn_df, device_df, ip_df, acc_dev_agg, dev_ip_agg
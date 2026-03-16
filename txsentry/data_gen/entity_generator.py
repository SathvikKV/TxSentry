"""Generate synthetic entity tables: customers, accounts, devices, IPs, merchants, beneficiaries, watchlists.

These entities are used by the scenario injector to create realistic fraud patterns.
Entities that already exist in canonical/ (from Module 0) are supplemented, not replaced.
"""

import logging
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()
Faker.seed(42)

# --- MCC codes with realistic fraud rates ---
MCC_CATEGORIES = [
    {"code": "5411", "name": "Grocery Stores", "country": "US", "fraud_rate": 0.005, "risk_tier": "LOW"},
    {"code": "5541", "name": "Gas Stations", "country": "US", "fraud_rate": 0.008, "risk_tier": "LOW"},
    {"code": "5812", "name": "Restaurants", "country": "US", "fraud_rate": 0.006, "risk_tier": "LOW"},
    {"code": "5912", "name": "Drug Stores", "country": "US", "fraud_rate": 0.004, "risk_tier": "LOW"},
    {"code": "5311", "name": "Department Stores", "country": "US", "fraud_rate": 0.010, "risk_tier": "LOW"},
    {"code": "4816", "name": "Online Services", "country": "US", "fraud_rate": 0.025, "risk_tier": "MEDIUM"},
    {"code": "5999", "name": "Misc Retail", "country": "US", "fraud_rate": 0.020, "risk_tier": "MEDIUM"},
    {"code": "7995", "name": "Gambling", "country": "US", "fraud_rate": 0.045, "risk_tier": "HIGH"},
    {"code": "6012", "name": "Financial Institutions", "country": "US", "fraud_rate": 0.015, "risk_tier": "MEDIUM"},
    {"code": "4829", "name": "Wire Transfers", "country": "US", "fraud_rate": 0.060, "risk_tier": "HIGH"},
    {"code": "6051", "name": "Crypto Exchanges", "country": "US", "fraud_rate": 0.070, "risk_tier": "HIGH"},
    {"code": "5944", "name": "Jewelry Stores", "country": "US", "fraud_rate": 0.030, "risk_tier": "MEDIUM"},
    {"code": "7011", "name": "Hotels", "country": "US", "fraud_rate": 0.012, "risk_tier": "LOW"},
    {"code": "3000", "name": "Airlines", "country": "US", "fraud_rate": 0.018, "risk_tier": "MEDIUM"},
    {"code": "5732", "name": "Electronics Stores", "country": "US", "fraud_rate": 0.022, "risk_tier": "MEDIUM"},
]

MERCHANT_COUNTRIES = ["US", "US", "US", "US", "GB", "CA", "DE", "IN", "SG", "AE", "NG", "BR"]

ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "BUSINESS", "PREPAID"]
ACCOUNT_TYPE_WEIGHTS = [0.50, 0.20, 0.20, 0.10]

RISK_TIERS = ["LOW", "MEDIUM", "HIGH"]
RISK_TIER_WEIGHTS = [0.70, 0.20, 0.10]

DEVICE_TYPES = ["MOBILE", "DESKTOP", "TABLET"]
DEVICE_TYPE_WEIGHTS = [0.60, 0.25, 0.15]
OS_BY_DEVICE = {
    "MOBILE": ["iOS 17", "Android 14", "Android 13", "iOS 16"],
    "DESKTOP": ["Windows 11", "macOS 14", "Linux", "Windows 10"],
    "TABLET": ["iPadOS 17", "Android 14", "Android 13"],
}


def generate_customers(n: int = 50_000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} customers...")
    return pd.DataFrame({
        "customer_id": [f"CUST_SYN_{i}" for i in range(n)],
        "name": [fake.name() for _ in range(n)],
        "dob": pd.to_datetime([
            fake.date_of_birth(minimum_age=18, maximum_age=80) for _ in range(n)
        ]),
        "country": np.random.choice(MERCHANT_COUNTRIES, n),
        "created_at": pd.to_datetime([
            fake.date_time_between(start_date="-3y", end_date="-30d") for _ in range(n)
        ]),
        "risk_tier": np.random.choice(RISK_TIERS, n, p=RISK_TIER_WEIGHTS),
    })


def generate_accounts(customers: pd.DataFrame, accounts_per_customer: float = 1.6, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    n_customers = len(customers)
    target_accounts = int(n_customers * accounts_per_customer)
    logger.info(f"Generating ~{target_accounts} accounts for {n_customers} customers...")

    rows = []
    acct_idx = 0
    for _, cust in customers.iterrows():
        n_accts = np.random.choice([1, 2, 3], p=[0.55, 0.30, 0.15])
        for _ in range(n_accts):
            rows.append({
                "account_id": f"ACC_SYN_{acct_idx}",
                "customer_id": cust["customer_id"],
                "account_type": np.random.choice(ACCOUNT_TYPES, p=ACCOUNT_TYPE_WEIGHTS),
                "balance": round(np.random.lognormal(mean=8, sigma=1.5), 2),
                "status": np.random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT"], p=[0.85, 0.05, 0.05, 0.05]),
                "created_at": cust["created_at"] + pd.Timedelta(days=np.random.randint(0, 90)),
            })
            acct_idx += 1

    logger.info(f"Generated {len(rows)} accounts")
    return pd.DataFrame(rows)


def generate_merchants(n: int = 5_000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} merchants...")

    rows = []
    for i in range(n):
        mcc = np.random.choice(MCC_CATEGORIES)
        country = mcc["country"] if np.random.random() < 0.7 else np.random.choice(MERCHANT_COUNTRIES)
        rows.append({
            "merchant_id": f"MERCH_{i}",
            "name": fake.company(),
            "category_code": mcc["code"],
            "country": country,
            "fraud_rate_hist": round(mcc["fraud_rate"] * np.random.uniform(0.5, 2.0), 4),
        })

    return pd.DataFrame(rows)


def generate_devices(n: int = 30_000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} synthetic devices...")

    rows = []
    for i in range(n):
        dev_type = np.random.choice(DEVICE_TYPES, p=DEVICE_TYPE_WEIGHTS)
        rows.append({
            "device_id": f"DEV_SYN_{i}",
            "device_type": dev_type,
            "os": np.random.choice(OS_BY_DEVICE[dev_type]),
            "fingerprint_hash": uuid.uuid4().hex,
            "first_seen_at": pd.Timestamp("2024-02-01") + pd.Timedelta(days=np.random.randint(0, 330)),
        })

    return pd.DataFrame(rows)


def generate_ips(n: int = 20_000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} synthetic IPs...")

    return pd.DataFrame({
        "ip_id": [f"IP_SYN_{i}" for i in range(n)],
        "ip_addr": [f"{np.random.randint(1,255)}.{np.random.randint(0,255)}.{np.random.randint(0,255)}.{np.random.randint(1,255)}" for _ in range(n)],
        "country": np.random.choice(MERCHANT_COUNTRIES, n),
        "isp": [f"ISP_{np.random.randint(1, 500)}" for _ in range(n)],
        "is_vpn": np.random.random(n) < 0.05,
        "is_datacenter": np.random.random(n) < 0.03,
    })


def generate_beneficiaries(n: int = 10_000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} beneficiaries...")

    return pd.DataFrame({
        "beneficiary_id": [f"BENE_{i}" for i in range(n)],
        "name": [fake.name() for _ in range(n)],
        "account_ref": [f"EXT_{uuid.uuid4().hex[:8]}" for _ in range(n)],
        "bank_code": [f"BANK_{np.random.randint(100, 999)}" for _ in range(n)],
        "added_at": [
            pd.Timestamp("2024-02-01") + pd.Timedelta(days=np.random.randint(0, 330))
            for _ in range(n)
        ],
    })


def generate_watchlist(n: int = 500, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Generating {n} watchlist entities...")

    reasons = [
        "OFAC SDN List", "UN Sanctions", "EU Sanctions",
        "PEP", "Adverse Media", "Law Enforcement",
    ]
    entity_types = ["INDIVIDUAL", "ORGANIZATION"]

    return pd.DataFrame({
        "entity_id": [f"WL_{i}" for i in range(n)],
        "entity_type": np.random.choice(entity_types, n, p=[0.7, 0.3]),
        "name": [fake.name() if np.random.random() < 0.7 else fake.company() for _ in range(n)],
        "reason": np.random.choice(reasons, n),
        "listed_at": [
            pd.Timestamp("2023-01-01") + pd.Timedelta(days=np.random.randint(0, 700))
            for _ in range(n)
        ],
        "source": np.random.choice(["OFAC", "UN", "EU", "INTERPOL", "LOCAL_LE"], n),
    })


def generate_all_entities(
    n_customers: int = 50_000,
    n_merchants: int = 5_000,
    n_devices: int = 30_000,
    n_ips: int = 20_000,
    n_beneficiaries: int = 10_000,
    n_watchlist: int = 500,
    output_dir: str = "data/synthetic",
    seed: int = 42,
) -> dict[str, pd.DataFrame]:
    """Generate all entity tables and write to Parquet."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    customers = generate_customers(n_customers, seed)
    accounts = generate_accounts(customers, seed=seed)
    merchants = generate_merchants(n_merchants, seed)
    devices = generate_devices(n_devices, seed)
    ips = generate_ips(n_ips, seed)
    beneficiaries = generate_beneficiaries(n_beneficiaries, seed)
    watchlist = generate_watchlist(n_watchlist, seed)

    tables = {
        "customer": customers,
        "account": accounts,
        "merchant": merchants,
        "device": devices,
        "ip_address": ips,
        "beneficiary": beneficiaries,
        "watchlist_entity": watchlist,
    }

    for name, df in tables.items():
        path = f"{output_dir}/{name}.parquet"
        df.to_parquet(path, index=False)
        logger.info(f"  Wrote {name}: {len(df)} rows -> {path}")

    return tables
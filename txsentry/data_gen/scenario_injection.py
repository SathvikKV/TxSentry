"""Inject labeled fraud scenarios into the synthetic transaction stream.

Each scenario is a class with a generate() method that produces fraud transaction
records conforming to the canonical schema.
"""

import logging
import uuid

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class FraudScenario:
    """Base class for fraud scenario generators."""

    name: str = "BASE"

    def __init__(self, accounts: pd.DataFrame, devices: pd.DataFrame,
                 ips: pd.DataFrame, beneficiaries: pd.DataFrame,
                 merchants: pd.DataFrame, seed: int = 42):
        self.accounts = accounts
        self.account_ids = accounts["account_id"].values
        self.devices = devices
        self.device_ids = devices["device_id"].values
        self.ips = ips
        self.ip_ids = ips["ip_id"].values
        self.beneficiaries = beneficiaries
        self.beneficiary_ids = beneficiaries["beneficiary_id"].values
        self.merchants = merchants
        self.merchant_ids = merchants["merchant_id"].values
        self.rng = np.random.RandomState(seed)

    def _random_timestamp(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.Timestamp:
        delta = (end - start).total_seconds()
        return start + pd.Timedelta(seconds=self.rng.randint(0, max(1, int(delta))))

    def _txn_id(self) -> str:
        return f"TXN_SYN_{uuid.uuid4().hex[:12]}"

    def generate(self, n_instances: int, time_start: pd.Timestamp,
                 time_end: pd.Timestamp) -> list[dict]:
        raise NotImplementedError


class MuleChain(FraudScenario):
    """Account A receives from multiple, forwards to B, B to C. Chain 2-4 hops."""

    name = "MULE_CHAIN"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            chain_len = self.rng.randint(2, 5)
            chain_accounts = self.rng.choice(self.account_ids, size=chain_len + 1, replace=False)
            amount = round(self.rng.uniform(500, 5000), 2)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(hours=24))
            device = self.rng.choice(self.device_ids)
            ip = self.rng.choice(self.ip_ids)
            merchant = self.rng.choice(self.merchant_ids)

            for hop in range(chain_len):
                records.append({
                    "txn_id": self._txn_id(),
                    "account_id": chain_accounts[hop],
                    "merchant_id": merchant,
                    "beneficiary_id": chain_accounts[hop + 1],
                    "device_id": device,
                    "ip_id": ip,
                    "amount": round(amount * self.rng.uniform(0.9, 1.0), 2),
                    "currency": "USD",
                    "txn_type": "TRANSFER",
                    "channel": "ONLINE",
                    "timestamp": base_time + pd.Timedelta(hours=hop * self.rng.randint(1, 6)),
                    "is_fraud": True,
                    "fraud_scenario": self.name,
                    "source": "SYNTHETIC",
                })
        return records


class FanOut(FraudScenario):
    """Single account sends to 5-15 beneficiaries within 2 hours, near $10K."""

    name = "FAN_OUT"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            source = self.rng.choice(self.account_ids)
            n_targets = self.rng.randint(5, 16)
            targets = self.rng.choice(self.beneficiary_ids, size=n_targets, replace=False)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(hours=3))
            device = self.rng.choice(self.device_ids)
            ip = self.rng.choice(self.ip_ids)

            for i, target in enumerate(targets):
                records.append({
                    "txn_id": self._txn_id(),
                    "account_id": source,
                    "merchant_id": self.rng.choice(self.merchant_ids),
                    "beneficiary_id": target,
                    "device_id": device,
                    "ip_id": ip,
                    "amount": round(self.rng.uniform(8500, 9999), 2),
                    "currency": "USD",
                    "txn_type": "TRANSFER",
                    "channel": "ONLINE",
                    "timestamp": base_time + pd.Timedelta(minutes=i * self.rng.randint(3, 15)),
                    "is_fraud": True,
                    "fraud_scenario": self.name,
                    "source": "SYNTHETIC",
                })
        return records


class FanIn(FraudScenario):
    """5-10 accounts send to a single beneficiary within 24h, sharing device/IP."""

    name = "FAN_IN"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            n_sources = self.rng.randint(5, 11)
            sources = self.rng.choice(self.account_ids, size=n_sources, replace=False)
            target = self.rng.choice(self.beneficiary_ids)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(hours=24))
            shared_device = self.rng.choice(self.device_ids)
            shared_ip = self.rng.choice(self.ip_ids)

            for i, src in enumerate(sources):
                records.append({
                    "txn_id": self._txn_id(),
                    "account_id": src,
                    "merchant_id": self.rng.choice(self.merchant_ids),
                    "beneficiary_id": target,
                    "device_id": shared_device,
                    "ip_id": shared_ip,
                    "amount": round(self.rng.uniform(500, 3000), 2),
                    "currency": "USD",
                    "txn_type": "TRANSFER",
                    "channel": "MOBILE",
                    "timestamp": base_time + pd.Timedelta(hours=i * self.rng.uniform(1, 4)),
                    "is_fraud": True,
                    "fraud_scenario": self.name,
                    "source": "SYNTHETIC",
                })
        return records


class AccountTakeoverBurst(FraudScenario):
    """Dormant 30+ days, new device login, new payee, large transfer."""

    name = "ACCOUNT_TAKEOVER_BURST"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            account = self.rng.choice(self.account_ids)
            new_device = self.rng.choice(self.device_ids)
            new_ip = self.rng.choice(self.ip_ids)
            new_bene = self.rng.choice(self.beneficiary_ids)
            merchant = self.rng.choice(self.merchant_ids)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(hours=3))
            amount = round(self.rng.uniform(3000, 25000), 2)

            records.append({
                "txn_id": self._txn_id(),
                "account_id": account,
                "merchant_id": merchant,
                "beneficiary_id": new_bene,
                "device_id": new_device,
                "ip_id": new_ip,
                "amount": amount,
                "currency": "USD",
                "txn_type": "TRANSFER",
                "channel": "ONLINE",
                "timestamp": base_time,
                "is_fraud": True,
                "fraud_scenario": self.name,
                "source": "SYNTHETIC",
            })
            # Second transfer shortly after
            if self.rng.random() < 0.6:
                records.append({
                    "txn_id": self._txn_id(),
                    "account_id": account,
                    "merchant_id": merchant,
                    "beneficiary_id": self.rng.choice(self.beneficiary_ids),
                    "device_id": new_device,
                    "ip_id": new_ip,
                    "amount": round(amount * self.rng.uniform(0.5, 1.5), 2),
                    "currency": "USD",
                    "txn_type": "TRANSFER",
                    "channel": "ONLINE",
                    "timestamp": base_time + pd.Timedelta(minutes=self.rng.randint(10, 90)),
                    "is_fraud": True,
                    "fraud_scenario": self.name,
                    "source": "SYNTHETIC",
                })
        return records


class SharedDeviceRing(FraudScenario):
    """One device fingerprint associated with 4-8 accounts, each transacting."""

    name = "SHARED_DEVICE_RING"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            shared_device = self.rng.choice(self.device_ids)
            n_accounts = self.rng.randint(4, 9)
            ring_accounts = self.rng.choice(self.account_ids, size=n_accounts, replace=False)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(days=7))

            for i, acc in enumerate(ring_accounts):
                n_txns = self.rng.randint(1, 4)
                for j in range(n_txns):
                    records.append({
                        "txn_id": self._txn_id(),
                        "account_id": acc,
                        "merchant_id": self.rng.choice(self.merchant_ids),
                        "beneficiary_id": self.rng.choice(self.beneficiary_ids),
                        "device_id": shared_device,
                        "ip_id": self.rng.choice(self.ip_ids),
                        "amount": round(self.rng.uniform(200, 5000), 2),
                        "currency": "USD",
                        "txn_type": self.rng.choice(["TRANSFER", "PAYMENT"]),
                        "channel": "MOBILE",
                        "timestamp": base_time + pd.Timedelta(
                            days=i, hours=j * self.rng.randint(1, 8)
                        ),
                        "is_fraud": True,
                        "fraud_scenario": self.name,
                        "source": "SYNTHETIC",
                    })
        return records


class Structuring(FraudScenario):
    """Series of transactions just below $10K within 3 days to same beneficiary."""

    name = "STRUCTURING"

    def generate(self, n_instances, time_start, time_end):
        records = []
        for _ in range(n_instances):
            account = self.rng.choice(self.account_ids)
            bene = self.rng.choice(self.beneficiary_ids)
            device = self.rng.choice(self.device_ids)
            ip = self.rng.choice(self.ip_ids)
            merchant = self.rng.choice(self.merchant_ids)
            base_time = self._random_timestamp(time_start, time_end - pd.Timedelta(days=3))
            n_txns = self.rng.randint(3, 7)

            for i in range(n_txns):
                records.append({
                    "txn_id": self._txn_id(),
                    "account_id": account,
                    "merchant_id": merchant,
                    "beneficiary_id": bene,
                    "device_id": device,
                    "ip_id": ip,
                    "amount": round(self.rng.uniform(9000, 9999), 2),
                    "currency": "USD",
                    "txn_type": "TRANSFER",
                    "channel": "ONLINE",
                    "timestamp": base_time + pd.Timedelta(
                        hours=i * self.rng.randint(6, 24)
                    ),
                    "is_fraud": True,
                    "fraud_scenario": self.name,
                    "source": "SYNTHETIC",
                })
        return records


ALL_SCENARIOS = [
    MuleChain,
    FanOut,
    FanIn,
    AccountTakeoverBurst,
    SharedDeviceRing,
    Structuring,
]
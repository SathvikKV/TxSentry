"""Kafka simulation producer — replays transactions from Parquet.

Can also run in HTTP mode (no Kafka required) to POST directly to the scoring API.

Usage:
    python -m txsentry.services.kafka_sim.producer --mode http --rate 50 --count 200
    python -m txsentry.services.kafka_sim.producer --mode kafka --rate 100
"""

import argparse
import json
import time
import logging
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

ALERTS_DIR = "data/alerts"
FEATURES_DIR = "data/features"
SCORING_URL = "http://localhost:8000/score"


def load_sample_transactions(n: int = 1000) -> pd.DataFrame:
    """Load a sample of transactions for replay."""
    path = f"{FEATURES_DIR}/txn_features.parquet"
    if not Path(path).exists():
        path = f"{ALERTS_DIR}/alert_events.parquet"

    df = pd.read_parquet(path)
    # Mix of high and low risk
    sample = pd.concat([
        df.sample(min(n // 2, len(df)), random_state=42),
        df[df["is_fraud"] == True].head(n // 2),
    ]).drop_duplicates(subset=["txn_id"]).head(n)

    logger.info(f"Loaded {len(sample)} transactions for replay")
    return sample


def replay_http(df: pd.DataFrame, rate: int = 50, count: int = None):
    """Replay transactions by POSTing to the scoring API."""
    logger.info(f"Replaying {len(df)} transactions to {SCORING_URL} at {rate} txn/s")

    latencies = []
    errors = 0
    total = min(count, len(df)) if count else len(df)

    for i, (_, row) in enumerate(df.iterrows()):
        if i >= total:
            break

        event = {
            "txn_id": str(row.get("txn_id", f"TXN_REPLAY_{i}")),
            "account_id": str(row.get("account_id", "")),
            "amount": float(row.get("amount", 0)),
            "txn_type": str(row.get("txn_type", "TRANSFER")),
            "channel": str(row.get("channel", "ONLINE")),
            "timestamp": str(row.get("timestamp", "")),
        }

        try:
            start = time.time()
            resp = requests.post(SCORING_URL, json=event, timeout=5)
            latency = (time.time() - start) * 1000
            latencies.append(latency)

            if resp.status_code == 200:
                result = resp.json()
                if (i + 1) % 50 == 0 or result.get("action") != "ALLOW":
                    logger.info(
                        f"  [{i+1}/{total}] {event['txn_id']}: "
                        f"score={result['final_risk_score']:.4f}, "
                        f"action={result['action']}, "
                        f"latency={latency:.1f}ms"
                    )
            else:
                errors += 1
                if errors <= 5:
                    logger.warning(f"  Error {resp.status_code}: {resp.text[:100]}")

        except requests.exceptions.ConnectionError:
            errors += 1
            if errors == 1:
                logger.error(f"Cannot connect to {SCORING_URL}. Is the API running?")
            if errors >= 5:
                logger.error("Too many connection errors. Stopping.")
                break

        # Rate limiting
        if rate > 0:
            time.sleep(1.0 / rate)

    # Summary
    if latencies:
        import numpy as np
        logger.info(f"\nReplay complete:")
        logger.info(f"  Transactions sent: {len(latencies)}")
        logger.info(f"  Errors: {errors}")
        logger.info(f"  Latency p50: {np.percentile(latencies, 50):.1f}ms")
        logger.info(f"  Latency p95: {np.percentile(latencies, 95):.1f}ms")
        logger.info(f"  Latency p99: {np.percentile(latencies, 99):.1f}ms")
        logger.info(f"  Latency max: {max(latencies):.1f}ms")


def replay_kafka(df: pd.DataFrame, rate: int = 100, topic: str = "txsentry.transactions"):
    """Replay transactions to Kafka topic."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python not installed. Use --mode http instead.")
        return

    logger.info(f"Replaying to Kafka topic '{topic}' at {rate} txn/s")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i, (_, row) in enumerate(df.iterrows()):
        event = {
            "txn_id": str(row.get("txn_id", f"TXN_REPLAY_{i}")),
            "account_id": str(row.get("account_id", "")),
            "amount": float(row.get("amount", 0)),
            "txn_type": str(row.get("txn_type", "TRANSFER")),
            "channel": str(row.get("channel", "ONLINE")),
            "timestamp": str(row.get("timestamp", "")),
        }
        producer.send(topic, event)

        if (i + 1) % 100 == 0:
            logger.info(f"  Sent {i+1}/{len(df)}")

        if rate > 0:
            time.sleep(1.0 / rate)

    producer.flush()
    logger.info(f"  Sent {len(df)} transactions to Kafka")


def main():
    parser = argparse.ArgumentParser(description="TxSentry Transaction Replay")
    parser.add_argument("--mode", choices=["http", "kafka"], default="http")
    parser.add_argument("--rate", type=int, default=50, help="Transactions per second")
    parser.add_argument("--count", type=int, default=200, help="Number of transactions")
    args = parser.parse_args()

    df = load_sample_transactions(args.count)

    if args.mode == "http":
        replay_http(df, rate=args.rate, count=args.count)
    else:
        replay_kafka(df, rate=args.rate)


if __name__ == "__main__":
    main()
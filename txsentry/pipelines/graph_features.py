"""Module 3: Graph Feature Engineering.

Builds a heterogeneous account-device-beneficiary graph from transaction data
and extracts node-level, topology, and community features.

Usage:
    python -m txsentry.pipelines.graph_features
"""

import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import networkx as nx
from community import community_louvain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

FEATURES_DIR = "data/features"
CANONICAL_DIR = "data/canonical"
SYNTHETIC_DIR = "data/synthetic"

# Limits to keep graph construction tractable on a laptop
MAX_EDGES_PER_TYPE = 2_000_000


def load_transaction_data() -> pd.DataFrame:
    """Load the feature table (already has both canonical + synthetic)."""
    path = f"{FEATURES_DIR}/txn_features.parquet"
    logger.info(f"Loading features from {path}...")
    # Only load columns we need for graph construction
    cols = [
        "txn_id", "account_id", "device_id", "ip_id",
        "beneficiary_id", "merchant_id", "amount",
        "timestamp", "is_fraud",
    ]
    df = pd.read_parquet(path, columns=cols)
    logger.info(f"Loaded {len(df):,} transactions")
    return df


def build_graph(txn_df: pd.DataFrame) -> nx.Graph:
    """Build a heterogeneous undirected graph from transaction relationships.

    Nodes: accounts, devices, IPs, beneficiaries
    Edges: account-device, device-IP, account-beneficiary, account-merchant
    """
    logger.info("Building heterogeneous graph...")
    G = nx.Graph()

    # --- Account-Device edges ---
    acc_dev = (
        txn_df[txn_df["device_id"].notna()]
        .groupby(["account_id", "device_id"])
        .agg(txn_count=("txn_id", "count"), total_amount=("amount", "sum"))
        .reset_index()
    )
    if len(acc_dev) > MAX_EDGES_PER_TYPE:
        acc_dev = acc_dev.nlargest(MAX_EDGES_PER_TYPE, "txn_count")
    for _, row in acc_dev.iterrows():
        G.add_node(row["account_id"], node_type="account")
        G.add_node(row["device_id"], node_type="device")
        G.add_edge(row["account_id"], row["device_id"],
                    edge_type="account_device",
                    weight=row["txn_count"])
    logger.info(f"  Account-Device edges: {len(acc_dev):,}")

    # --- Device-IP edges ---
    dev_ip = (
        txn_df[txn_df["device_id"].notna() & txn_df["ip_id"].notna()]
        .groupby(["device_id", "ip_id"])
        .agg(txn_count=("txn_id", "count"))
        .reset_index()
    )
    if len(dev_ip) > MAX_EDGES_PER_TYPE:
        dev_ip = dev_ip.nlargest(MAX_EDGES_PER_TYPE, "txn_count")
    for _, row in dev_ip.iterrows():
        G.add_node(row["device_id"], node_type="device")
        G.add_node(row["ip_id"], node_type="ip")
        G.add_edge(row["device_id"], row["ip_id"],
                    edge_type="device_ip",
                    weight=row["txn_count"])
    logger.info(f"  Device-IP edges: {len(dev_ip):,}")

    # --- Account-Beneficiary edges ---
    acc_bene = (
        txn_df[txn_df["beneficiary_id"].notna()]
        .groupby(["account_id", "beneficiary_id"])
        .agg(txn_count=("txn_id", "count"), total_amount=("amount", "sum"))
        .reset_index()
    )
    if len(acc_bene) > MAX_EDGES_PER_TYPE:
        acc_bene = acc_bene.nlargest(MAX_EDGES_PER_TYPE, "txn_count")
    for _, row in acc_bene.iterrows():
        G.add_node(row["account_id"], node_type="account")
        G.add_node(row["beneficiary_id"], node_type="beneficiary")
        G.add_edge(row["account_id"], row["beneficiary_id"],
                    edge_type="account_beneficiary",
                    weight=row["txn_count"])
    logger.info(f"  Account-Beneficiary edges: {len(acc_bene):,}")

    logger.info(f"  Graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    return G


def compute_node_features(G: nx.Graph, txn_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-account graph features."""
    logger.info("Computing node-level features...")

    accounts = [n for n, d in G.nodes(data=True) if d.get("node_type") == "account"]
    logger.info(f"  Computing features for {len(accounts):,} accounts")

    rows = []
    batch_size = 100_000
    for i in range(0, len(accounts), batch_size):
        batch = accounts[i:i + batch_size]
        if i > 0:
            logger.info(f"    ...processed {i:,}/{len(accounts):,}")

        for acc in batch:
            neighbors = list(G.neighbors(acc))
            degree = len(neighbors)

            # Neighbor type counts
            device_neighbors = [n for n in neighbors if G.nodes[n].get("node_type") == "device"]
            bene_neighbors = [n for n in neighbors if G.nodes[n].get("node_type") == "beneficiary"]

            # Device shared account count: for each device neighbor,
            # how many other accounts share it?
            shared_device_counts = []
            for dev in device_neighbors:
                dev_neighbors = [n for n in G.neighbors(dev)
                                 if G.nodes[n].get("node_type") == "account" and n != acc]
                shared_device_counts.append(len(dev_neighbors))

            device_shared_account_count = max(shared_device_counts) if shared_device_counts else 0

            # Beneficiary in-degree: how many accounts connect to each beneficiary?
            bene_in_degrees = []
            for bene in bene_neighbors:
                bene_acct_neighbors = [n for n in G.neighbors(bene)
                                       if G.nodes[n].get("node_type") == "account"]
                bene_in_degrees.append(len(bene_acct_neighbors))

            max_bene_in_degree = max(bene_in_degrees) if bene_in_degrees else 0

            rows.append({
                "account_id": acc,
                "account_degree": degree,
                "device_shared_account_count": device_shared_account_count,
                "beneficiary_in_degree": max_bene_in_degree,
                "num_devices": len(device_neighbors),
                "num_beneficiaries": len(bene_neighbors),
            })

    logger.info(f"  Node features computed for {len(rows):,} accounts")
    return pd.DataFrame(rows)


def compute_topology_flags(txn_df: pd.DataFrame) -> pd.DataFrame:
    """Compute boolean topology pattern flags per transaction.

    These are computed directly from transaction data (more efficient
    than traversing the graph for each pattern).
    """
    logger.info("Computing topology pattern flags...")

    # Work with a copy
    df = txn_df[["txn_id", "account_id", "beneficiary_id", "device_id",
                  "amount", "timestamp"]].copy()

    # --- Fan-out: account sent to 5+ beneficiaries in 24h ---
    logger.info("  Fan-out detection...")
    df["date"] = df["timestamp"].dt.date
    fan_out = (
        df[df["beneficiary_id"].notna()]
        .groupby(["account_id", "date"])["beneficiary_id"]
        .nunique()
        .reset_index(name="unique_benes_day")
    )
    fan_out_accounts = set(
        fan_out.loc[fan_out["unique_benes_day"] >= 5, "account_id"]
    )

    # --- Fan-in: beneficiary received from 5+ accounts in 24h ---
    logger.info("  Fan-in detection...")
    fan_in = (
        df[df["beneficiary_id"].notna()]
        .groupby(["beneficiary_id", "date"])["account_id"]
        .nunique()
        .reset_index(name="unique_senders_day")
    )
    fan_in_benes = set(
        fan_in.loc[fan_in["unique_senders_day"] >= 5, "beneficiary_id"]
    )

    # --- Shared device: device used by 3+ accounts ---
    logger.info("  Shared device detection...")
    shared_dev = (
        df[df["device_id"].notna()]
        .groupby("device_id")["account_id"]
        .nunique()
        .reset_index(name="unique_accounts")
    )
    shared_devices = set(
        shared_dev.loc[shared_dev["unique_accounts"] >= 3, "device_id"]
    )

    # --- Structuring: 3+ transactions $9000-$9999 to same bene in 3 days ---
    logger.info("  Structuring detection...")
    struct_mask = (df["amount"] >= 9000) & (df["amount"] <= 9999) & df["beneficiary_id"].notna()
    struct_df = df[struct_mask].copy()
    struct_df["date_group"] = struct_df["timestamp"].dt.floor("3D")
    structuring = (
        struct_df.groupby(["account_id", "beneficiary_id", "date_group"])
        .size()
        .reset_index(name="count")
    )
    structuring_accounts = set(
        structuring.loc[structuring["count"] >= 3, "account_id"]
    )

    # --- Mule chain: received then forwarded within 24h ---
    logger.info("  Mule chain detection...")
    # Accounts that appear as both sender and receiver (via beneficiary)
    senders = set(df["account_id"].unique())
    receivers_as_bene = set(df["beneficiary_id"].dropna().unique())
    mule_candidates = senders & receivers_as_bene

    # --- Assign flags to transactions ---
    logger.info("  Assigning flags...")
    flags = df[["txn_id", "account_id"]].copy()
    flags["is_fan_out_source"] = flags["account_id"].isin(fan_out_accounts)
    flags["is_fan_in_target"] = df["beneficiary_id"].isin(fan_in_benes)
    flags["is_shared_device"] = df["device_id"].isin(shared_devices)
    flags["structuring_flag"] = flags["account_id"].isin(structuring_accounts)
    flags["is_mule_chain_member"] = flags["account_id"].isin(mule_candidates)

    result = flags[["txn_id", "is_fan_out_source", "is_fan_in_target",
                      "is_shared_device", "structuring_flag", "is_mule_chain_member"]]

    for col in ["is_fan_out_source", "is_fan_in_target", "is_shared_device",
                "structuring_flag", "is_mule_chain_member"]:
        count = result[col].sum()
        logger.info(f"    {col}: {count:,} transactions flagged")

    return result


def compute_community_features(G: nx.Graph, txn_df: pd.DataFrame) -> pd.DataFrame:
    """Run Louvain community detection and compute community-level fraud rate."""
    logger.info("Running Louvain community detection...")

    # Extract account subgraph for community detection (faster)
    account_nodes = [n for n, d in G.nodes(data=True) if d.get("node_type") == "account"]

    # Build account-to-account graph via shared devices/beneficiaries
    logger.info("  Building account projection graph...")
    A = nx.Graph()
    A.add_nodes_from(account_nodes)

    # Connect accounts that share a device
    device_nodes = [n for n, d in G.nodes(data=True) if d.get("node_type") == "device"]
    for dev in device_nodes:
        accts = [n for n in G.neighbors(dev) if G.nodes[n].get("node_type") == "account"]
        for i in range(len(accts)):
            for j in range(i + 1, len(accts)):
                if A.has_edge(accts[i], accts[j]):
                    A[accts[i]][accts[j]]["weight"] += 1
                else:
                    A.add_edge(accts[i], accts[j], weight=1)

    logger.info(f"  Account projection: {A.number_of_nodes():,} nodes, {A.number_of_edges():,} edges")

    # Only run Louvain if graph has edges
    if A.number_of_edges() == 0:
        logger.warning("  No edges in account projection — skipping community detection")
        return pd.DataFrame({
            "account_id": account_nodes,
            "community_id": 0,
            "community_size": len(account_nodes),
            "community_fraud_rate": 0.0,
        })

    partition = community_louvain.best_partition(A, random_state=42)
    n_communities = len(set(partition.values()))
    logger.info(f"  Found {n_communities} communities")

    # Compute fraud rate per account (from transaction data)
    acct_fraud = (
        txn_df.groupby("account_id")["is_fraud"]
        .mean()
        .reset_index()
        .rename(columns={"is_fraud": "acct_fraud_rate"})
    )

    # Build community DataFrame
    comm_df = pd.DataFrame([
        {"account_id": acc, "community_id": comm}
        for acc, comm in partition.items()
    ])

    # Community size
    comm_sizes = comm_df["community_id"].value_counts().reset_index()
    comm_sizes.columns = ["community_id", "community_size"]
    comm_df = comm_df.merge(comm_sizes, on="community_id", how="left")

    # Community fraud rate (average fraud rate of accounts in same community)
    comm_df = comm_df.merge(acct_fraud, on="account_id", how="left")
    comm_df["acct_fraud_rate"] = comm_df["acct_fraud_rate"].fillna(0.0)
    comm_fraud = (
        comm_df.groupby("community_id")["acct_fraud_rate"]
        .mean()
        .reset_index()
        .rename(columns={"acct_fraud_rate": "community_fraud_rate"})
    )
    comm_df = comm_df.merge(comm_fraud, on="community_id", how="left")

    result = comm_df[["account_id", "community_id", "community_size", "community_fraud_rate"]]
    logger.info(f"  Community features computed for {len(result):,} accounts")
    return result


def run():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting Graph Feature Engineering Pipeline")
    logger.info("=" * 60)

    # Load data
    txn_df = load_transaction_data()

    # Build graph
    G = build_graph(txn_df)

    # Compute features
    node_features = compute_node_features(G, txn_df)
    topology_flags = compute_topology_flags(txn_df)
    community_features = compute_community_features(G, txn_df)

    # --- Merge all graph features ---
    logger.info("\nMerging graph features...")

    # Node features + community features (both keyed on account_id)
    account_graph_features = node_features.merge(
        community_features, on="account_id", how="outer"
    )

    # Fill NaN for accounts not in graph
    for col in account_graph_features.columns:
        if account_graph_features[col].dtype in [np.float64, np.int64]:
            account_graph_features[col] = account_graph_features[col].fillna(0)

    # Compute a composite graph risk score (0-1)
    account_graph_features["graph_risk_score"] = np.clip(
        (
            account_graph_features["device_shared_account_count"] * 0.15 +
            account_graph_features["beneficiary_in_degree"].clip(upper=20) / 20 * 0.25 +
            account_graph_features["community_fraud_rate"] * 0.30 +
            (account_graph_features["account_degree"] > 10).astype(float) * 0.15 +
            (account_graph_features["num_beneficiaries"] > 5).astype(float) * 0.15
        ),
        0.0, 1.0
    )

    # --- Write outputs ---
    logger.info("\nWriting output files...")
    Path(FEATURES_DIR).mkdir(parents=True, exist_ok=True)

    # Account-level graph features
    account_graph_features.to_parquet(
        f"{FEATURES_DIR}/graph_features.parquet",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    logger.info(f"  graph_features.parquet: {len(account_graph_features):,} accounts")

    # Transaction-level topology flags
    topology_flags.to_parquet(
        f"{FEATURES_DIR}/topology_flags.parquet",
        index=False,
    )
    logger.info(f"  topology_flags.parquet: {len(topology_flags):,} transactions")

    # --- Summary ---
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("GRAPH FEATURE ENGINEERING REPORT")
    logger.info("=" * 60)
    logger.info(f"Graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    logger.info(f"Accounts with graph features: {len(account_graph_features):,}")
    logger.info(f"Transactions with topology flags: {len(topology_flags):,}")

    logger.info(f"\nGraph risk score distribution:")
    score = account_graph_features["graph_risk_score"]
    logger.info(f"  mean={score.mean():.4f}, median={score.median():.4f}, "
                f"p95={score.quantile(0.95):.4f}, max={score.max():.4f}")

    logger.info(f"\nCommunity stats:")
    logger.info(f"  Communities: {account_graph_features['community_id'].nunique()}")
    logger.info(f"  Avg community size: {account_graph_features['community_size'].mean():.0f}")
    logger.info(f"  Avg community fraud rate: {account_graph_features['community_fraud_rate'].mean():.4f}")

    logger.info(f"\nOutput file sizes:")
    for f in sorted(Path(FEATURES_DIR).glob("*.parquet")):
        if f.is_dir():
            size_mb = sum(p.stat().st_size for p in f.rglob("*")) / (1024 * 1024)
        else:
            size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name}: {size_mb:.1f} MB")

    logger.info(f"\nPipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
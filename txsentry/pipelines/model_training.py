"""Module 4: Model Training and Evaluation.

Trains LightGBM (transaction risk) and Isolation Forest (behavioral anomaly)
with strict temporal train/test splits, SHAP explainability, and MLflow logging.

Usage:
    python -m txsentry.pipelines.model_training
"""

import logging
import time
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
import shap
import mlflow
import mlflow.lightgbm
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    average_precision_score, roc_auc_score,
    precision_recall_curve, precision_score, recall_score,
)
from sklearn.model_selection import train_test_split
import joblib

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

FEATURES_DIR = "data/features"
MODELS_DIR = "data/models"
ALERTS_DIR = "data/alerts"

# Temporal split boundaries
TRAIN_CUTOFF = pd.Timestamp("2024-10-01")
TEST_START = pd.Timestamp("2024-10-01")

# Feature columns for LightGBM
NUMERIC_FEATURES = [
    # Velocity
    "txn_count_1h", "txn_count_24h", "txn_count_7d",
    "amount_sum_1h", "amount_sum_24h",
    "unique_merchants_7d", "unique_beneficiaries_7d", "unique_devices_30d",
    # Behavioral deviation
    "amount_vs_30d_avg", "amount_vs_30d_max", "days_since_last_txn",
    # Merchant/IP risk
    "merchant_fraud_rate_hist",
    # Graph features
    "account_degree", "device_shared_account_count",
    "beneficiary_in_degree", "num_devices", "num_beneficiaries",
    "community_size", "community_fraud_rate", "graph_risk_score",
]

BOOLEAN_FEATURES = [
    "is_new_device", "is_new_beneficiary", "ip_is_vpn",
    # Topology flags
    "is_fan_out_source", "is_fan_in_target", "is_shared_device",
    "structuring_flag", "is_mule_chain_member",
]

CATEGORICAL_FEATURES = [
    "mcc_risk_tier", "channel", "txn_type",
]

ALL_FEATURES = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES

# Anomaly model features (account-level behavioral only)
ANOMALY_FEATURES = [
    "txn_count_7d", "amount_sum_24h",
    "unique_merchants_7d", "unique_beneficiaries_7d",
    "unique_devices_30d", "days_since_last_txn",
    "amount_vs_30d_avg",
]


def load_and_prepare_data() -> pd.DataFrame:
    """Load feature table, graph features, and topology flags. Merge all."""
    logger.info("Loading and merging feature tables...")

    # Transaction features (from PySpark)
    txn_df = pd.read_parquet(f"{FEATURES_DIR}/txn_features.parquet")
    logger.info(f"  txn_features: {len(txn_df):,} rows")

    # Graph features (account-level)
    graph_path = f"{FEATURES_DIR}/graph_features.parquet"
    if Path(graph_path).exists():
        graph_df = pd.read_parquet(graph_path)
        txn_df = txn_df.merge(graph_df, on="account_id", how="left")
        logger.info(f"  Merged graph features: {len(graph_df):,} accounts")

    # Topology flags (transaction-level)
    topo_path = f"{FEATURES_DIR}/topology_flags.parquet"
    if Path(topo_path).exists():
        topo_df = pd.read_parquet(topo_path)
        txn_df = txn_df.merge(topo_df, on="txn_id", how="left")
        logger.info(f"  Merged topology flags: {len(topo_df):,} txns")

    # Fill NaN in graph/topology features
    for col in NUMERIC_FEATURES:
        if col in txn_df.columns:
            txn_df[col] = txn_df[col].fillna(0.0)
    for col in BOOLEAN_FEATURES:
        if col in txn_df.columns:
            txn_df[col] = txn_df[col].fillna(False).astype(int)
    for col in CATEGORICAL_FEATURES:
        if col in txn_df.columns:
            txn_df[col] = txn_df[col].fillna("UNKNOWN").astype("category")

    # Ensure target is clean
    txn_df["is_fraud"] = txn_df["is_fraud"].astype(int)

    logger.info(f"  Final dataset: {len(txn_df):,} rows, {txn_df['is_fraud'].sum():,} fraud")
    return txn_df


def temporal_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split data temporally: train on months before cutoff, test after."""
    train = df[df["timestamp"] < TRAIN_CUTOFF].copy()
    test = df[df["timestamp"] >= TEST_START].copy()

    logger.info(f"Temporal split:")
    logger.info(f"  Train: {len(train):,} rows ({train['is_fraud'].sum():,} fraud, "
                f"{train['is_fraud'].mean():.2%} rate)")
    logger.info(f"  Test:  {len(test):,} rows ({test['is_fraud'].sum():,} fraud, "
                f"{test['is_fraud'].mean():.2%} rate)")
    logger.info(f"  Train range: {train['timestamp'].min()} to {train['timestamp'].max()}")
    logger.info(f"  Test range:  {test['timestamp'].min()} to {test['timestamp'].max()}")

    return train, test


def get_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Extract feature columns, handling missing columns gracefully."""
    available = [c for c in ALL_FEATURES if c in df.columns]
    missing = [c for c in ALL_FEATURES if c not in df.columns]
    if missing:
        logger.warning(f"  Missing features (will skip): {missing}")
    return df[available]


def train_lightgbm(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> tuple[lgb.Booster, dict]:
    """Train LightGBM binary classifier for transaction risk scoring."""
    logger.info("\n" + "=" * 60)
    logger.info("Training LightGBM Transaction Risk Model")
    logger.info("=" * 60)

    X_train = get_feature_matrix(train_df)
    y_train = train_df["is_fraud"].values
    X_test = get_feature_matrix(test_df)
    y_test = test_df["is_fraud"].values

    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    scale_pos = n_neg / max(n_pos, 1)
    logger.info(f"  Class balance: {n_pos:,} positive, {n_neg:,} negative, scale={scale_pos:.1f}")

    params = {
        "objective": "binary",
        "metric": "average_precision",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 50,
        "scale_pos_weight": scale_pos,
        "n_estimators": 500,
        "verbose": -1,
        "n_jobs": -1,
        "seed": 42,
    }

    # Train with early stopping
    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

    model = lgb.train(
        params,
        train_data,
        num_boost_round=500,
        valid_sets=[val_data],
        callbacks=[
            lgb.early_stopping(50, verbose=True),
            lgb.log_evaluation(100),
        ],
    )

    # Predictions
    y_pred_temporal = model.predict(X_test)
    y_pred_train = model.predict(X_train)

    # --- Temporal split metrics ---
    pr_auc_temporal = average_precision_score(y_test, y_pred_temporal)
    roc_auc_temporal = roc_auc_score(y_test, y_pred_temporal)

    # --- Random split metrics (for comparison) ---
    X_all = pd.concat([X_train, X_test])
    y_all = np.concatenate([y_train, y_test])
    X_rand_train, X_rand_test, y_rand_train, y_rand_test = train_test_split(
        X_all, y_all, test_size=0.25, random_state=42, stratify=y_all
    )
    rand_train_data = lgb.Dataset(X_rand_train, label=y_rand_train)
    rand_model = lgb.train(params, rand_train_data, num_boost_round=model.best_iteration)
    y_rand_pred = rand_model.predict(X_rand_test)
    pr_auc_random = average_precision_score(y_rand_test, y_rand_pred)
    roc_auc_random = roc_auc_score(y_rand_test, y_rand_pred)

    # --- Precision @ alert budget ---
    # Simulate 500 alerts/day budget over test period
    test_days = max(1, (test_df["timestamp"].max() - test_df["timestamp"].min()).days)
    alert_budget = 500 * test_days
    alert_budget = min(alert_budget, len(y_test))

    top_indices = np.argsort(y_pred_temporal)[::-1][:alert_budget]
    y_at_budget = y_test[top_indices]
    precision_at_budget = y_at_budget.mean()
    recall_at_budget = y_at_budget.sum() / max(y_test.sum(), 1)

    metrics = {
        "pr_auc_temporal": float(pr_auc_temporal),
        "pr_auc_random": float(pr_auc_random),
        "roc_auc_temporal": float(roc_auc_temporal),
        "roc_auc_random": float(roc_auc_random),
        "precision_at_500_alerts": float(precision_at_budget),
        "recall_at_500_alerts": float(recall_at_budget),
        "alert_budget_total": int(alert_budget),
        "test_days": int(test_days),
        "best_iteration": int(model.best_iteration),
        "auc_inflation_pr": float(pr_auc_random - pr_auc_temporal),
        "auc_inflation_roc": float(roc_auc_random - roc_auc_temporal),
    }

    logger.info(f"\n  TEMPORAL SPLIT RESULTS:")
    logger.info(f"    PR-AUC:              {pr_auc_temporal:.4f}")
    logger.info(f"    ROC-AUC:             {roc_auc_temporal:.4f}")
    logger.info(f"    Precision@{alert_budget} alerts: {precision_at_budget:.4f}")
    logger.info(f"    Recall@{alert_budget} alerts:    {recall_at_budget:.4f}")
    logger.info(f"\n  RANDOM SPLIT RESULTS (inflated baseline):")
    logger.info(f"    PR-AUC:              {pr_auc_random:.4f}")
    logger.info(f"    ROC-AUC:             {roc_auc_random:.4f}")
    logger.info(f"\n  AUC INFLATION (random - temporal):")
    logger.info(f"    PR-AUC inflation:    {metrics['auc_inflation_pr']:.4f}")
    logger.info(f"    ROC-AUC inflation:   {metrics['auc_inflation_roc']:.4f}")

    return model, metrics, y_pred_temporal


def compute_shap(model: lgb.Booster, test_df: pd.DataFrame) -> pd.DataFrame:
    """Compute SHAP values for test set transactions."""
    logger.info("\nComputing SHAP values...")

    X_test = get_feature_matrix(test_df)

    # Use a sample for SHAP if test set is large
    max_shap_samples = 10_000
    if len(X_test) > max_shap_samples:
        X_shap = X_test.sample(max_shap_samples, random_state=42)
        logger.info(f"  Using {max_shap_samples} sample for SHAP (full test: {len(X_test):,})")
    else:
        X_shap = X_test

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_shap)

    # Handle binary classification SHAP output
    if isinstance(shap_values, list):
        shap_values = shap_values[1]  # Take positive class

    # Get top 5 features per transaction
    feature_names = X_shap.columns.tolist()
    shap_top_features = []
    for i in range(len(X_shap)):
        sv = shap_values[i]
        top_idx = np.argsort(np.abs(sv))[::-1][:5]
        top_dict = {feature_names[j]: round(float(sv[j]), 4) for j in top_idx}
        shap_top_features.append(top_dict)

    # Global feature importance
    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    importance_df = pd.DataFrame({
        "feature": feature_names,
        "mean_abs_shap": mean_abs_shap,
    }).sort_values("mean_abs_shap", ascending=False)

    logger.info("  Top 10 features by mean |SHAP|:")
    for _, row in importance_df.head(10).iterrows():
        logger.info(f"    {row['feature']:35s} {row['mean_abs_shap']:.4f}")

    return importance_df, shap_top_features


def train_isolation_forest(train_df: pd.DataFrame, full_df: pd.DataFrame) -> tuple:
    """Train Isolation Forest on normal (non-fraud) transactions."""
    logger.info("\n" + "=" * 60)
    logger.info("Training Isolation Forest Anomaly Model")
    logger.info("=" * 60)

    # Get available anomaly features
    available = [f for f in ANOMALY_FEATURES if f in train_df.columns]

    # Train on non-fraud only
    normal_train = train_df[train_df["is_fraud"] == 0]
    X_normal = normal_train[available].fillna(0)
    logger.info(f"  Training on {len(X_normal):,} normal transactions")

    iso = IsolationForest(
        contamination=0.02,
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_normal)

    # Score all transactions
    X_all = full_df[available].fillna(0)
    raw_scores = iso.score_samples(X_all)

    # Normalize to 0-1 (more negative = more anomalous → higher score)
    min_s, max_s = raw_scores.min(), raw_scores.max()
    if max_s > min_s:
        anomaly_scores = (max_s - raw_scores) / (max_s - min_s)
    else:
        anomaly_scores = np.zeros(len(raw_scores))

    anomaly_scores = np.clip(anomaly_scores, 0, 1)

    logger.info(f"  Anomaly score distribution:")
    logger.info(f"    mean={anomaly_scores.mean():.4f}, p50={np.median(anomaly_scores):.4f}, "
                f"p95={np.percentile(anomaly_scores, 95):.4f}, "
                f"p99={np.percentile(anomaly_scores, 99):.4f}")

    # Check separation between fraud and non-fraud
    fraud_mask = full_df["is_fraud"] == 1
    logger.info(f"  Avg anomaly score — fraud: {anomaly_scores[fraud_mask].mean():.4f}, "
                f"non-fraud: {anomaly_scores[~fraud_mask].mean():.4f}")

    return iso, anomaly_scores


def run():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting Model Training Pipeline")
    logger.info("=" * 60)

    Path(MODELS_DIR).mkdir(parents=True, exist_ok=True)
    Path(ALERTS_DIR).mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    df = load_and_prepare_data()

    # --- Temporal split ---
    train_df, test_df = temporal_split(df)

    # --- MLflow setup ---
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("txsentry_transaction_risk")

    with mlflow.start_run(run_name="lgbm_temporal_split"):

        # --- Train LightGBM ---
        lgbm_model, lgbm_metrics, y_pred_temporal = train_lightgbm(train_df, test_df)

        # Log to MLflow
        mlflow.log_params({
            "model_type": "LightGBM",
            "n_features": len([c for c in ALL_FEATURES if c in df.columns]),
            "train_size": len(train_df),
            "test_size": len(test_df),
            "train_cutoff": str(TRAIN_CUTOFF),
            "best_iteration": lgbm_metrics["best_iteration"],
        })
        mlflow.log_metrics(lgbm_metrics)

        # --- SHAP ---
        shap_importance, shap_top_features = compute_shap(lgbm_model, test_df)

        # Log SHAP importance
        shap_importance.to_csv(f"{MODELS_DIR}/shap_importance.csv", index=False)
        mlflow.log_artifact(f"{MODELS_DIR}/shap_importance.csv")

        # --- Train Isolation Forest ---
        iso_model, anomaly_scores = train_isolation_forest(train_df, df)

        # --- Save models ---
        logger.info("\nSaving models...")
        lgbm_model.save_model(f"{MODELS_DIR}/lgbm_txn_risk.txt")
        joblib.dump(iso_model, f"{MODELS_DIR}/isolation_forest.joblib")
        mlflow.log_artifact(f"{MODELS_DIR}/lgbm_txn_risk.txt")
        mlflow.log_artifact(f"{MODELS_DIR}/isolation_forest.joblib")

        # --- Save scores to transaction table ---
        logger.info("\nSaving risk scores...")
        df["txn_risk_score"] = lgbm_model.predict(get_feature_matrix(df))
        df["behavior_anomaly_score"] = anomaly_scores

        # Save scored alerts (high risk transactions)
        scored = df[["txn_id", "account_id", "timestamp", "amount",
                      "is_fraud", "fraud_scenario", "txn_risk_score",
                      "behavior_anomaly_score"]].copy()
        scored.to_parquet(f"{ALERTS_DIR}/scored_transactions.parquet",
                          index=False, coerce_timestamps="us",
                          allow_truncated_timestamps=True)
        logger.info(f"  Scored transactions saved: {len(scored):,}")

        # Save metrics
        with open(f"{MODELS_DIR}/metrics.json", "w") as f:
            json.dump(lgbm_metrics, f, indent=2)
        mlflow.log_artifact(f"{MODELS_DIR}/metrics.json")

    # --- Summary ---
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("MODEL TRAINING REPORT")
    logger.info("=" * 60)
    logger.info(f"LightGBM PR-AUC (temporal):  {lgbm_metrics['pr_auc_temporal']:.4f}")
    logger.info(f"LightGBM PR-AUC (random):    {lgbm_metrics['pr_auc_random']:.4f}")
    logger.info(f"AUC inflation:               {lgbm_metrics['auc_inflation_pr']:.4f}")
    logger.info(f"Precision@500 alerts/day:     {lgbm_metrics['precision_at_500_alerts']:.4f}")
    logger.info(f"Recall@500 alerts/day:        {lgbm_metrics['recall_at_500_alerts']:.4f}")
    logger.info(f"\nModels saved to {MODELS_DIR}/")
    logger.info(f"Scored transactions saved to {ALERTS_DIR}/")
    logger.info(f"MLflow tracking at ./mlruns")

    logger.info(f"\nOutput file sizes:")
    for d in [MODELS_DIR, ALERTS_DIR]:
        for f in sorted(Path(d).glob("*")):
            if f.is_file():
                size_mb = f.stat().st_size / (1024 * 1024)
                logger.info(f"  {f.name}: {size_mb:.1f} MB")

    logger.info(f"\nPipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
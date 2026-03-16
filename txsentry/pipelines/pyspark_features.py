"""Module 2: PySpark Feature Engineering Pipeline.

Computes velocity, behavioral deviation, and merchant/IP risk features
over the unified canonical + synthetic transaction dataset.

Usage:
    python -m txsentry.pipelines.pyspark_features
"""

import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CANONICAL_DIR = "data/canonical"
SYNTHETIC_DIR = "data/synthetic"
FEATURES_DIR = "data/features"


def create_spark_session() -> SparkSession:
    """Create a local Spark session optimized for single-machine processing."""
    spark = (
        SparkSession.builder
        .appName("txsentry_features")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.nanosAsLong", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_and_union_transactions(spark: SparkSession) -> DataFrame:
    """Load canonical and synthetic transactions, union them."""
    dfs = []

    canonical_path = f"{CANONICAL_DIR}/transaction_event.parquet"
    if Path(canonical_path).exists():
        canonical = spark.read.parquet(canonical_path)
        logger.info(f"Loaded canonical: {canonical.count():,} rows")
        dfs.append(canonical)

    synthetic_path = f"{SYNTHETIC_DIR}/transaction_event.parquet"
    if Path(synthetic_path).exists():
        synthetic = spark.read.parquet(synthetic_path)
        logger.info(f"Loaded synthetic: {synthetic.count():,} rows")
        dfs.append(synthetic)

    if not dfs:
        raise RuntimeError("No transaction data found!")

    # Union all sources with matching columns
    # Select only the common columns to avoid schema mismatches
    common_cols = [
        "txn_id", "account_id", "merchant_id", "beneficiary_id",
        "device_id", "ip_id", "amount", "currency", "txn_type",
        "channel", "timestamp", "is_fraud", "fraud_scenario", "source",
    ]

    unified = dfs[0].select(*[F.col(c) for c in common_cols])
    for df in dfs[1:]:
        unified = unified.unionByName(df.select(*[F.col(c) for c in common_cols]))

    logger.info(f"Unified dataset: {unified.count():,} rows")
    return unified


def load_entity_tables(spark: SparkSession) -> dict[str, DataFrame]:
    """Load supplementary entity tables for join-based features."""
    tables = {}

    # Merchants (synthetic only — canonical has no merchants)
    merchant_path = f"{SYNTHETIC_DIR}/merchant.parquet"
    if Path(merchant_path).exists():
        tables["merchant"] = spark.read.parquet(merchant_path)
        logger.info(f"Loaded merchants: {tables['merchant'].count():,}")

    # IP addresses — try synthetic first, then canonical
    for prefix in [SYNTHETIC_DIR, CANONICAL_DIR]:
        ip_path = f"{prefix}/ip_address.parquet"
        if Path(ip_path).exists():
            tables["ip_address"] = spark.read.parquet(ip_path)
            logger.info(f"Loaded IPs from {prefix}: {tables['ip_address'].count():,}")
            break

    # Devices
    for prefix in [SYNTHETIC_DIR, CANONICAL_DIR]:
        dev_path = f"{prefix}/device.parquet"
        if Path(dev_path).exists():
            tables["device"] = spark.read.parquet(dev_path)
            logger.info(f"Loaded devices from {prefix}: {tables['device'].count():,}")
            break

    # Accounts
    for prefix in [SYNTHETIC_DIR, CANONICAL_DIR]:
        acct_path = f"{prefix}/account.parquet"
        if Path(acct_path).exists():
            tables["account"] = spark.read.parquet(acct_path)
            logger.info(f"Loaded accounts from {prefix}: {tables['account'].count():,}")
            break

    return tables


def add_timestamp_epoch(df: DataFrame) -> DataFrame:
    """Add a long column with unix timestamp for range-based windows."""
    return df.withColumn("ts_epoch", F.unix_timestamp("timestamp"))


def compute_velocity_features(df: DataFrame) -> DataFrame:
    """Compute transaction count and amount velocity over rolling windows.

    Uses rangeBetween on unix timestamp for exact time-based windows.
    """
    logger.info("Computing velocity features...")

    # Define windows partitioned by account, ordered by epoch
    def time_window(seconds: int):
        return (
            Window.partitionBy("account_id")
            .orderBy("ts_epoch")
            .rangeBetween(-seconds, 0)
        )

    HOUR = 3600
    DAY = 86400

    df = (
        df
        # Transaction counts
        .withColumn("txn_count_1h", F.count("txn_id").over(time_window(1 * HOUR)))
        .withColumn("txn_count_24h", F.count("txn_id").over(time_window(24 * HOUR)))
        .withColumn("txn_count_7d", F.count("txn_id").over(time_window(7 * DAY)))
        # Amount sums
        .withColumn("amount_sum_1h", F.sum("amount").over(time_window(1 * HOUR)))
        .withColumn("amount_sum_24h", F.sum("amount").over(time_window(24 * HOUR)))
        # Unique entity counts
        .withColumn("unique_merchants_7d",
                     F.size(F.collect_set("merchant_id").over(time_window(7 * DAY))))
        .withColumn("unique_beneficiaries_7d",
                     F.size(F.collect_set("beneficiary_id").over(time_window(7 * DAY))))
        .withColumn("unique_devices_30d",
                     F.size(F.collect_set("device_id").over(time_window(30 * DAY))))
    )

    logger.info("  Velocity features complete")
    return df


def compute_behavioral_features(df: DataFrame) -> DataFrame:
    """Compute behavioral deviation features using 30-day rolling stats."""
    logger.info("Computing behavioral deviation features...")

    DAY = 86400
    w30d = (
        Window.partitionBy("account_id")
        .orderBy("ts_epoch")
        .rangeBetween(-30 * DAY, -1)  # Exclude current row to avoid leakage
    )

    # Account-level rolling stats (30-day lookback, excluding current txn)
    df = (
        df
        .withColumn("_avg_30d", F.avg("amount").over(w30d))
        .withColumn("_max_30d", F.max("amount").over(w30d))
        .withColumn("amount_vs_30d_avg",
                     F.when(F.col("_avg_30d").isNotNull() & (F.col("_avg_30d") > 0),
                            F.col("amount") / F.col("_avg_30d"))
                     .otherwise(1.0))
        .withColumn("amount_vs_30d_max",
                     F.when(F.col("_max_30d").isNotNull() & (F.col("_max_30d") > 0),
                            F.col("amount") / F.col("_max_30d"))
                     .otherwise(1.0))
        .drop("_avg_30d", "_max_30d")
    )

    # Days since last transaction
    w_prev = (
        Window.partitionBy("account_id")
        .orderBy("ts_epoch")
        .rowsBetween(-1, -1)
    )
    df = df.withColumn(
        "days_since_last_txn",
        F.when(
            F.lag("ts_epoch").over(Window.partitionBy("account_id").orderBy("ts_epoch")).isNotNull(),
            (F.col("ts_epoch") - F.lag("ts_epoch").over(
                Window.partitionBy("account_id").orderBy("ts_epoch")
            )) / DAY
        ).otherwise(0.0)
    )

    # Is new device (not seen on this account in prior 30 days)
    w30d_devices = (
        Window.partitionBy("account_id")
        .orderBy("ts_epoch")
        .rangeBetween(-30 * DAY, -1)
    )
    df = df.withColumn(
        "_prev_devices",
        F.collect_set("device_id").over(w30d_devices)
    ).withColumn(
        "is_new_device",
        ~F.array_contains(F.col("_prev_devices"), F.col("device_id"))
    ).drop("_prev_devices")

    # Is new beneficiary (not seen in prior 30 days)
    df = df.withColumn(
        "_prev_benes",
        F.collect_set("beneficiary_id").over(w30d_devices)
    ).withColumn(
        "is_new_beneficiary",
        F.when(
            F.col("beneficiary_id").isNotNull(),
            ~F.array_contains(F.col("_prev_benes"), F.col("beneficiary_id"))
        ).otherwise(False)
    ).drop("_prev_benes")

    logger.info("  Behavioral features complete")
    return df


def compute_merchant_ip_features(df: DataFrame, entities: dict) -> DataFrame:
    """Join merchant fraud rate and IP risk flags."""
    logger.info("Computing merchant/IP risk features...")

    # Merchant fraud rate
    if "merchant" in entities:
        merchant_df = entities["merchant"].select(
            F.col("merchant_id"),
            F.col("fraud_rate_hist").alias("merchant_fraud_rate_hist"),
            F.col("category_code").alias("mcc_code"),
        )
        df = df.join(F.broadcast(merchant_df), on="merchant_id", how="left")

        # MCC risk tier
        df = df.withColumn(
            "mcc_risk_tier",
            F.when(F.col("merchant_fraud_rate_hist") > 0.04, "HIGH")
            .when(F.col("merchant_fraud_rate_hist") > 0.015, "MEDIUM")
            .otherwise("LOW")
        )
    else:
        df = (
            df
            .withColumn("merchant_fraud_rate_hist", F.lit(0.0))
            .withColumn("mcc_code", F.lit(None).cast(StringType()))
            .withColumn("mcc_risk_tier", F.lit("LOW"))
        )

    # IP features
    if "ip_address" in entities:
        ip_df = entities["ip_address"].select(
            F.col("ip_id"),
            F.col("is_vpn").alias("ip_is_vpn"),
            F.col("country").alias("ip_country"),
        )
        df = df.join(F.broadcast(ip_df), on="ip_id", how="left")
    else:
        df = (
            df
            .withColumn("ip_is_vpn", F.lit(False))
            .withColumn("ip_country", F.lit(None).cast(StringType()))
        )

    # Fill nulls from failed joins
    df = (
        df
        .withColumn("merchant_fraud_rate_hist",
                     F.coalesce(F.col("merchant_fraud_rate_hist"), F.lit(0.0)))
        .withColumn("ip_is_vpn",
                     F.coalesce(F.col("ip_is_vpn"), F.lit(False)))
        .withColumn("mcc_risk_tier",
                     F.coalesce(F.col("mcc_risk_tier"), F.lit("LOW")))
    )

    logger.info("  Merchant/IP features complete")
    return df


def compute_account_profiles(df: DataFrame) -> DataFrame:
    """Compute per-account behavioral profiles (30-day rolling stats).

    Written as a separate table for the anomaly model.
    """
    logger.info("Computing account profiles...")

    profiles = (
        df.groupBy("account_id")
        .agg(
            F.count("txn_id").alias("total_txn_count"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.stddev("amount").alias("std_amount"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
            F.countDistinct("beneficiary_id").alias("unique_beneficiaries"),
            F.countDistinct("device_id").alias("unique_devices"),
            F.avg("txn_count_7d").alias("avg_txn_count_7d"),
            F.avg("amount_sum_24h").alias("avg_amount_sum_24h"),
            F.max("is_fraud").cast("boolean").alias("has_fraud"),
        )
        .withColumn("std_amount", F.coalesce(F.col("std_amount"), F.lit(0.0)))
    )

    logger.info(f"  Account profiles: {profiles.count():,} accounts")
    return profiles


def run():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting PySpark Feature Engineering Pipeline")
    logger.info("=" * 60)

    spark = create_spark_session()
    logger.info(f"Spark session created: {spark.sparkContext.uiWebUrl}")

    # --- Load data ---
    logger.info("\nLoading transaction data...")
    txn_df = load_and_union_transactions(spark)
    entities = load_entity_tables(spark)

    # Add epoch column for window functions
    txn_df = add_timestamp_epoch(txn_df)

    # Cache the base dataframe since we'll use it multiple times
    txn_df.cache()
    row_count = txn_df.count()
    logger.info(f"Cached {row_count:,} transactions")

    # --- Compute features ---
    logger.info("\nComputing features...")

    # Velocity features (window functions)
    txn_df = compute_velocity_features(txn_df)

    # Behavioral deviation features
    txn_df = compute_behavioral_features(txn_df)

    # Merchant/IP risk features (joins)
    txn_df = compute_merchant_ip_features(txn_df, entities)

    # --- Write feature table ---
    logger.info("\nWriting feature tables...")
    Path(FEATURES_DIR).mkdir(parents=True, exist_ok=True)

    # Select final feature columns
    feature_cols = [
        # IDs and labels
        "txn_id", "account_id", "merchant_id", "beneficiary_id",
        "device_id", "ip_id", "timestamp", "amount",
        "txn_type", "channel", "is_fraud", "fraud_scenario", "source",
        # Velocity
        "txn_count_1h", "txn_count_24h", "txn_count_7d",
        "amount_sum_1h", "amount_sum_24h",
        "unique_merchants_7d", "unique_beneficiaries_7d", "unique_devices_30d",
        # Behavioral deviation
        "amount_vs_30d_avg", "amount_vs_30d_max",
        "days_since_last_txn", "is_new_device", "is_new_beneficiary",
        # Merchant/IP risk
        "merchant_fraud_rate_hist", "mcc_code", "mcc_risk_tier",
        "ip_is_vpn", "ip_country",
    ]

    features_df = txn_df.select(*feature_cols)
    features_df.write.mode("overwrite").parquet(f"{FEATURES_DIR}/txn_features.parquet")
    logger.info(f"  Wrote txn_features.parquet")

    # Account profiles
    profiles_df = compute_account_profiles(txn_df)
    profiles_df.write.mode("overwrite").parquet(f"{FEATURES_DIR}/account_profiles.parquet")
    logger.info(f"  Wrote account_profiles.parquet")

    # --- Summary ---
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("FEATURE ENGINEERING REPORT")
    logger.info("=" * 60)

    # Read back to get file sizes
    for f in sorted(Path(FEATURES_DIR).iterdir()):
        if f.is_dir():
            size_mb = sum(p.stat().st_size for p in f.rglob("*")) / (1024 * 1024)
            logger.info(f"  {f.name}: {size_mb:.1f} MB")
        elif f.is_file():
            size_mb = f.stat().st_size / (1024 * 1024)
            logger.info(f"  {f.name}: {size_mb:.1f} MB")

    logger.info(f"\nPipeline complete in {elapsed:.1f}s")

    spark.stop()
    logger.info("Spark session stopped")


if __name__ == "__main__":
    run()
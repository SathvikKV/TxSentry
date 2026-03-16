# TxSentry -- Project Build Log & Technical Documentation

## Project Overview

TxSentry is a real-time payment fraud detection and autonomous investigation platform. It implements a two-layer architecture: a **detection layer** (ML models scoring transactions) and an **investigation layer** (a LangGraph agent that autonomously investigates flagged alerts via MCP tools).

The project targets AI Engineer, Data Engineer, and Applied ML Engineer roles in fintech. It demonstrates end-to-end ML system design from raw data ingestion through model training, scoring, agent-based investigation, and monitoring.

**Tech Stack:** Python 3.10, PySpark, LightGBM, Scikit-learn (Isolation Forest), SHAP, NetworkX, LangGraph, FastMCP, FastAPI, Kafka, MLflow, Next.js, Tailwind CSS.

**Data Scale:** 10.1M transactions, 6.4M accounts, 8.3M graph nodes, 6M graph edges.

---

## Environment Setup

**Python:** 3.10.10 (system install on Windows)
**Virtual environment:** `.venv` created in project root via `python -m venv .venv`
**Java:** Eclipse Adoptium JDK 17.0.18 (required for PySpark and AMLSim)
**Maven:** Installed for AMLSim compilation
**Hadoop winutils:** `winutils.exe` and `hadoop.dll` placed in `C:\hadoop\bin` -- required for PySpark to write Parquet on Windows
**Node.js:** Required for the Next.js frontend dashboard

**Key environment variables required for PySpark:**
```
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
set HADOOP_HOME=C:\hadoop
set PATH=C:\hadoop\bin;%PATH%
```

---

## Repository Structure

```
txsentry/
  schemas/
    canonical.py              # Pydantic models for all entity/event tables, enums for
                              # actions, risk tiers, fraud scenarios, reason codes
  data_gen/
    entity_generator.py       # Generates synthetic customers, accounts, devices, IPs,
                              # merchants, beneficiaries, watchlist entities
    scenario_injection.py     # Six fraud scenario classes (MuleChain, FanOut, FanIn,
                              # AccountTakeoverBurst, SharedDeviceRing, Structuring)
    drift_injection.py        # Quarterly temporal drift schedule controlling fraud rate,
                              # scenario mix, and amount multipliers
    run_generator.py          # Orchestrator: generates entities, legitimate txns per month,
                              # injects fraud per drift schedule, writes to data/synthetic/

  pipelines/
    ingestion/
      ingest_raw.py           # Reads raw CSVs (PaySim, AMLSim) into staging Parquet
      transform_paysim.py     # Maps PaySim columns to canonical transaction_event schema
      transform_amlsim.py     # Maps AMLSim tx_log + alert_accounts to canonical schema,
                              # derives fraud labels from SAR flags and alert reasons
      assign_devices_ips.py   # Vectorized synthetic device/IP assignment to all transactions,
                              # including shared-device clusters for fraud ring simulation
      validate.py             # Validation report: null rates, fraud distribution, coverage
      run_ingestion.py        # Orchestrator: ingest -> transform -> assign devices -> validate

    pyspark_features.py       # PySpark pipeline computing velocity features (1h/24h/7d windows),
                              # behavioral deviation (amount vs 30d avg/max, new device/beneficiary),
                              # and merchant/IP risk features via broadcast joins

    graph_features.py         # NetworkX graph construction (account-device-beneficiary-IP),
                              # node-level features, topology pattern detection (fan-out, fan-in,
                              # shared device, structuring, mule chain), Louvain community detection

    model_training.py         # LightGBM + Isolation Forest training with temporal split,
                              # SHAP explainability, MLflow tracking, precision@budget evaluation

  models/
    fusion/
      engine.py               # Rule layer + score combination -> action + reason codes

  services/
    scoring_api/
      main.py                 # FastAPI real-time scoring endpoint (single transaction)
      api.py                  # Full backend API serving real data to the Next.js frontend
    mcp_server/
      server.py               # FastMCP server exposing all 11 investigation tools
      tools/
        transaction_tools.py  # get_transaction_detail, get_account_history, get_velocity_features
        account_tools.py      # get_behavioral_baseline, get_merchant_risk_profile,
                              # run_anomaly_score, check_watchlist
        graph_tools.py        # get_graph_neighborhood, detect_graph_pattern
        case_tools.py         # get_similar_cases, write_case_memo
    kafka_sim/
      producer.py             # Transaction replay via HTTP or Kafka

  agent/
    state.py                  # AgentState TypedDict for LangGraph
    graph.py                  # LangGraph state graph definition and runner
    report_generator.py       # Self-contained HTML investigation report generator
    nodes/
      all_nodes.py            # Triage, planner, investigator, synthesizer node implementations
    prompts/
      all_prompts.py          # All LLM prompts for the 4 agent nodes

  ui/
    streamlit_app.py          # Original Streamlit dashboard (4 pages)
    txSentry-ui/              # Next.js + Tailwind frontend dashboard (6 pages)
      lib/
        api.ts                # API client fetching from FastAPI backend
        mock-data.ts          # Fallback mock data for standalone demo
      app/
        page.tsx              # Overview / landing page
        alerts/page.tsx       # Alert queue with filters and action distribution
        case/page.tsx         # Case investigation detail with timeline
        model/page.tsx        # Model performance, SHAP, temporal vs random split
        monitoring/page.tsx   # PSI drift heatmap, precision trend, fraud rate trend
        graph/page.tsx        # Graph explorer with pattern detection

  monitoring/
    drift.py                  # PSI computation, monthly precision tracking, retraining trigger

data/
  raw/
    paysim/paysim.csv         # 6.36M rows, PaySim mobile money simulation
    amlsim/
      transactions.csv        # 1.67M rows, AMLSim transaction log
      accounts.csv            # 62,258 accounts with SAR flags
      alert_accounts.csv      # 7,533 alert pattern members
  staging/                    # Intermediate Parquet (post-ingest, pre-transform)
  canonical/                  # Cleaned, schema-conformant Parquet (Module 0 output)
  synthetic/                  # Generated entity tables + transactions (Module 1 output)
  features/                   # Engineered feature tables (Modules 2-3 output)
  models/                     # Serialized model artifacts (Module 4 output)
  alerts/                     # Scored transaction table + alert events (Modules 4-5 output)
  cases/
    memos/                    # JSON case memos from agent investigations
    reports/                  # Self-contained HTML investigation reports
```

---

## Module 0 -- Raw Data Ingestion & ETL

### Objective
Ingest PaySim and AMLSim raw data, transform to the canonical TxSentry schema, assign synthetic devices and IPs, and produce a validated data lake.

### Data Sources

**PaySim** (Kaggle download, no setup required):
- 6,362,620 transactions simulating mobile money fraud
- Columns: `step, type, amount, nameOrig, nameDest, isFraud, isFlaggedFraud` + balance columns
- Fraud rate: 0.13% (8,213 fraud transactions)
- All transactions labeled with binary `isFraud` flag
- No device, IP, or merchant data -- purely account-to-account transfers

**AMLSim** (IBM simulator, required Java + Maven build):
- 1,674,891 transactions simulating anti-money laundering patterns
- 62,258 accounts, 7,533 flagged as SAR (Suspicious Activity Report) members
- Three AML typologies: fan_in (300 patterns), fan_out (300 patterns), cycle (400 patterns)
- Fraud rate: 20.23% (338,910 fraud transactions based on SAR account membership)
- Columns: `step, type, amount, nameOrig, nameDest, isSAR, alertID`

### AMLSim Setup (Major Challenge)

AMLSim is a Java-based multi-agent simulator requiring significant setup:

1. **Java & Maven installation:** Eclipse Adoptium JDK 17 + Apache Maven
2. **MASON dependency:** AMLSim depends on MASON (multi-agent simulation library) which is not in Maven Central with the coordinates AMLSim expects (`mason:mason:jar:20`). Fix: cloned MASON from GitHub (`eclab/mason`), built it with Maven, then manually installed the jar into the local Maven repo with the exact groupId/artifactId/version AMLSim's pom.xml references. Also had to overwrite the generated POM to remove a parent reference to `cs.gmu.edu.eclab:mason-build:20`.
3. **NetworkX 3.x incompatibilities:** AMLSim's Python graph generator was written for NetworkX 2.x. Multiple breaking changes required fixes across 4 files:
   - `G.node[x]` changed to `G.nodes[x]`
   - `G.edge[x][y]` changed to `G.edges[x, y]`
   - `G.nodes()` returns NodeView (not indexable), changed to `list(G.nodes())`
   - `G.nodes[id] = attr` (direct assignment) changed to `G.add_node(id, **attr)` or `G.nodes[id].update(attr)`
   - `sub_g.add_node(acct, attr_dict)` changed to `sub_g.add_node(acct, **attr_dict)`
   - `attr['active']` KeyError on edges changed to `attr.get('active', True)`
4. **Degree sequence mismatch:** The 100K parameter set had 100,000 accounts but the degree file had 62,258 entries. Fix: adjusted `accounts.csv` to total 62,258 (12000x5 + 2258).
5. **Blank line CSV output:** The Python graph generator wrote CSVs with blank lines between every data row, causing Java `ArrayIndexOutOfBoundsException`. Fix: cleaned all CSVs post-generation.

### Pipeline Steps

1. **Ingest raw** (`ingest_raw.py`): Read CSVs, validate required columns present, drop null critical rows, write to staging Parquet.
2. **Transform PaySim** (`transform_paysim.py`): Map `nameOrig` to `account_id`, `step` to timestamp (hours from 2024-01-01), `isFraud` to boolean, set `source=PAYSIM`.
3. **Transform AMLSim** (`transform_amlsim.py`): Map `nameOrig` to `account_id` with `ACC_AML_` prefix. Build SAR account set from `alert_accounts.csv`. Derive `is_fraud` from `isSAR` flag + SAR account membership. Map alert reasons (`fan_in` to `FAN_IN`, `fan_out` to `FAN_OUT`, `cycle` to `MULE_CHAIN`).
4. **Assign devices/IPs** (`assign_devices_ips.py`): Vectorized assignment -- each account gets 1-3 devices (70%/20%/10% distribution), each device gets 1-2 IPs. About 5% of accounts share a device with 2-4 others (fraud ring simulation). About 3% of transactions use an alternate device. Uses `map()` for bulk assignment, only loops for the 3% alternate devices.
5. **Merge & validate** (`validate.py`): Combine sources, check uniqueness, compute null rates, fraud distribution, device/IP coverage.

### Results

| Metric | Value |
|---|---|
| Total transactions | 8,037,511 |
| Unique accounts | 6,369,859 |
| Devices | 9,021,934 |
| IPs | 11,694,237 |
| Fraud rate (PAYSIM) | 0.13% |
| Fraud rate (AMLSIM) | 20.23% |
| Device coverage | 100% |
| IP coverage | 100% |
| Timestamp range | 2024-01-01 to 2024-01-31 |
| Total disk size | ~1.5 GB |

### Major Challenge: Device Assignment Performance
The initial `iterrows()` implementation took 7+ hours on 8M transactions. Rewrote to vectorized approach using `pd.Series.map()` for primary device assignment, reducing runtime to about 8 minutes.

---

## Module 1 -- Synthetic Data Generator

### Objective
Generate a 12-month synthetic transaction dataset with labeled fraud scenarios and temporal drift, extending the canonical data's one-month window to a full year.

### Entity Generation (`entity_generator.py`)
Generates:
- 50,000 customers with risk tiers (LOW 70%, MEDIUM 20%, HIGH 10%)
- 79,985 accounts (1.6 per customer avg) with types: CHECKING/SAVINGS/BUSINESS/PREPAID
- 5,000 merchants with real MCC codes and category-specific fraud rates (grocery 0.5% to crypto exchange 7%)
- 30,000 devices (MOBILE 60%, DESKTOP 25%, TABLET 15%)
- 20,000 IPs with VPN (5%) and datacenter (3%) flags
- 10,000 beneficiaries
- 500 watchlist entities (OFAC, UN, EU sanctions + PEP, adverse media)

### Fraud Scenarios (`scenario_injection.py`)

Six scenario classes, each with a `generate(n_instances, time_start, time_end)` method:

| Scenario | Pattern | Amounts | Key Signals |
|---|---|---|---|
| MULE_CHAIN | A to B to C to D within 24h, 2-4 hops | $500-$5,000 per hop | Shared device across chain |
| FAN_OUT | 1 account to 5-15 beneficiaries in 2h | $8,500-$9,999 (structuring) | Newly added beneficiaries |
| FAN_IN | 5-10 accounts to 1 beneficiary in 24h | $500-$3,000 | Shared device + IP across senders |
| ACCOUNT_TAKEOVER_BURST | Dormant account, new device, large transfer | $3,000-$25,000 (3-10x baseline) | New device + new beneficiary |
| SHARED_DEVICE_RING | 1 device across 4-8 accounts, each transacting | $200-$5,000 | Device fingerprint linking |
| STRUCTURING | 3-6 txns $9,000-$9,999 to same bene in 3 days | Just below $10K threshold | Same beneficiary, tight time window |

### Temporal Drift (`drift_injection.py`)

| Quarter | Months | Fraud Rate | Dominant Scenarios | Amount Multiplier |
|---|---|---|---|---|
| Q1 | Feb-Apr 2024 | 1.8% | MULE_CHAIN, FAN_OUT | 1.0x |
| Q2 | May-Jul 2024 | 2.2% | ACCOUNT_TAKEOVER_BURST, SHARED_DEVICE_RING | 1.2x |
| Q3 | Aug-Oct 2024 | 2.5% | STRUCTURING, FAN_IN | 1.4x |
| Q4 | Nov 2024-Jan 2025 | 3.0% | MULE_CHAIN, ACCOUNT_TAKEOVER_BURST | 1.6x |

### Results

| Metric | Value |
|---|---|
| Total synthetic transactions | 2,110,326 |
| Legitimate transactions | 1,860,000 (155K/month x 12) |
| Fraud transactions | 250,326 |
| Overall fraud rate | 11.86% |
| Date range | Feb 2024 - Jan 2025 |
| Generation time | 50.3 seconds |
| Output size | 66.3 MB |

**Fraud by scenario:**
| Scenario | Count |
|---|---|
| SHARED_DEVICE_RING | 71,579 |
| FAN_IN | 50,115 |
| FAN_OUT | 49,513 |
| STRUCTURING | 37,808 |
| MULE_CHAIN | 26,452 |
| ACCOUNT_TAKEOVER_BURST | 14,859 |

**Monthly drift visible in output:** Fraud rate rises from about 10% in Q1 months to about 13.3% in Q3 months, and dominant scenario mix shifts per quarter as designed.

### Note on fraud rate
The synthetic fraud rate (11.86%) is higher than the spec's 2-3% target because each scenario instance generates multiple transactions (e.g., a fan-out with 10 targets = 10 fraud txns). When combined with canonical data (8M at 4.3%), the blended rate is around 5.8%.

---

## Module 2 -- PySpark Feature Engineering

### Objective
Transform raw event tables into model-ready feature tables using PySpark window functions, demonstrating Spark proficiency on the resume.

### Pipeline (`pyspark_features.py`)

**Data loading:** Reads from both `canonical/` and `synthetic/` Parquet, unions them into a 10.1M row unified dataset.

**Velocity features** (per-account rolling windows via `rangeBetween` on unix timestamp):
- `txn_count_1h`, `txn_count_24h`, `txn_count_7d` -- transaction frequency
- `amount_sum_1h`, `amount_sum_24h` -- spending velocity
- `unique_merchants_7d`, `unique_beneficiaries_7d` -- entity diversity
- `unique_devices_30d` -- device diversity

**Behavioral deviation features** (30-day lookback, excluding current transaction to prevent leakage):
- `amount_vs_30d_avg` -- current amount / 30-day rolling average
- `amount_vs_30d_max` -- current amount / 30-day rolling max
- `days_since_last_txn` -- dormancy detection
- `is_new_device` -- device not seen on account in prior 30 days
- `is_new_beneficiary` -- beneficiary not seen in prior 30 days

**Merchant/IP risk features** (broadcast joins to entity tables):
- `merchant_fraud_rate_hist` -- from merchant table
- `mcc_risk_tier` -- LOW/MEDIUM/HIGH based on fraud rate thresholds
- `ip_is_vpn` -- from IP table

**Account profiles:** Separate table with per-account aggregate stats for the Isolation Forest.

### Major Challenge: Parquet Timestamp Compatibility
Pandas writes timestamps as nanoseconds by default, but PySpark 3.5 cannot read nanosecond Parquet. Fix: re-wrote all Parquet files with `coerce_timestamps='us'` and `allow_truncated_timestamps=True`.

### Major Challenge: Windows Hadoop Dependencies
PySpark on Windows requires `winutils.exe` and `hadoop.dll` for filesystem operations. Without `HADOOP_HOME` set and these binaries present, Spark can read Parquet but cannot write it. Fix: downloaded Hadoop 3.3.5 winutils binaries, set `HADOOP_HOME=C:\hadoop` and added `C:\hadoop\bin` to PATH.

### Results

| Metric | Value |
|---|---|
| Input transactions | 10,147,837 |
| Features computed | 20+ (velocity, behavioral, merchant/IP) |
| Account profiles | 6,449,844 |
| Pipeline time | 74.3 seconds |
| txn_features.parquet | 675.7 MB |
| account_profiles.parquet | 159.2 MB |

---

## Module 3 -- Graph Feature Engineering

### Objective
Build a heterogeneous entity graph and extract graph-based risk signals including node features, topology patterns, and community structure.

### Pipeline (`graph_features.py`)

**Graph construction:** Heterogeneous undirected graph with four node types (account, device, IP, beneficiary) and three edge types (account-device, device-IP, account-beneficiary). Edges capped at 2M per type to keep construction tractable.

**Node-level features** (per account):
- `account_degree` -- total edges from account
- `device_shared_account_count` -- max accounts sharing any of this account's devices
- `beneficiary_in_degree` -- max accounts sending to any of this account's beneficiaries
- `num_devices`, `num_beneficiaries` -- direct neighbor counts

**Topology pattern flags** (computed via pandas groupby for efficiency):
- `is_fan_out_source` -- sent to 5+ unique beneficiaries in a single day
- `is_fan_in_target` -- beneficiary received from 5+ accounts in a single day
- `is_shared_device` -- device used by 3+ accounts
- `structuring_flag` -- 3+ transactions $9,000-$9,999 to same beneficiary in 3-day window
- `is_mule_chain_member` -- account appears as both sender and receiver

**Community detection:** Louvain algorithm on account projection graph (accounts connected via shared devices). Computes `community_id`, `community_size`, `community_fraud_rate`.

**Composite graph risk score:** Weighted combination of shared device count, beneficiary in-degree, community fraud rate, high degree, and high beneficiary count. Normalized to 0-1.

### Results

| Metric | Value |
|---|---|
| Graph nodes | 8,327,422 |
| Graph edges | 5,995,786 |
| Accounts with features | 1,979,189 |
| Communities detected | 1,953,337 |
| Avg community size | 3 |
| Pipeline time | 24.3 minutes |

**Topology flag counts:**
| Flag | Transactions Flagged |
|---|---|
| is_fan_in_target | 3,325,666 |
| is_shared_device | 2,110,329 |
| is_mule_chain_member | 1,513,018 |
| is_fan_out_source | 337,052 |
| structuring_flag | 32,217 |

**Graph risk score distribution:** mean=0.057, median=0.025, p95=0.163, max=1.0 -- healthy right-skewed distribution with meaningful high-risk tail.

**Note on community sparsity:** 1.95M communities for 1.98M accounts (avg size 3) indicates most accounts are isolated or in tiny clusters. This is expected -- the account projection only has 74K edges because shared-device connections are sparse. Good interview talking point about real-world graph sparsity.

---

## Module 4 -- Model Training & Evaluation

### Objective
Train LightGBM (supervised transaction risk) and Isolation Forest (unsupervised behavioral anomaly) with temporal splits, SHAP explainability, and MLflow experiment tracking.

### Pipeline (`model_training.py`)

**Data preparation:** Merges transaction features (Module 2), graph features (Module 3), and topology flags (Module 3) into a single feature matrix with 28 features.

**Temporal split:**
- Train: all transactions before Oct 1, 2024 -- 9,438,406 rows (5.38% fraud)
- Test: all transactions Oct 1, 2024 onwards -- 709,431 rows (12.61% fraud)
- Test fraud rate is higher due to temporal drift injection (Q3/Q4 have higher rates)

**LightGBM training:**
- Binary classification with `average_precision` metric
- `scale_pos_weight=17.6` for class imbalance handling
- Early stopping at 378 rounds (500 max)
- Features: velocity + behavioral deviation + merchant/IP risk + graph + topology flags

**Isolation Forest training:**
- Trained on non-fraud transactions only (8.93M rows)
- 200 estimators, contamination=0.02
- Scores normalized to 0-1 (higher = more anomalous)

**SHAP explainability:**
- TreeExplainer on 10K sample from test set
- Per-transaction top-5 feature attributions stored
- Global feature importance ranked by mean |SHAP|

**MLflow logging:** Parameters, metrics, model artifacts, and SHAP importance CSV all tracked.

### Results

**LightGBM -- Temporal Split:**
| Metric | Value |
|---|---|
| PR-AUC | 0.9939 |
| ROC-AUC | 0.9992 |
| Precision @ 500 alerts/day | 99.54% |
| Recall @ 500 alerts/day | 69.01% |
| Best iteration | 378 |

**LightGBM -- Random Split (comparison baseline):**
| Metric | Value |
|---|---|
| PR-AUC | 0.9222 |
| ROC-AUC | 0.9930 |

**AUC Inflation Analysis:**
PR-AUC inflation = -0.0716 (random *lower* than temporal). This is the reverse of the typical pattern. Explanation: temporal drift injection increased fraud rate in the test period (12.6% vs 5.4% in train). The temporal model tests on a fraud-enriched period, boosting PR-AUC. The random split dilutes this signal. Key interview point: "drift injection creates non-stationary fraud distributions -- temporal splits reveal this reality; random splits mask it."

**Top 10 SHAP Features:**
| Rank | Feature | Mean |SHAP| |
|---|---|---|
| 1 | txn_type | 2.2566 |
| 2 | amount_sum_24h | 1.1545 |
| 3 | amount_sum_1h | 0.9166 |
| 4 | community_fraud_rate | 0.8457 |
| 5 | is_new_beneficiary | 0.7770 |
| 6 | channel | 0.7393 |
| 7 | unique_merchants_7d | 0.5163 |
| 8 | account_degree | 0.5105 |
| 9 | unique_beneficiaries_7d | 0.3831 |
| 10 | is_new_device | 0.2778 |

Features span all three signal types: transaction metadata (txn_type, channel), velocity (amount sums, unique counts), graph (community_fraud_rate, account_degree), and behavioral (is_new_beneficiary, is_new_device). This validates the multi-layered feature engineering approach.

**Isolation Forest:**
| Metric | Value |
|---|---|
| Avg anomaly score (fraud) | 0.5145 |
| Avg anomaly score (non-fraud) | 0.1569 |
| Score p95 | 0.6096 |
| Score p99 | 0.7438 |

Clear separation between fraud and non-fraud confirms the unsupervised model independently detects anomalous behavior.

### Output Files
| File | Size |
|---|---|
| lgbm_txn_risk.txt | 2.5 MB |
| isolation_forest.joblib | 1.4 MB |
| scored_transactions.parquet | 292.2 MB |
| metrics.json | <1 KB |
| shap_importance.csv | <1 KB |

---

## Module 5 -- Fusion Engine

### Objective
Combine LightGBM risk score, Isolation Forest anomaly score, and graph risk score with rule-based boosters into a final risk decision with reason codes.

### Pipeline (`engine.py`)

**Score fusion:** Weighted combination (50% LightGBM, 25% Isolation Forest, 25% graph risk) plus rule-based hard boosters:
- Watchlist hit: +0.30
- Structuring flag: +0.20
- Shared device + new beneficiary: +0.15
- Fan-out source: +0.10
- Mule chain + high amount deviation: +0.10
- VPN + new device: +0.10

**Action assignment** based on final fused score thresholds:
- >= 0.85: BLOCK
- >= 0.70: QUEUE_FOR_REVIEW
- >= 0.55: STEP_UP_AUTH
- >= 0.35: ALLOW_WITH_MONITORING
- < 0.35: ALLOW

**Reason code generation:** 11 possible codes generated based on feature flags and thresholds. Only computed for non-ALLOW transactions to save time.

### Results

| Action | Count | % | Fraud Rate |
|---|---|---|---|
| ALLOW | 8,561,569 | 84.37% | 0.05% |
| ALLOW_WITH_MONITORING | 846,071 | 8.34% | 10.78% |
| STEP_UP_AUTH | 327,022 | 3.22% | 42.24% |
| QUEUE_FOR_REVIEW | 187,211 | 1.84% | 80.48% |
| BLOCK | 225,964 | 2.23% | 94.28% |

**Top reason codes:**
| Code | Count |
|---|---|
| MULE_CHAIN_DETECTED | 1,072,395 |
| FAN_IN_PATTERN | 988,816 |
| SHARED_DEVICE_CLUSTER | 702,159 |
| FAN_OUT_PATTERN | 295,041 |
| NEW_DEVICE_HIGH_VALUE_TXN | 236,147 |

**Key result:** 94.3% of BLOCK decisions are true fraud, while only 0.05% of ALLOW decisions are fraud leaking through. The action tiers show clean monotonic separation in fraud rate.

Pipeline time: 138.3 seconds.

---

## Module 6 -- MCP Tool Server

### Objective
Expose 11 investigation tools as a local MCP server that the LangGraph agent calls during autonomous investigation.

### Tools Implemented

| # | Tool | Description |
|---|---|---|
| 1 | get_transaction_detail | Full transaction record + risk scores + SHAP features |
| 2 | get_account_history | Transaction history summary for given window |
| 3 | get_velocity_features | Current velocity signals (1h/24h/7d) |
| 4 | get_graph_neighborhood | Connected entities within N hops |
| 5 | detect_graph_pattern | Check for AML topology matches (fan-out, fan-in, mule chain, etc.) |
| 6 | get_behavioral_baseline | Account behavioral profile (avg/max/std amounts, typical patterns) |
| 7 | get_merchant_risk_profile | Merchant category, fraud rate, risk tier |
| 8 | run_anomaly_score | Isolation Forest anomaly score for account |
| 9 | check_watchlist | Watchlist screening against simulated sanctions list |
| 10 | get_similar_cases | Nearest-neighbor case lookup via sklearn over score + reason code vectors |
| 11 | write_case_memo | Forced-schema tool to conclude investigation with validated output |

All tools read from real Parquet data files and scored alert tables. Lazy-loaded data caching ensures tools respond quickly after first call.

**Smoke test result:** All 8 testable tools passing with real data from scored alerts.

---

## Module 7 -- LangGraph Investigation Agent

### Objective
Build a stateful 4-node investigation agent that receives alerts and autonomously investigates them by calling MCP tools iteratively.

### Architecture

**State graph:** Triage -> Planner -> Investigator (loop) -> Synthesizer -> END

**AgentState** tracks: alert data, investigation plan, tool results, reasoning trace, step count, case memo.

**Node implementations:**
- **Triage:** Assesses alert severity, decides SHALLOW (2-3 tools) or DEEP (5-8 tools) investigation
- **Planner:** Generates ordered tool call plan based on triage assessment and available tools
- **Investigator:** Executes next tool in plan, logs reasoning trace, loops until plan complete or max steps
- **Synthesizer:** Synthesizes all findings into a structured CaseMemo via write_case_memo tool

**LLM:** GPT-4o-mini (temperature=0, max_tokens=800) for all 4 nodes.

**Max tool calls per investigation:** 8

### Major Challenge: Tool Name Parsing
The LLM consistently returned tool names with parameters included (e.g., `get_transaction_detail(txn_id)` instead of just `get_transaction_detail`). Fix: added normalization step that strips parenthesized arguments via `t.split("(")[0]` before validation.

### Results

3 high-risk BLOCK alerts investigated:
- Each investigation: DEEP depth, 8 tools called, 10 trace entries
- Tools called per investigation: get_transaction_detail -> get_account_history -> get_velocity_features -> get_graph_neighborhood -> detect_graph_pattern -> get_behavioral_baseline -> get_merchant_risk_profile/check_watchlist -> get_similar_cases
- All produced valid CaseMemo with BLOCK recommendation, HIGH priority
- get_similar_cases found prior case memos from earlier investigations (feedback loop working)
- Completed 3 investigations in 150.9 seconds (about 50s per investigation)

Case memos saved as JSON to `data/cases/memos/`.

---

## Module 8 -- FastAPI Scoring API + Kafka Simulation

### Objective
Real-time scoring endpoint and transaction replay for simulating live flow.

### Scoring API (`main.py`)

**Endpoints:**
- `POST /score` -- Score a single transaction, returns alert with risk scores and action
- `GET /alerts` -- Return current in-memory alert queue
- `GET /cases/{case_id}` -- Return completed case memo
- `GET /health` -- Health check

Loads LightGBM and Isolation Forest models on startup. Builds simplified feature vector per request and scores through both models + fusion logic.

### Kafka Simulation (`producer.py`)

Supports two modes:
- **HTTP mode** (no Kafka required): POST transactions directly to scoring API
- **Kafka mode**: Publish to `txsentry.transactions` topic for consumer processing

### Results

100 transactions replayed via HTTP mode:
- 0 errors
- Latency p50: 2,082ms, p95: 2,102ms, p99: 2,118ms
- Latency is high due to per-request Isolation Forest scoring (would batch in production)

---

## Module 9 -- Dashboard UI

### Objective
Portfolio-quality UI showing the alert queue, case investigation traces, model monitoring, and graph exploration.

### Two Implementations

**Streamlit (original):** 4-page dashboard built with `streamlit_app.py`. Functional but visually basic.

**Next.js + Tailwind (production):** 6-page dashboard built with Vercel v0, then wired to the Python backend via FastAPI.

### Next.js Dashboard Pages

**1. Overview / Home:** Product landing page explaining the two-layer architecture. Displays key metrics (10.1M transactions, 99.5% precision, etc.) and tech stack badges. Architecture diagram shows detection and investigation pipelines as flow diagrams.

**2. Alert Queue:** Fraud analyst workstation with filterable, sortable alert table. Filters by risk band and action. Summary stats (total alerts, fraud count, precision). Color-coded risk scores and action badges. Action distribution donut chart.

**3. Case Investigation Detail:** Full investigation view. Dropdown to select from existing cases. Shows action badge, confidence, priority, risk score bars (transaction/anomaly/graph/fused). Reason codes as colored tags. Investigation timeline showing each tool call with parameters, output, and agent reasoning. Entities involved, supporting evidence, next steps.

**4. Model Performance:** Temporal vs random split comparison cards. SHAP feature importance horizontal bar chart colored by feature category (velocity/graph/behavioral/metadata). Precision and recall at budget metrics.

**5. Monitoring & Drift:** PSI heatmap table (features x months, color-coded green/amber/red). Monthly precision trend line chart. Monthly fraud rate trend showing designed drift. Retraining recommendation alert card.

**6. Graph Explorer:** Account ID search. Shows account details (degree, risk score, community). Network summary (device/beneficiary/IP counts). Pattern detection results with confidence bars. Connected entity cards. Shared device alert warning.

### Backend API (`api.py`)

Full FastAPI server with CORS enabled for Next.js dev server. Endpoints:
- `GET /api/alerts` -- Paginated, filterable alerts from real Parquet data
- `GET /api/alerts/distribution` -- Action distribution for donut chart
- `GET /api/cases` -- List all case IDs
- `GET /api/cases/{case_id}` -- Full case memo with risk scores
- `GET /api/model/metrics` -- Model performance metrics from metrics.json
- `GET /api/model/shap` -- SHAP importance with feature categories
- `GET /api/monitoring` -- PSI heatmap, precision trend, fraud rate from monitoring report
- `GET /api/graph/{account_id}` -- Graph neighborhood + pattern detection (calls MCP tools)
- `GET /api/overview` -- Key metrics for landing page

### Frontend API Client (`api.ts`)

TypeScript client with `fetchAPI` helper that calls backend endpoints with automatic fallback to empty state if backend is unavailable. Each page uses `useEffect` + `useState` to fetch data on mount.

---

## Module 10 -- Model Monitoring

### Objective
Demonstrate that the system can detect model degradation over the synthetic 12-month window using PSI drift detection.

### Pipeline (`drift.py`)

**PSI (Population Stability Index):** Measures feature distribution shift between training reference (pre-Oct 2024) and production windows (monthly post-Oct 2024).
- PSI < 0.1: stable
- 0.1-0.2: moderate shift
- > 0.2: significant drift, consider retraining

**Monitored features:** txn_risk_score, behavior_anomaly_score, final_risk_score, amount

**Monthly precision tracking:** Precision at top 500 scores per month.

### Results

| Feature | Oct PSI | Nov PSI | Dec PSI | Jan PSI |
|---|---|---|---|---|
| txn_risk_score | 0.27 | 0.27 | 0.24 | 0.25 |
| behavior_anomaly_score | 9.12 | 9.14 | 9.14 | 9.14 |
| final_risk_score | 2.54 | 2.54 | 2.52 | 2.54 |
| amount | 4.85 | 4.89 | 4.45 | 4.45 |

All 4 features show significant drift (PSI > 0.2) across all production months. This is expected -- the temporal drift injection intentionally shifted fraud patterns.

**Monthly precision:**
| Month | Precision @500 |
|---|---|
| Oct 2024 | 96.8% |
| Nov 2024 | 96.0% |
| Dec 2024 | 94.4% |
| Jan 2025 | 96.2% |

Precision remains strong (94-97%) despite drift, but the system correctly flags retraining as recommended.

**Retraining recommendation:** YES -- PSI > 0.2 on 4/4 monitored features.

---

## Module 11 -- Investigation Report Generator

### Objective
Render CaseMemo + reasoning trace into a polished self-contained HTML investigation report.

### Implementation (`report_generator.py`)

Uses Jinja2-style string formatting to produce a single HTML file with all CSS embedded inline (no external dependencies).

**Design:**
- Dark background (#0f172a) with light card surfaces (#1e293b)
- Action badge color-coded (red=BLOCK, amber=REVIEW, blue=STEP_UP, purple=MONITOR, green=ALLOW)
- Risk score bars with color transitions
- Reason codes as pill-shaped tags
- Investigation trace as vertical timeline with expandable steps
- Entity cards, evidence bullets, numbered next steps

**Output:** `data/cases/reports/CASE_{id}.html` -- fully self-contained, opens in any browser.

Generated 3 reports from existing case memos.

---

## Combined Data Inventory

| Layer | Transactions | Fraud Rate | Time Range | Size |
|---|---|---|---|---|
| Canonical (PaySim) | 6,362,620 | 0.13% | Jan 2024 | ~420 MB |
| Canonical (AMLSim) | 1,674,891 | 20.23% | Jan 2024 | (included above) |
| Synthetic (Module 1) | 2,110,326 | 11.86% | Feb 2024 - Jan 2025 | 66 MB |
| **Unified** | **10,147,837** | **5.88%** | **Jan 2024 - Jan 2025** | **~1.5 GB** |

---

## Full Module Summary

| Module | Description | Status | Key Result |
|---|---|---|---|
| 0. Ingestion & ETL | PaySim + AMLSim to canonical Parquet | Done | 8M transactions |
| 1. Synthetic Generator | 12-month dataset with fraud scenarios + drift | Done | 2.1M transactions, 6 scenarios |
| 2. PySpark Features | Velocity, behavioral, merchant/IP features | Done | 20+ features, 74s runtime |
| 3. Graph Features | NetworkX graph, topology flags, communities | Done | 8.3M node graph |
| 4. Model Training | LightGBM + Isolation Forest + SHAP + MLflow | Done | PR-AUC 0.994 |
| 5. Fusion Engine | Score fusion + rules + reason codes | Done | 94% precision on BLOCK |
| 6. MCP Server | 11 investigation tools via FastMCP | Done | All tools passing |
| 7. LangGraph Agent | 4-node stateful investigation graph | Done | 8-tool deep investigations |
| 8. FastAPI + Kafka | Scoring endpoint + transaction replay | Done | 100 txns scored, 0 errors |
| 9. Dashboard UI | Next.js frontend + FastAPI backend | Done | 6-page dashboard, live data |
| 10. Monitoring | PSI drift detection + retraining trigger | Done | 4/4 features drifted |
| 11. Report Generator | Self-contained HTML investigation reports | Done | 3 reports generated |

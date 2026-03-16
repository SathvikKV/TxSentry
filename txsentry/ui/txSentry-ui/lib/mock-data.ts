// TxSentry Mock Data

export type RiskBand = 'CRITICAL' | 'HIGH' | 'ELEVATED' | 'MEDIUM' | 'LOW'
export type Action = 'BLOCK' | 'QUEUE_FOR_REVIEW' | 'STEP_UP_AUTH' | 'ALLOW_WITH_MONITORING' | 'ALLOW'

export interface Alert {
  alertId: string
  timestamp: string
  accountId: string
  amount: number
  riskScore: number
  riskBand: RiskBand
  action: Action
  isFraud: boolean
}

export const alerts: Alert[] = [
  { alertId: 'ALT_00059801', timestamp: '2024-12-15 14:23:45', accountId: 'ACC_AML_152', amount: 983.83, riskScore: 0.995, riskBand: 'CRITICAL', action: 'BLOCK', isFraud: true },
  { alertId: 'ALT_00059802', timestamp: '2024-12-15 14:21:12', accountId: 'ACC_PS_C1234567', amount: 2500.00, riskScore: 0.92, riskBand: 'CRITICAL', action: 'BLOCK', isFraud: true },
  { alertId: 'ALT_00059803', timestamp: '2024-12-15 14:18:33', accountId: 'ACC_ML_8821', amount: 15750.00, riskScore: 0.88, riskBand: 'HIGH', action: 'QUEUE_FOR_REVIEW', isFraud: true },
  { alertId: 'ALT_00059804', timestamp: '2024-12-15 14:15:08', accountId: 'ACC_FI_3392', amount: 450.25, riskScore: 0.75, riskBand: 'HIGH', action: 'STEP_UP_AUTH', isFraud: false },
  { alertId: 'ALT_00059805', timestamp: '2024-12-15 14:12:55', accountId: 'ACC_TX_7741', amount: 8900.00, riskScore: 0.68, riskBand: 'ELEVATED', action: 'STEP_UP_AUTH', isFraud: true },
  { alertId: 'ALT_00059806', timestamp: '2024-12-15 14:10:22', accountId: 'ACC_AML_287', amount: 125.50, riskScore: 0.55, riskBand: 'ELEVATED', action: 'ALLOW_WITH_MONITORING', isFraud: false },
  { alertId: 'ALT_00059807', timestamp: '2024-12-15 14:08:11', accountId: 'ACC_PS_D4567890', amount: 3200.00, riskScore: 0.52, riskBand: 'MEDIUM', action: 'ALLOW_WITH_MONITORING', isFraud: false },
  { alertId: 'ALT_00059808', timestamp: '2024-12-15 14:05:44', accountId: 'ACC_ML_9932', amount: 75.00, riskScore: 0.35, riskBand: 'MEDIUM', action: 'ALLOW', isFraud: false },
  { alertId: 'ALT_00059809', timestamp: '2024-12-15 14:03:28', accountId: 'ACC_FI_1123', amount: 1850.00, riskScore: 0.28, riskBand: 'LOW', action: 'ALLOW', isFraud: false },
  { alertId: 'ALT_00059810', timestamp: '2024-12-15 14:01:15', accountId: 'ACC_TX_5582', amount: 50.00, riskScore: 0.15, riskBand: 'LOW', action: 'ALLOW', isFraud: false },
  { alertId: 'ALT_00059811', timestamp: '2024-12-14 16:45:33', accountId: 'ACC_AML_443', amount: 25000.00, riskScore: 0.99, riskBand: 'CRITICAL', action: 'BLOCK', isFraud: true },
  { alertId: 'ALT_00059812', timestamp: '2024-12-14 16:42:18', accountId: 'ACC_PS_E7891234', amount: 6500.00, riskScore: 0.85, riskBand: 'HIGH', action: 'QUEUE_FOR_REVIEW', isFraud: true },
  { alertId: 'ALT_00059813', timestamp: '2024-12-14 15:38:55', accountId: 'ACC_ML_2241', amount: 1200.00, riskScore: 0.72, riskBand: 'HIGH', action: 'STEP_UP_AUTH', isFraud: false },
  { alertId: 'ALT_00059814', timestamp: '2024-12-14 15:35:42', accountId: 'ACC_FI_8874', amount: 340.00, riskScore: 0.58, riskBand: 'ELEVATED', action: 'ALLOW_WITH_MONITORING', isFraud: false },
  { alertId: 'ALT_00059815', timestamp: '2024-12-14 14:22:11', accountId: 'ACC_TX_3328', amount: 4500.00, riskScore: 0.42, riskBand: 'MEDIUM', action: 'ALLOW', isFraud: false },
  { alertId: 'ALT_00059816', timestamp: '2024-11-28 09:15:22', accountId: 'ACC_AML_551', amount: 12800.00, riskScore: 0.91, riskBand: 'CRITICAL', action: 'BLOCK', isFraud: true },
  { alertId: 'ALT_00059817', timestamp: '2024-11-15 11:32:44', accountId: 'ACC_PS_F2345678', amount: 890.00, riskScore: 0.78, riskBand: 'HIGH', action: 'QUEUE_FOR_REVIEW', isFraud: true },
  { alertId: 'ALT_00059818', timestamp: '2024-10-22 08:55:18', accountId: 'ACC_ML_6673', amount: 2100.00, riskScore: 0.62, riskBand: 'ELEVATED', action: 'STEP_UP_AUTH', isFraud: false },
  { alertId: 'ALT_00059819', timestamp: '2024-10-10 14:48:33', accountId: 'ACC_FI_9912', amount: 550.00, riskScore: 0.38, riskBand: 'MEDIUM', action: 'ALLOW', isFraud: false },
  { alertId: 'ALT_00059820', timestamp: '2024-10-05 10:22:05', accountId: 'ACC_TX_1147', amount: 175.00, riskScore: 0.12, riskBand: 'LOW', action: 'ALLOW', isFraud: false },
]

export const mockCase = {
  caseId: 'CASE_00059801',
  alertId: 'ALT_00059801',
  accountId: 'ACC_AML_152',
  amount: 983.83,
  action: 'BLOCK' as Action,
  confidence: 99.5,
  priority: 'HIGH' as const,
  reasonCodes: ['FAN_OUT_PATTERN', 'FAN_IN_PATTERN', 'AMOUNT_4X_BASELINE', 'MULE_CHAIN_DETECTED'],
  summary: 'This transaction was flagged due to a critical risk score and multiple indicators of a mule chain fraud scenario.',
  riskScores: {
    txnRisk: 0.98,
    anomaly: 0.67,
    graph: 0.85,
    final: 0.995
  },
  investigationSteps: [
    {
      step: 1,
      tool: 'get_transaction_detail',
      params: { txn_id: 'TXN_00059801' },
      output: 'Transaction of $983.83 from ACC_AML_152 to beneficiary BEN_7721 via mobile channel at 14:23:45 UTC',
      reasoning: 'Starting investigation by retrieving the full transaction details to understand the context.'
    },
    {
      step: 2,
      tool: 'get_account_history',
      params: { account_id: 'ACC_AML_152', days: 30 },
      output: '47 transactions in last 30 days. Sudden spike in activity starting 3 days ago. Previous avg: 5 txn/week.',
      reasoning: 'Checking recent account behavior to identify any unusual patterns or velocity changes.'
    },
    {
      step: 3,
      tool: 'get_velocity_features',
      params: { account_id: 'ACC_AML_152' },
      output: 'amount_sum_24h: $4,521.50 (4.2x baseline), unique_beneficiaries_24h: 8 (vs avg 1.2), txn_count_1h: 5',
      reasoning: 'Velocity features show significant deviation from baseline - this is consistent with mule account behavior.'
    },
    {
      step: 4,
      tool: 'get_graph_neighborhood',
      params: { account_id: 'ACC_AML_152', hops: 2 },
      output: 'Connected to 12 accounts via shared device DEV_A8X92. 3 accounts flagged as confirmed fraud in past 90 days.',
      reasoning: 'Graph analysis reveals shared device cluster with known fraud accounts - strong indicator of coordinated activity.'
    },
    {
      step: 5,
      tool: 'detect_graph_pattern',
      params: { account_id: 'ACC_AML_152' },
      output: 'Patterns detected: FAN_OUT (confidence: 0.94), FAN_IN (confidence: 0.87), MULE_CHAIN (confidence: 0.91)',
      reasoning: 'Multiple high-confidence fraud patterns detected. The mule chain pattern is particularly concerning.'
    },
    {
      step: 6,
      tool: 'get_behavioral_baseline',
      params: { account_id: 'ACC_AML_152' },
      output: 'Baseline amount: $234.50, current: $983.83 (4.2x). Typical channel: web, current: mobile. Time anomaly: 0.78',
      reasoning: 'Transaction significantly deviates from established behavioral baseline across multiple dimensions.'
    },
    {
      step: 7,
      tool: 'get_merchant_risk_profile',
      params: { merchant_id: 'MCH_CRYPTO_881' },
      output: 'High-risk category: Cryptocurrency Exchange. Fraud rate: 8.2% (vs platform avg 0.5%). Recent SAR filed.',
      reasoning: 'Merchant is flagged as high-risk with recent suspicious activity report - adds to overall risk assessment.'
    },
    {
      step: 8,
      tool: 'get_similar_cases',
      params: { pattern: 'MULE_CHAIN', limit: 5 },
      output: 'Found 5 similar cases in past 60 days. 4/5 confirmed as fraud. Avg loss prevented: $12,450.',
      reasoning: 'Historical similar cases strongly support the fraud hypothesis. Proceeding to synthesize findings.'
    }
  ],
  entities: [
    { type: 'Account', id: 'ACC_AML_152', label: 'Source Account' },
    { type: 'Device', id: 'DEV_A8X92', label: 'Mobile Device' },
    { type: 'Beneficiary', id: 'BEN_7721', label: 'Recipient' },
    { type: 'Merchant', id: 'MCH_CRYPTO_881', label: 'Crypto Exchange' },
    { type: 'IP', id: 'IP_192.168.1.xx', label: 'Connection IP' }
  ],
  evidence: [
    'Transaction amount 4.2x higher than account baseline',
    'Device shared with 3 confirmed fraud accounts',
    'FAN_OUT pattern detected with 94% confidence',
    'MULE_CHAIN pattern matches 4 recent fraud cases',
    'High-risk merchant with active SAR',
    'Velocity spike: 8 unique beneficiaries in 24h vs 1.2 average'
  ],
  nextSteps: [
    'Block transaction and freeze account for investigation',
    'File SAR within 24 hours',
    'Escalate device cluster for network analysis',
    'Review linked accounts (12 total) for similar patterns',
    'Update mule chain detection model with new pattern variant'
  ]
}

export const shapFeatures = [
  { feature: 'txn_type', importance: 2.26, category: 'metadata' },
  { feature: 'amount_sum_24h', importance: 1.15, category: 'velocity' },
  { feature: 'amount_sum_1h', importance: 0.92, category: 'velocity' },
  { feature: 'community_fraud_rate', importance: 0.85, category: 'graph' },
  { feature: 'is_new_beneficiary', importance: 0.78, category: 'behavioral' },
  { feature: 'channel', importance: 0.74, category: 'metadata' },
  { feature: 'unique_merchants_7d', importance: 0.52, category: 'velocity' },
  { feature: 'account_degree', importance: 0.51, category: 'graph' },
  { feature: 'unique_beneficiaries_7d', importance: 0.38, category: 'velocity' },
  { feature: 'is_new_device', importance: 0.28, category: 'behavioral' }
]

export const monthlyPrecision = [
  { month: 'Oct 2024', precision: 96.8 },
  { month: 'Nov 2024', precision: 96.0 },
  { month: 'Dec 2024', precision: 94.4 },
  { month: 'Jan 2025', precision: 96.2 }
]

export const monthlyFraudRate = [
  { month: 'Jan 2024', rate: 5.2 },
  { month: 'Feb 2024', rate: 5.5 },
  { month: 'Mar 2024', rate: 6.1 },
  { month: 'Apr 2024', rate: 6.8 },
  { month: 'May 2024', rate: 7.2 },
  { month: 'Jun 2024', rate: 7.9 },
  { month: 'Jul 2024', rate: 8.5 },
  { month: 'Aug 2024', rate: 9.2 },
  { month: 'Sep 2024', rate: 10.1 },
  { month: 'Oct 2024', rate: 11.2 },
  { month: 'Nov 2024', rate: 12.1 },
  { month: 'Dec 2024', rate: 13.0 }
]

export const psiHeatmap = [
  { feature: 'txn_risk_score', values: { oct: 0.08, nov: 0.15, dec: 0.22, jan: 0.28 } },
  { feature: 'behavior_anomaly_score', values: { oct: 0.12, nov: 0.18, dec: 0.25, jan: 0.31 } },
  { feature: 'final_risk_score', values: { oct: 0.09, nov: 0.16, dec: 0.24, jan: 0.29 } },
  { feature: 'amount', values: { oct: 0.11, nov: 0.19, dec: 0.27, jan: 0.33 } }
]

export const techStack = [
  'Python', 'PySpark', 'LightGBM', 'SHAP', 'NetworkX', 
  'LangGraph', 'FastMCP', 'FastAPI', 'Kafka', 'MLflow', 'Streamlit'
]

export const keyMetrics = [
  { label: 'Transactions Processed', value: '10.1M' },
  { label: 'Fraud Detected', value: '597K' },
  { label: 'Precision @Budget', value: '99.5%' },
  { label: 'PR-AUC', value: '0.994' },
  { label: 'Fraud Scenarios', value: '6' },
  { label: 'Investigation Tools', value: '11' }
]

export const actionDistribution = [
  { action: 'ALLOW', count: 9234521, color: '#22c55e' },
  { action: 'ALLOW_WITH_MONITORING', count: 523412, color: '#a855f7' },
  { action: 'STEP_UP_AUTH', count: 187234, color: '#3b82f6' },
  { action: 'QUEUE_FOR_REVIEW', count: 89421, color: '#f59e0b' },
  { action: 'BLOCK', count: 65412, color: '#ef4444' }
]

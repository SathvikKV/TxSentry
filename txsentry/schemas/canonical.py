"""Canonical data schemas for TxSentry.

All modules must conform to these schemas. Used for validation
throughout the ingestion, feature engineering, and scoring pipelines.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# --- Enums ---

class AccountType(str, Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    BUSINESS = "BUSINESS"
    PREPAID = "PREPAID"


class RiskTier(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class AccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DORMANT = "DORMANT"
    FROZEN = "FROZEN"
    CLOSED = "CLOSED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TABLET = "TABLET"


class TxnType(str, Enum):
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    CASH_IN = "CASH_IN"
    DEBIT = "DEBIT"


class Channel(str, Enum):
    MOBILE = "MOBILE"
    ONLINE = "ONLINE"
    BRANCH = "BRANCH"
    ATM = "ATM"


class Action(str, Enum):
    ALLOW = "ALLOW"
    ALLOW_WITH_MONITORING = "ALLOW_WITH_MONITORING"
    STEP_UP_AUTH = "STEP_UP_AUTH"
    QUEUE_FOR_REVIEW = "QUEUE_FOR_REVIEW"
    BLOCK = "BLOCK"


class Priority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class FraudScenario(str, Enum):
    MULE_CHAIN = "MULE_CHAIN"
    FAN_OUT = "FAN_OUT"
    FAN_IN = "FAN_IN"
    ACCOUNT_TAKEOVER_BURST = "ACCOUNT_TAKEOVER_BURST"
    SHARED_DEVICE_RING = "SHARED_DEVICE_RING"
    STRUCTURING = "STRUCTURING"
    PAYSIM_FRAUD = "PAYSIM_FRAUD"
    AMLSIM_AML = "AMLSIM_AML"


class ReasonCode(str, Enum):
    NEW_DEVICE_HIGH_VALUE_TXN = "NEW_DEVICE_HIGH_VALUE_TXN"
    SHARED_DEVICE_CLUSTER = "SHARED_DEVICE_CLUSTER"
    STRUCTURING_PATTERN = "STRUCTURING_PATTERN"
    HIGH_GRAPH_FANOUT = "HIGH_GRAPH_FANOUT"
    AMOUNT_4X_BASELINE = "AMOUNT_4X_BASELINE"
    DORMANT_ACCOUNT_REACTIVATION = "DORMANT_ACCOUNT_REACTIVATION"
    NEW_PAYEE_LARGE_TXN = "NEW_PAYEE_LARGE_TXN"
    BURST_VELOCITY = "BURST_VELOCITY"
    WATCHLIST_HIT = "WATCHLIST_HIT"
    MULE_CHAIN_DETECTED = "MULE_CHAIN_DETECTED"
    FAN_IN_PATTERN = "FAN_IN_PATTERN"
    FAN_OUT_PATTERN = "FAN_OUT_PATTERN"
    IP_COUNTRY_MISMATCH = "IP_COUNTRY_MISMATCH"
    HIGH_RISK_MERCHANT = "HIGH_RISK_MERCHANT"


# --- Entity Models ---

class Customer(BaseModel):
    customer_id: str
    name: str
    dob: datetime
    country: str
    created_at: datetime
    risk_tier: RiskTier


class Account(BaseModel):
    account_id: str
    customer_id: str
    account_type: AccountType
    balance: float
    status: AccountStatus
    created_at: datetime


class Device(BaseModel):
    device_id: str
    device_type: DeviceType
    os: str
    fingerprint_hash: str
    first_seen_at: datetime


class IPAddress(BaseModel):
    ip_id: str
    ip_addr: str
    country: str
    isp: str
    is_vpn: bool
    is_datacenter: bool


class Merchant(BaseModel):
    merchant_id: str
    name: str
    category_code: str
    country: str
    fraud_rate_hist: float


class Beneficiary(BaseModel):
    beneficiary_id: str
    name: str
    account_ref: str
    bank_code: str
    added_at: datetime


class WatchlistEntity(BaseModel):
    entity_id: str
    entity_type: str
    name: str
    reason: str
    listed_at: datetime
    source: str


# --- Event Models ---

class TransactionEvent(BaseModel):
    txn_id: str
    account_id: str
    merchant_id: Optional[str] = None
    beneficiary_id: Optional[str] = None
    device_id: Optional[str] = None
    ip_id: Optional[str] = None
    amount: float
    currency: str = "USD"
    txn_type: str
    channel: str
    timestamp: datetime
    is_fraud: bool = False
    fraud_scenario: Optional[str] = None
    source: Optional[str] = None


class LoginEvent(BaseModel):
    login_id: str
    account_id: str
    device_id: str
    ip_id: str
    timestamp: datetime
    success: bool
    mfa_used: bool


class PayeeAddEvent(BaseModel):
    event_id: str
    account_id: str
    beneficiary_id: str
    device_id: str
    timestamp: datetime


class AccountProfileChange(BaseModel):
    event_id: str
    account_id: str
    change_type: str
    old_value: str
    new_value: str
    device_id: str
    timestamp: datetime


class AlertEvent(BaseModel):
    alert_id: str
    txn_id: str
    account_id: str
    triggered_at: datetime
    txn_risk_score: float
    behavior_anomaly_score: float
    graph_risk_score: float
    final_risk_score: float
    risk_band: str
    action: Action
    reason_codes: list[str] = Field(default_factory=list)
    shap_top_features: dict = Field(default_factory=dict)


class CaseMemo(BaseModel):
    case_id: str
    alert_id: str
    recommended_action: Action
    confidence: float = Field(ge=0.0, le=1.0)
    priority: Priority
    reason_codes: list[str]
    entities_involved: dict
    summary: str
    supporting_evidence: list[str]
    tools_called: list[str]
    next_steps: list[str]


class InvestigationStep(BaseModel):
    step: int
    tool: str
    inputs: dict
    output_summary: str
    agent_reasoning: str


class CaseEvent(BaseModel):
    case_id: str
    alert_id: str
    opened_at: datetime
    closed_at: Optional[datetime] = None
    agent_action: Optional[Action] = None
    agent_confidence: Optional[float] = None
    reasoning_trace: list[InvestigationStep] = Field(default_factory=list)
    memo: Optional[CaseMemo] = None


# --- Relationship / Edge Models ---

class CustomerOwnsAccount(BaseModel):
    customer_id: str
    account_id: str


class AccountUsedByDevice(BaseModel):
    account_id: str
    device_id: str
    first_seen: datetime
    last_seen: datetime
    txn_count: int


class DeviceSeenOnIP(BaseModel):
    device_id: str
    ip_id: str
    first_seen: datetime
    last_seen: datetime


class AccountPaidBeneficiary(BaseModel):
    account_id: str
    beneficiary_id: str
    total_txns: int
    total_amount: float


class AccountToMerchant(BaseModel):
    account_id: str
    merchant_id: str
    total_txns: int
    total_amount: float


class EntityWatchlistHit(BaseModel):
    entity_id: str
    entity_type: str
    watchlist_entity_id: str
    matched_at: datetime
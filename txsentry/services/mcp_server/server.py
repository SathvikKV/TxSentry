"""Module 6: MCP Tool Server.

Exposes all 11 investigation tools as a local MCP server that the
LangGraph agent calls during autonomous investigation.

Usage:
    python -m txsentry.services.mcp_server.server

Tools:
    1. get_transaction_detail     - Full transaction record + risk scores
    2. get_account_history        - Transaction history summary
    3. get_velocity_features      - Current velocity signals
    4. get_graph_neighborhood     - Connected entities within N hops
    5. detect_graph_pattern       - Check for AML topology matches
    6. get_behavioral_baseline    - Account behavioral profile
    7. get_merchant_risk_profile  - Merchant category + fraud rate
    8. run_anomaly_score          - Isolation Forest anomaly score
    9. check_watchlist            - Watchlist screening
   10. get_similar_cases          - Nearest-neighbor case lookup
   11. write_case_memo            - Conclude investigation with structured output
"""

import logging

from fastmcp import FastMCP

from txsentry.services.mcp_server.tools.transaction_tools import (
    get_transaction_detail,
    get_account_history,
    get_velocity_features,
)
from txsentry.services.mcp_server.tools.graph_tools import (
    get_graph_neighborhood,
    detect_graph_pattern,
)
from txsentry.services.mcp_server.tools.account_tools import (
    get_behavioral_baseline,
    get_merchant_risk_profile,
    run_anomaly_score,
    check_watchlist,
)
from txsentry.services.mcp_server.tools.case_tools import (
    get_similar_cases,
    write_case_memo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP("TxSentry Investigation Tools")


# --- Tool 1 ---
@mcp.tool()
def tool_get_transaction_detail(txn_id: str) -> dict:
    """Get full transaction record including device, IP, merchant, risk scores, and SHAP top features."""
    return get_transaction_detail(txn_id)


# --- Tool 2 ---
@mcp.tool()
def tool_get_account_history(account_id: str, window_days: int = 30) -> dict:
    """Get transaction history summary for the account over the given window."""
    return get_account_history(account_id, window_days)


# --- Tool 3 ---
@mcp.tool()
def tool_get_velocity_features(account_id: str) -> dict:
    """Get current velocity signals: txn counts and amount sums over 1h/24h/7d windows."""
    return get_velocity_features(account_id)


# --- Tool 4 ---
@mcp.tool()
def tool_get_graph_neighborhood(entity_id: str, entity_type: str = "account", hops: int = 2) -> dict:
    """Return connected entities within N hops. entity_type: account|device|ip|beneficiary."""
    return get_graph_neighborhood(entity_id, entity_type, hops)


# --- Tool 5 ---
@mcp.tool()
def tool_detect_graph_pattern(account_id: str) -> dict:
    """Check if account's graph substructure matches known AML topologies.
    Returns: patterns detected, confidence, and supporting evidence."""
    return detect_graph_pattern(account_id)


# --- Tool 6 ---
@mcp.tool()
def tool_get_behavioral_baseline(account_id: str, window_days: int = 30) -> dict:
    """Account behavioral profile: avg/max/std of amounts, typical merchants, device usage."""
    return get_behavioral_baseline(account_id, window_days)


# --- Tool 7 ---
@mcp.tool()
def tool_get_merchant_risk_profile(merchant_id: str) -> dict:
    """Merchant category, historical fraud rate, dispute rate, risk tier."""
    return get_merchant_risk_profile(merchant_id)


# --- Tool 8 ---
@mcp.tool()
def tool_run_anomaly_score(account_id: str) -> dict:
    """Run Isolation Forest scoring on account's recent session features. Returns score 0-1."""
    return run_anomaly_score(account_id)


# --- Tool 9 ---
@mcp.tool()
def tool_check_watchlist(entity_id: str, entity_type: str = "INDIVIDUAL") -> dict:
    """Check entity against simulated watchlist. Returns hit (bool), reason, source."""
    return check_watchlist(entity_id, entity_type)


# --- Tool 10 ---
@mcp.tool()
def tool_get_similar_cases(txn_risk_score: float, reason_codes: list[str], top_k: int = 3) -> dict:
    """Nearest-neighbor lookup over past case feature vectors.
    Uses sklearn NearestNeighbors over [score + one-hot reason codes]."""
    return get_similar_cases(txn_risk_score, reason_codes, top_k)


# --- Tool 11 ---
@mcp.tool()
def tool_write_case_memo(
    case_id: str,
    alert_id: str,
    recommended_action: str,
    confidence: float,
    priority: str,
    reason_codes: list[str],
    entities_involved: dict,
    summary: str,
    supporting_evidence: list[str],
    tools_called: list[str],
    next_steps: list[str],
) -> dict:
    """Conclude investigation with structured case memo.
    recommended_action must be: ALLOW|ALLOW_WITH_MONITORING|STEP_UP_AUTH|QUEUE_FOR_REVIEW|BLOCK.
    priority must be: LOW|MEDIUM|HIGH."""
    return write_case_memo(
        case_id=case_id,
        alert_id=alert_id,
        recommended_action=recommended_action,
        confidence=confidence,
        priority=priority,
        reason_codes=reason_codes,
        entities_involved=entities_involved,
        summary=summary,
        supporting_evidence=supporting_evidence,
        tools_called=tools_called,
        next_steps=next_steps,
    )


def test_tools():
    """Quick smoke test — call each tool with sample data."""
    logger.info("Running MCP tool smoke tests...")

    # Get a sample transaction and account from alert data
    import pandas as pd
    alerts = pd.read_parquet("data/alerts/alert_events.parquet")

    # Pick a high-risk transaction
    high_risk = alerts[alerts["action"] == "BLOCK"].head(1)
    if high_risk.empty:
        high_risk = alerts.head(1)

    sample = high_risk.iloc[0]
    txn_id = sample["txn_id"]
    account_id = sample["account_id"]

    logger.info(f"\nSample txn: {txn_id}, account: {account_id}")

    # Test each tool
    tests = [
        ("get_transaction_detail", lambda: get_transaction_detail(txn_id)),
        ("get_account_history", lambda: get_account_history(account_id, 30)),
        ("get_velocity_features", lambda: get_velocity_features(account_id)),
        ("get_graph_neighborhood", lambda: get_graph_neighborhood(account_id, "account", 2)),
        ("detect_graph_pattern", lambda: detect_graph_pattern(account_id)),
        ("get_behavioral_baseline", lambda: get_behavioral_baseline(account_id)),
        ("run_anomaly_score", lambda: run_anomaly_score(account_id)),
        ("check_watchlist", lambda: check_watchlist(account_id)),
    ]

    for name, fn in tests:
        try:
            result = fn()
            status = "ERROR" if "error" in result else "OK"
            logger.info(f"  {name:35s} [{status}]")
            if status == "OK":
                # Print first few keys
                keys = list(result.keys())[:4]
                logger.info(f"    keys: {keys}")
        except Exception as e:
            logger.error(f"  {name:35s} [FAIL] {e}")

    logger.info("\nSmoke tests complete.")


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        test_tools()
    else:
        logger.info("Starting TxSentry MCP Server...")
        logger.info("11 tools registered")
        mcp.run()
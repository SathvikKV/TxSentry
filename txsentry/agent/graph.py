"""Module 7: LangGraph Investigation Agent.

Stateful 4-node graph: Triage → Planner → Investigator (loop) → Synthesizer.

Usage:
    python -m txsentry.agent.graph
"""

import json
import logging
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END

from txsentry.agent.state import AgentState
from txsentry.agent.nodes.all_nodes import (
    triage_node, planner_node, investigator_node, synthesizer_node,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

ALERTS_DIR = "data/alerts"
MAX_STEPS = 8


def build_graph():
    """Build the LangGraph investigation agent."""
    builder = StateGraph(AgentState)

    builder.add_node("triage", triage_node)
    builder.add_node("planner", planner_node)
    builder.add_node("investigator", investigator_node)
    builder.add_node("synthesizer", synthesizer_node)

    builder.set_entry_point("triage")
    builder.add_edge("triage", "planner")
    builder.add_edge("planner", "investigator")

    def should_continue(state: AgentState) -> str:
        if state.get("concluded", False):
            return "synthesizer"
        if state["step_count"] >= MAX_STEPS:
            return "synthesizer"
        if state["step_count"] >= len(state.get("investigation_plan", [])):
            return "synthesizer"
        return "investigator"

    builder.add_conditional_edges("investigator", should_continue, {
        "investigator": "investigator",
        "synthesizer": "synthesizer",
    })
    builder.add_edge("synthesizer", END)

    return builder.compile()


def investigate_alert(alert_data: dict) -> dict:
    """Run the full investigation pipeline on a single alert."""
    graph = build_graph()

    initial_state = {
        "alert_id": str(alert_data.get("alert_id", "")),
        "txn_id": str(alert_data.get("txn_id", "")),
        "account_id": str(alert_data.get("account_id", "")),
        "alert_data": alert_data,
        "messages": [],
        "investigation_plan": [],
        "tool_results": [],
        "reasoning_trace": [],
        "step_count": 0,
        "triage_depth": "DEEP",
        "concluded": False,
        "case_memo": None,
    }

    result = graph.invoke(initial_state)
    return result


def run():
    """Investigate sample high-risk alerts."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting LangGraph Investigation Agent")
    logger.info("=" * 60)

    # Load alerts
    alerts = pd.read_parquet(f"{ALERTS_DIR}/alert_events.parquet")

    # Pick a few high-risk alerts to investigate
    high_risk = alerts[alerts["action"] == "BLOCK"].head(3)
    if len(high_risk) == 0:
        high_risk = alerts.nlargest(3, "final_risk_score")

    logger.info(f"Selected {len(high_risk)} alerts for investigation\n")

    results = []
    for i, (_, alert) in enumerate(high_risk.iterrows()):
        logger.info(f"\n{'='*60}")
        logger.info(f"INVESTIGATION {i+1}/{len(high_risk)}")
        logger.info(f"Alert: {alert['alert_id']}, Account: {alert['account_id']}")
        logger.info(f"Risk Score: {alert['final_risk_score']:.4f}, Action: {alert['action']}")
        logger.info(f"{'='*60}")

        alert_dict = {
            "alert_id": str(alert.get("alert_id", "")),
            "txn_id": str(alert.get("txn_id", "")),
            "account_id": str(alert.get("account_id", "")),
            "amount": float(alert.get("amount", 0)),
            "timestamp": str(alert.get("timestamp", "")),
            "txn_risk_score": float(alert.get("txn_risk_score", 0)),
            "behavior_anomaly_score": float(alert.get("behavior_anomaly_score", 0)),
            "final_risk_score": float(alert.get("final_risk_score", 0)),
            "risk_band": str(alert.get("risk_band", "")),
            "action": str(alert.get("action", "")),
            "reason_codes": alert.get("reason_codes", []),
            "is_fraud": bool(alert.get("is_fraud", False)),
            "fraud_scenario": str(alert.get("fraud_scenario", "")),
        }

        try:
            result = investigate_alert(alert_dict)
            memo = result.get("case_memo", {})
            trace = result.get("reasoning_trace", [])

            logger.info(f"\n  CASE MEMO:")
            logger.info(f"    Action:     {memo.get('recommended_action', 'N/A')}")
            logger.info(f"    Confidence: {memo.get('confidence', 'N/A')}")
            logger.info(f"    Priority:   {memo.get('priority', 'N/A')}")
            logger.info(f"    Summary:    {memo.get('summary', 'N/A')}")
            logger.info(f"    Reasons:    {memo.get('reason_codes', [])}")
            logger.info(f"    Evidence:   {len(memo.get('supporting_evidence', []))} points")
            logger.info(f"    Steps:      {len(trace)} trace entries")
            logger.info(f"    Tools used: {memo.get('tools_called', [])}")

            results.append({"alert_id": alert_dict["alert_id"], "memo": memo, "trace": trace})

        except Exception as e:
            logger.error(f"  Investigation failed: {e}")
            import traceback
            traceback.print_exc()
            results.append({"alert_id": alert_dict["alert_id"], "error": str(e)})

    # Summary
    elapsed = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"INVESTIGATION SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Alerts investigated: {len(results)}")
    successful = [r for r in results if "memo" in r]
    logger.info(f"Successful: {len(successful)}")
    for r in successful:
        memo = r["memo"]
        logger.info(f"  {r['alert_id']}: {memo.get('recommended_action', 'N/A')} "
                     f"(confidence={memo.get('confidence', 'N/A')}, "
                     f"priority={memo.get('priority', 'N/A')})")

    logger.info(f"\nCompleted in {elapsed:.1f}s")

    # Save case memos are already written by write_case_memo tool
    cases_dir = Path("data/cases/memos")
    case_files = list(cases_dir.glob("*.json"))
    logger.info(f"Case memos saved: {len(case_files)} files in {cases_dir}")


if __name__ == "__main__":
    run()
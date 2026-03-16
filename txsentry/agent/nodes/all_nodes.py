"""All node implementations for the LangGraph investigation agent."""

import json
import logging

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

from txsentry.agent.state import AgentState
from txsentry.agent.prompts.all_prompts import (
    TRIAGE_PROMPT, PLANNER_PROMPT, INVESTIGATOR_PROMPT, SYNTHESIZER_PROMPT,
)
from txsentry.services.mcp_server.tools.transaction_tools import (
    get_transaction_detail, get_account_history, get_velocity_features,
)
from txsentry.services.mcp_server.tools.graph_tools import (
    get_graph_neighborhood, detect_graph_pattern,
)
from txsentry.services.mcp_server.tools.account_tools import (
    get_behavioral_baseline, get_merchant_risk_profile,
    run_anomaly_score, check_watchlist,
)
from txsentry.services.mcp_server.tools.case_tools import (
    get_similar_cases, write_case_memo,
)

logger = logging.getLogger(__name__)

# Tool dispatch table
TOOL_DISPATCH = {
    "get_transaction_detail": get_transaction_detail,
    "get_account_history": get_account_history,
    "get_velocity_features": get_velocity_features,
    "get_graph_neighborhood": get_graph_neighborhood,
    "detect_graph_pattern": detect_graph_pattern,
    "get_behavioral_baseline": get_behavioral_baseline,
    "get_merchant_risk_profile": get_merchant_risk_profile,
    "run_anomaly_score": run_anomaly_score,
    "check_watchlist": check_watchlist,
    "get_similar_cases": get_similar_cases,
}


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(model="gpt-4o-mini", temperature=0, max_tokens=800)


def _parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown fences."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    return json.loads(text)


# --- TRIAGE NODE ---

def triage_node(state: AgentState) -> dict:
    """Assess alert severity and determine investigation depth."""
    logger.info("  [TRIAGE] Assessing alert severity...")

    llm = _get_llm()
    prompt = TRIAGE_PROMPT.format(alert_data=json.dumps(state["alert_data"], indent=2, default=str))

    response = llm.invoke([HumanMessage(content=prompt)])
    result = _parse_json_response(response.content)

    triage_depth = result.get("triage_depth", "DEEP")
    assessment = result.get("initial_assessment", "")
    signals = result.get("key_risk_signals", [])

    logger.info(f"    Depth: {triage_depth}")
    logger.info(f"    Assessment: {assessment}")

    return {
        "triage_depth": triage_depth,
        "messages": [
            HumanMessage(content=prompt),
            AIMessage(content=response.content),
        ],
        "reasoning_trace": [{
            "step": 0,
            "tool": "triage",
            "inputs": {"alert_id": state["alert_id"]},
            "output_summary": f"Depth: {triage_depth}. {assessment}",
            "agent_reasoning": f"Key signals: {', '.join(signals)}",
        }],
    }


# --- PLANNER NODE ---

def planner_node(state: AgentState) -> dict:
    """Generate an ordered list of tools to call."""
    logger.info("  [PLANNER] Creating investigation plan...")

    llm = _get_llm()

    # Build triage summary from last trace entry
    triage_trace = state["reasoning_trace"][-1] if state["reasoning_trace"] else {}
    triage_assessment = triage_trace.get("output_summary", "No triage performed")

    prompt = PLANNER_PROMPT.format(
        triage_assessment=triage_assessment,
        alert_data=json.dumps(state["alert_data"], indent=2, default=str),
        triage_depth=state.get("triage_depth", "DEEP"),
    )

    response = llm.invoke([HumanMessage(content=prompt)])
    logger.info(f"    Raw LLM plan response: {response.content[:300]}")
    result = _parse_json_response(response.content)

    plan = result.get("investigation_plan", [])
    rationale = result.get("rationale", "")
    logger.info(f"    Parsed plan before validation: {plan}")

    # Validate and normalize tool names
    # LLM often returns "get_transaction_detail(txn_id)" — strip the args
    valid_tools = set(TOOL_DISPATCH.keys())
    normalized_plan = []
    for t in plan:
        # Strip parenthesized arguments: "get_transaction_detail(txn_id)" -> "get_transaction_detail"
        clean = t.split("(")[0].strip()
        if clean in valid_tools:
            normalized_plan.append(clean)
        elif clean.startswith("tool_") and clean[5:] in valid_tools:
            normalized_plan.append(clean[5:])
    plan = normalized_plan

    # Enforce depth limits
    depth = state.get("triage_depth", "DEEP")
    if depth == "SHALLOW":
        plan = plan[:3]
    else:
        plan = plan[:8]

    logger.info(f"    Plan ({len(plan)} tools): {plan}")
    logger.info(f"    Rationale: {rationale}")

    return {
        "investigation_plan": plan,
        "messages": [
            HumanMessage(content=prompt),
            AIMessage(content=response.content),
        ],
        "reasoning_trace": state["reasoning_trace"] + [{
            "step": 1,
            "tool": "planner",
            "inputs": {"depth": depth},
            "output_summary": f"Plan: {plan}",
            "agent_reasoning": rationale,
        }],
    }


# --- INVESTIGATOR NODE ---

def investigator_node(state: AgentState) -> dict:
    """Execute the next tool in the investigation plan."""
    step = state["step_count"]
    plan = state["investigation_plan"]

    if step >= len(plan):
        logger.info("  [INVESTIGATOR] Plan complete, moving to synthesis")
        return {"concluded": True}

    tool_name = plan[step]
    logger.info(f"  [INVESTIGATOR] Step {step + 1}/{len(plan)}: {tool_name}")

    llm = _get_llm()

    # Build previous findings summary
    prev_findings = []
    for tr in state["tool_results"]:
        prev_findings.append(f"Tool: {tr.get('tool', 'unknown')} → {_summarize(tr.get('result', {}))}")

    prompt = INVESTIGATOR_PROMPT.format(
        step_count=step + 1,
        tool_name=tool_name,
        alert_data=json.dumps(state["alert_data"], indent=2, default=str),
        previous_findings="\n".join(prev_findings) if prev_findings else "None yet",
    )

    response = llm.invoke([HumanMessage(content=prompt)])

    try:
        parsed = _parse_json_response(response.content)
        params = parsed.get("parameters", {})
        reasoning = parsed.get("reasoning", "")
    except (json.JSONDecodeError, KeyError):
        # Fallback: use default parameters from alert data
        params = _default_params(tool_name, state)
        reasoning = "Used default parameters (LLM response parsing failed)"

    # Execute the tool
    tool_fn = TOOL_DISPATCH.get(tool_name)
    if tool_fn is None:
        result = {"error": f"Unknown tool: {tool_name}"}
    else:
        try:
            result = tool_fn(**params)
        except Exception as e:
            logger.warning(f"    Tool error: {e}. Retrying with defaults.")
            params = _default_params(tool_name, state)
            try:
                result = tool_fn(**params)
            except Exception as e2:
                result = {"error": str(e2)}

    output_summary = _summarize(result)
    logger.info(f"    Result: {output_summary[:100]}")

    new_tool_result = {
        "tool": tool_name,
        "parameters": params,
        "result": result,
    }

    new_trace = {
        "step": step + 2,  # +2 because triage=0, planner=1
        "tool": tool_name,
        "inputs": params,
        "output_summary": output_summary,
        "agent_reasoning": reasoning,
    }

    return {
        "step_count": step + 1,
        "tool_results": state["tool_results"] + [new_tool_result],
        "reasoning_trace": state["reasoning_trace"] + [new_trace],
        "messages": [
            HumanMessage(content=prompt),
            AIMessage(content=response.content),
        ],
    }


# --- SYNTHESIZER NODE ---

def synthesizer_node(state: AgentState) -> dict:
    """Synthesize all findings into a final case memo."""
    logger.info("  [SYNTHESIZER] Writing case memo...")

    llm = _get_llm()

    # Build trace summary for the prompt
    trace_summary = []
    for t in state["reasoning_trace"]:
        trace_summary.append(
            f"Step {t['step']}: [{t['tool']}] {t['output_summary']}"
        )

    # Build tool results summary
    results_summary = []
    for tr in state["tool_results"]:
        results_summary.append(
            f"{tr['tool']}: {_summarize(tr['result'])}"
        )

    prompt = SYNTHESIZER_PROMPT.format(
        alert_data=json.dumps(state["alert_data"], indent=2, default=str),
        investigation_trace="\n".join(trace_summary),
        tool_results="\n".join(results_summary),
    )

    response = llm.invoke([HumanMessage(content=prompt)])
    result = _parse_json_response(response.content)

    # Build case memo
    case_id = f"CASE_{state['alert_id'].replace('ALT_', '')}"
    tools_called = [tr["tool"] for tr in state["tool_results"]]

    memo_result = write_case_memo(
        case_id=case_id,
        alert_id=state["alert_id"],
        recommended_action=result.get("recommended_action", "QUEUE_FOR_REVIEW"),
        confidence=result.get("confidence", 0.5),
        priority=result.get("priority", "MEDIUM"),
        reason_codes=result.get("reason_codes", []),
        entities_involved=result.get("entities_involved", {}),
        summary=result.get("summary", "Investigation complete."),
        supporting_evidence=result.get("supporting_evidence", []),
        tools_called=tools_called,
        next_steps=result.get("next_steps", []),
    )

    logger.info(f"    Action: {result.get('recommended_action')}")
    logger.info(f"    Confidence: {result.get('confidence')}")
    logger.info(f"    Priority: {result.get('priority')}")
    logger.info(f"    Case saved: {memo_result.get('saved_to')}")

    return {
        "concluded": True,
        "case_memo": {
            "case_id": case_id,
            **result,
            "tools_called": tools_called,
        },
        "messages": [
            HumanMessage(content=prompt),
            AIMessage(content=response.content),
        ],
    }


# --- Helpers ---

def _summarize(result: dict, max_len: int = 200) -> str:
    """Create a brief summary of a tool result."""
    if "error" in result:
        return f"Error: {result['error']}"
    # Pick key fields to summarize
    summary_parts = []
    for key in ["account_id", "txn_id", "amount", "action", "risk_band",
                 "patterns_detected", "confidence", "interpretation",
                 "total_transactions", "hit", "reason", "similar_cases"]:
        if key in result:
            val = result[key]
            if isinstance(val, list) and len(val) > 3:
                val = val[:3]
            summary_parts.append(f"{key}={val}")
    s = ", ".join(summary_parts)
    return s[:max_len] if s else str(result)[:max_len]


def _default_params(tool_name: str, state: AgentState) -> dict:
    """Generate default parameters for a tool based on alert data."""
    alert = state["alert_data"]
    txn_id = state["txn_id"]
    account_id = state["account_id"]

    defaults = {
        "get_transaction_detail": {"txn_id": txn_id},
        "get_account_history": {"account_id": account_id, "window_days": 30},
        "get_velocity_features": {"account_id": account_id},
        "get_graph_neighborhood": {"entity_id": account_id, "entity_type": "account", "hops": 2},
        "detect_graph_pattern": {"account_id": account_id},
        "get_behavioral_baseline": {"account_id": account_id, "window_days": 30},
        "get_merchant_risk_profile": {"merchant_id": alert.get("merchant_id", "MERCH_0")},
        "run_anomaly_score": {"account_id": account_id},
        "check_watchlist": {"entity_id": account_id, "entity_type": "INDIVIDUAL"},
        "get_similar_cases": {
            "txn_risk_score": float(alert.get("txn_risk_score", 0.5)),
            "reason_codes": alert.get("reason_codes", []),
            "top_k": 3,
        },
    }
    return defaults.get(tool_name, {})
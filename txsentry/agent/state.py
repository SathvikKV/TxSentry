"""Agent state definition for the LangGraph investigation agent."""

from typing import TypedDict, Annotated
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    alert_id: str
    txn_id: str
    account_id: str
    alert_data: dict                          # Full alert_event record
    messages: Annotated[list, add_messages]    # LLM conversation history
    investigation_plan: list[str]             # Ordered list of tools to call
    tool_results: list[dict]                  # Accumulated tool outputs
    reasoning_trace: list[dict]               # Step-by-step trace log
    step_count: int
    triage_depth: str                         # SHALLOW or DEEP
    concluded: bool
    case_memo: dict | None
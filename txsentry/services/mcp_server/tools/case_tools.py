"""Case management and similar case lookup tools."""

import json
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors

ALERTS_DIR = "data/alerts"
CASES_DIR = "data/cases/memos"

_cache = {}

# Reason code universe for one-hot encoding
REASON_CODES = [
    "NEW_DEVICE_HIGH_VALUE_TXN", "SHARED_DEVICE_CLUSTER", "STRUCTURING_PATTERN",
    "HIGH_GRAPH_FANOUT", "AMOUNT_4X_BASELINE", "DORMANT_ACCOUNT_REACTIVATION",
    "NEW_PAYEE_LARGE_TXN", "BURST_VELOCITY", "WATCHLIST_HIT",
    "MULE_CHAIN_DETECTED", "FAN_IN_PATTERN", "FAN_OUT_PATTERN",
    "IP_COUNTRY_MISMATCH", "HIGH_RISK_MERCHANT",
]


def get_similar_cases(txn_risk_score: float, reason_codes: list[str], top_k: int = 3) -> dict:
    """Nearest-neighbor lookup over past case feature vectors.

    Uses sklearn NearestNeighbors over [score + one-hot reason codes].
    This is NOT RAG — it is vector similarity over structured ML features.
    """
    cases_dir = Path(CASES_DIR)
    if not cases_dir.exists():
        cases_dir.mkdir(parents=True, exist_ok=True)

    # Load existing cases
    case_files = list(cases_dir.glob("*.json"))
    if not case_files:
        return {
            "similar_cases": [],
            "message": "No prior cases exist yet for comparison.",
        }

    cases = []
    case_vectors = []
    for f in case_files:
        case = json.loads(f.read_text())
        cases.append(case)
        vec = _encode_case_vector(
            case.get("confidence", 0.5),
            case.get("reason_codes", []),
        )
        case_vectors.append(vec)

    # Encode query
    query_vec = _encode_case_vector(txn_risk_score, reason_codes)

    # Fit NearestNeighbors
    X = np.array(case_vectors)
    k = min(top_k, len(cases))
    nn = NearestNeighbors(n_neighbors=k, metric="cosine")
    nn.fit(X)

    distances, indices = nn.kneighbors([query_vec])

    similar = []
    for dist, idx in zip(distances[0], indices[0]):
        c = cases[idx]
        similar.append({
            "case_id": c.get("case_id", ""),
            "recommended_action": c.get("recommended_action", ""),
            "confidence": c.get("confidence", 0),
            "reason_codes": c.get("reason_codes", []),
            "summary": c.get("summary", ""),
            "similarity_score": round(1 - float(dist), 4),
        })

    return {"similar_cases": similar}


def _encode_case_vector(score: float, reason_codes: list[str]) -> list[float]:
    """Encode a case as [score] + one-hot reason codes."""
    vec = [score]
    for code in REASON_CODES:
        vec.append(1.0 if code in reason_codes else 0.0)
    return vec


def write_case_memo(
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
    """Forced-schema tool. Agent must call this to conclude investigation.

    Validates recommended_action against allowed taxonomy. Saves to case_event table.
    """
    VALID_ACTIONS = {"ALLOW", "ALLOW_WITH_MONITORING", "STEP_UP_AUTH", "QUEUE_FOR_REVIEW", "BLOCK"}
    VALID_PRIORITIES = {"LOW", "MEDIUM", "HIGH"}

    if recommended_action not in VALID_ACTIONS:
        return {"error": f"Invalid action: {recommended_action}. Must be one of {VALID_ACTIONS}"}
    if priority not in VALID_PRIORITIES:
        return {"error": f"Invalid priority: {priority}. Must be one of {VALID_PRIORITIES}"}

    memo = {
        "case_id": case_id,
        "alert_id": alert_id,
        "recommended_action": recommended_action,
        "confidence": round(confidence, 2),
        "priority": priority,
        "reason_codes": reason_codes,
        "entities_involved": entities_involved,
        "summary": summary,
        "supporting_evidence": supporting_evidence,
        "tools_called": tools_called,
        "next_steps": next_steps,
        "created_at": datetime.utcnow().isoformat(),
    }

    # Save to file
    cases_dir = Path(CASES_DIR)
    cases_dir.mkdir(parents=True, exist_ok=True)
    memo_path = cases_dir / f"{case_id}.json"
    memo_path.write_text(json.dumps(memo, indent=2))

    return {
        "status": "success",
        "case_id": case_id,
        "saved_to": str(memo_path),
    }
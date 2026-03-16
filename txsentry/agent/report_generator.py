"""Module 11: Investigation Report Generator.

Renders CaseMemo + reasoning trace into a self-contained HTML report.

Usage:
    python -m txsentry.agent.report_generator
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CASES_DIR = "data/cases/memos"
REPORTS_DIR = "data/cases/reports"

ACTION_COLORS = {
    "BLOCK": "#dc2626",
    "QUEUE_FOR_REVIEW": "#d97706",
    "STEP_UP_AUTH": "#2563eb",
    "ALLOW_WITH_MONITORING": "#7c3aed",
    "ALLOW": "#16a34a",
}

PRIORITY_COLORS = {
    "HIGH": "#dc2626",
    "MEDIUM": "#d97706",
    "LOW": "#16a34a",
}

REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TxSentry Investigation Report — {case_id}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ background: #0f172a; color: #e2e8f0; font-family: system-ui, -apple-system, sans-serif; padding: 2rem; line-height: 1.6; }}
.container {{ max-width: 900px; margin: 0 auto; }}
.header {{ border-bottom: 2px solid #334155; padding-bottom: 1.5rem; margin-bottom: 2rem; }}
.header h1 {{ color: #f8fafc; font-size: 1.5rem; margin-bottom: 0.5rem; }}
.header .meta {{ color: #94a3b8; font-size: 0.875rem; }}
.badge {{ display: inline-block; padding: 0.25rem 0.75rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: white; }}
.card {{ background: #1e293b; border-radius: 0.75rem; padding: 1.5rem; margin-bottom: 1.5rem; }}
.card h2 {{ color: #f8fafc; font-size: 1.125rem; margin-bottom: 1rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }}
.score-bar {{ background: #334155; border-radius: 0.5rem; height: 1.5rem; margin: 0.5rem 0; position: relative; overflow: hidden; }}
.score-fill {{ height: 100%%; border-radius: 0.5rem; transition: width 0.3s; display: flex; align-items: center; padding-left: 0.5rem; font-size: 0.75rem; font-weight: 600; color: white; }}
.tag {{ display: inline-block; background: #334155; border-left: 3px solid #3b82f6; padding: 0.25rem 0.75rem; margin: 0.25rem; border-radius: 0 0.25rem 0.25rem 0; font-size: 0.8rem; font-family: 'Courier New', monospace; }}
.entity-card {{ display: inline-block; background: #334155; padding: 0.5rem 1rem; margin: 0.25rem; border-radius: 0.5rem; font-family: 'Courier New', monospace; font-size: 0.8rem; }}
.trace-step {{ background: #1e293b; border-left: 3px solid #3b82f6; padding: 1rem; margin: 0.75rem 0; border-radius: 0 0.5rem 0.5rem 0; }}
.trace-step .step-num {{ color: #3b82f6; font-weight: 700; font-size: 0.875rem; }}
.trace-step .tool-name {{ color: #f59e0b; font-family: 'Courier New', monospace; }}
.trace-step .reasoning {{ color: #94a3b8; font-size: 0.875rem; margin-top: 0.5rem; }}
.evidence {{ color: #cbd5e1; padding-left: 1rem; border-left: 2px solid #475569; margin: 0.5rem 0; }}
.next-step {{ background: #1e293b; padding: 0.5rem 1rem; margin: 0.25rem 0; border-radius: 0.5rem; }}
.next-step::before {{ content: "→ "; color: #3b82f6; font-weight: 700; }}
.footer {{ text-align: center; color: #475569; font-size: 0.75rem; margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #1e293b; }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🛡️ TxSentry Investigation Report</h1>
        <div class="meta">
            Case: <strong>{case_id}</strong> &nbsp;|&nbsp;
            Alert: <strong>{alert_id}</strong> &nbsp;|&nbsp;
            Generated: {generated_at}
        </div>
    </div>

    <!-- Executive Summary -->
    <div class="card">
        <h2>Executive Summary</h2>
        <div style="display: flex; gap: 2rem; align-items: center; margin-bottom: 1rem;">
            <div>
                <span class="badge" style="background: {action_color}; font-size: 1.25rem; padding: 0.5rem 1.5rem;">
                    {recommended_action}
                </span>
            </div>
            <div>
                <span class="badge" style="background: {priority_color};">Priority: {priority}</span>
            </div>
            <div style="color: #94a3b8;">
                Confidence: <strong style="color: #f8fafc;">{confidence:.1%}</strong>
            </div>
        </div>
        <p style="font-size: 1.05rem;">{summary}</p>
    </div>

    <!-- Reason Codes -->
    <div class="card">
        <h2>Reason Codes</h2>
        {reason_code_tags}
    </div>

    <!-- Entities Involved -->
    <div class="card">
        <h2>Entities Involved</h2>
        {entity_cards}
    </div>

    <!-- Supporting Evidence -->
    <div class="card">
        <h2>Supporting Evidence</h2>
        {evidence_items}
    </div>

    <!-- Investigation Trace -->
    <div class="card">
        <h2>Investigation Trace</h2>
        <p style="color: #94a3b8; margin-bottom: 1rem;">Tools called: {tools_called_str}</p>
        {trace_html}
    </div>

    <!-- Next Steps -->
    <div class="card">
        <h2>Recommended Next Steps</h2>
        {next_steps_html}
    </div>

    <div class="footer">
        TxSentry Fraud Investigation Platform &nbsp;|&nbsp; Report auto-generated from case memo
    </div>
</div>
</body>
</html>"""


def _score_color(score: float) -> str:
    if score >= 0.7:
        return "#dc2626"
    if score >= 0.4:
        return "#d97706"
    return "#16a34a"


def generate_report(case_memo: dict, reasoning_trace: list[dict] = None, output_path: str = None) -> str:
    """Render CaseMemo + trace into a self-contained HTML report."""

    case_id = case_memo.get("case_id", "UNKNOWN")
    action = case_memo.get("recommended_action", "N/A")

    # Reason code tags
    reason_codes = case_memo.get("reason_codes", [])
    reason_code_tags = "\n".join([f'<span class="tag">{code}</span>' for code in reason_codes])
    if not reason_codes:
        reason_code_tags = '<span style="color: #94a3b8;">No reason codes</span>'

    # Entity cards
    entities = case_memo.get("entities_involved", {})
    entity_html_parts = []
    for etype, eids in entities.items():
        if isinstance(eids, list) and eids:
            for eid in eids[:5]:
                entity_html_parts.append(f'<span class="entity-card">{etype[:-1]}: {eid}</span>')
    entity_cards = "\n".join(entity_html_parts) if entity_html_parts else '<span style="color: #94a3b8;">No entities recorded</span>'

    # Evidence
    evidence = case_memo.get("supporting_evidence", [])
    evidence_items = "\n".join([f'<div class="evidence">{ev}</div>' for ev in evidence])
    if not evidence:
        evidence_items = '<span style="color: #94a3b8;">No evidence recorded</span>'

    # Trace
    trace = reasoning_trace or []
    trace_parts = []
    for t in trace:
        trace_parts.append(f"""
        <div class="trace-step">
            <span class="step-num">Step {t.get('step', '?')}</span> —
            <span class="tool-name">{t.get('tool', 'unknown')}</span>
            <div style="margin-top: 0.25rem;">{t.get('output_summary', '')}</div>
            <div class="reasoning">{t.get('agent_reasoning', '')}</div>
        </div>""")
    trace_html = "\n".join(trace_parts) if trace_parts else '<span style="color: #94a3b8;">No trace available</span>'

    # Next steps
    next_steps = case_memo.get("next_steps", [])
    next_steps_html = "\n".join([f'<div class="next-step">{step}</div>' for step in next_steps])
    if not next_steps:
        next_steps_html = '<span style="color: #94a3b8;">No next steps recorded</span>'

    # Tools called string
    tools = case_memo.get("tools_called", [])
    tools_called_str = " → ".join(tools) if tools else "None"

    html = REPORT_TEMPLATE.format(
        case_id=case_id,
        alert_id=case_memo.get("alert_id", "N/A"),
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        action_color=ACTION_COLORS.get(action, "#6b7280"),
        priority_color=PRIORITY_COLORS.get(case_memo.get("priority", "MEDIUM"), "#d97706"),
        recommended_action=action,
        priority=case_memo.get("priority", "N/A"),
        confidence=case_memo.get("confidence", 0),
        summary=case_memo.get("summary", "No summary available."),
        reason_code_tags=reason_code_tags,
        entity_cards=entity_cards,
        evidence_items=evidence_items,
        trace_html=trace_html,
        next_steps_html=next_steps_html,
        tools_called_str=tools_called_str,
    )

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(html, encoding="utf-8")
        logger.info(f"Report written to {output_path}")

    return html


def run():
    """Generate reports for all existing case memos."""
    logger.info("=" * 60)
    logger.info("Generating Investigation Reports")
    logger.info("=" * 60)

    cases_dir = Path(CASES_DIR)
    reports_dir = Path(REPORTS_DIR)
    reports_dir.mkdir(parents=True, exist_ok=True)

    case_files = sorted(cases_dir.glob("*.json"))
    if not case_files:
        logger.warning("No case memos found.")
        return

    for case_file in case_files:
        case = json.loads(case_file.read_text())
        case_id = case.get("case_id", case_file.stem)
        output = reports_dir / f"{case_id}.html"
        generate_report(case, output_path=str(output))

    logger.info(f"\nGenerated {len(case_files)} reports in {reports_dir}")


if __name__ == "__main__":
    run()
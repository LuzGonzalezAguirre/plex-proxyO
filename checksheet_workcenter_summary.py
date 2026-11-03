"""Conservative department/WC summary from verified historical sequence evidence.

No current Plex card status or alert is inferred from timestamps alone.
A department is classified only using explicit Responsibility mappings.
"""


def classify_responsibility(raw):
    value = " ".join(str(raw or "").strip().upper().split())
    if value in {"CELL OPERATOR", "PRODUCTION", "PRODUCTION OPERATOR"}:
        return "PRODUCTION"
    if value in {"QUALITY", "QUALITY CONTROL", "QC", "QUALITY INSPECTOR"}:
        return "QUALITY"
    return "UNASSIGNED"


def summarize_workcenter_group(section_ids, config_by_section, sequence, truncated=False):
    """Summarize one explicitly ordered group of sections for one WC.

    The endpoint assesses only selected sections; a Workcenter may have other
    required checksheets, so full WC compliance is NEVER asserted.
    """
    responsibilities = [
        classify_responsibility(config_by_section[section]["responsibility"])
        for section in section_ids
    ]
    unique = sorted(set(responsibilities))
    department = unique[0] if len(unique) == 1 else "MIXED"
    reasons = []
    if department in {"MIXED", "UNASSIGNED"}:
        reasons.append("RESPONSIBILITY_NOT_UNAMBIGUOUS")
    if truncated:
        reasons.append("EVIDENCE_TRUNCATED")
    if not sequence["attempts"]:
        history_state = "NO_CAPTURE_OBSERVED"
    elif sequence["last_attempt_complete"]:
        history_state = "LATEST_OBSERVED_SEQUENCE_COMPLETE"
    else:
        history_state = "LATEST_OBSERVED_SEQUENCE_PARTIAL"
    reasons.append("CURRENT_SHIFT_JOB_AND_PLEX_TIMER_NOT_VERIFIED")
    reasons.append("OTHER_WC_CHECKSHEETS_NOT_EVALUATED")
    return {
        "workcenter_status": "UNVERIFIED",
        "department": department,
        "department_status": "UNVERIFIED",
        "historical_sequence_state": history_state,
        "last_historical_completion_at": sequence["last_completed_at"],
        "next_expected_section_from_history": sequence["next_expected_section"],
        "selected_sections": list(section_ids),
        "complete_historical_cycles": sequence["complete_cycles"],
        "evidence_truncated": bool(truncated),
        "blocking_reasons": reasons,
        "alert_eligible": False,
        "teams_notifications_enabled": False,
    }

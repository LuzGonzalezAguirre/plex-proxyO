"""Experimental checksheet timers. NOT authoritative Plex card colors.

Only caller-supplied verified anchors are evaluated. Required_Time and
Delay_Time semantics have not been confirmed against the Plex UI.
"""
from datetime import datetime, timedelta

def evaluate_timer(requirement, inspections, anchor, at_time, evidence_complete):
    interval = float(requirement.get("Required_Interval_Minutes") or 0)
    delay = float(requirement.get("Delay_Minutes") or 0)
    per_shift = bool(requirement.get("Required_Per_Shift"))
    if interval == -1 and not per_shift:
        return {"state": "NOT_REQUIRED", "hypothesis": False, "reason": "No checksheet required."}
    base = {
        "state": "UNVERIFIED",
        "hypothesis": True,
        "anchor_at": anchor.isoformat() if anchor else None,
        "evaluated_at": at_time.isoformat() if at_time else None,
        "interval_minutes": interval,
        "delay_minutes": delay,
        "rule": "EXPERIMENTAL_ANCHOR_PLUS_INTERVAL_AND_DELAY",
    }
    if not evidence_complete:
        return {**base, "reason": "Checksheet evidence is incomplete or truncated."}
    if anchor is None or at_time is None:
        return {**base, "reason": "Validated timer anchor and evaluation time are required."}
    if anchor > at_time:
        return {**base, "reason": "Timer anchor is after evaluation time."}
    if interval <= 0:
        return {**base, "reason": "Periodic time is absent; shift/change triggers are not yet modeled."}
    if per_shift:
        return {**base, "reason": "Combined per-shift plus interval semantics are not validated."}

    # Hypothesis: each inspection resets the interval and Delay_Time is grace.
    good = sorted(t for t in inspections if t is not None and anchor <= t <= at_time)
    latest = good[-1] if good else None
    start = latest if latest else anchor
    due_at = start + timedelta(minutes=interval)
    grace_end = due_at + timedelta(minutes=max(delay, 0))
    state = "PREDICTED_PENDING" if at_time >= grace_end else "PREDICTED_WITHIN_WINDOW"
    return {
        **base,
        "state": state,
        "reason": "Hypothesis only; compare against actual Plex orange/green card.",
        "last_inspection_at": latest.isoformat() if latest else None,
        "due_at": due_at.isoformat(),
        "grace_ends_at": grace_end.isoformat(),
        "minutes_to_due": round((due_at - at_time).total_seconds() / 60, 2),
    }

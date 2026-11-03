"""Pure, read-only reconstruction of ordered Plex checksheet capture sequences.

This is historical evidence, not Plex's authoritative card status. Callers
must supply records scoped to the same Workcenter, Part and Control Plan.
Job_Op_Key is retained as provenance; it is NOT a sequence partition because
Plex may carry a valid inspection across a Job change.
"""
from datetime import datetime


def reconstruct_sequence(section_ids, records):
    """Reconstruct ordered attempts from (section_key, inspection_date, job_op_key).

    A new capture of the first section begins a new attempt. Captures of later
    sections count only in order. Duplicate/early captures are retained as
    anomalies, never used to infer completion.
    """
    ids = [int(s) for s in section_ids]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("section_ids must be nonempty and unique")
    selected = set(ids)
    ordered = sorted(
        (r for r in records
         if int(r["section_key"]) in selected and r["inspection_date"] is not None),
        key=lambda r: (r["inspection_date"], int(r["section_key"])),
    )
    attempts = []
    current = None
    anomalies = []
    for record in ordered:
        section = int(record["section_key"])
        stamp = record["inspection_date"]
        if not isinstance(stamp, datetime):
            raise ValueError("inspection_date must be datetime")
        if section == ids[0]:
            if current is not None and not current["complete"]:
                anomalies.append("RESTART_BEFORE_COMPLETION")
            current = {
                "started_at": stamp.isoformat(),
                "completed_at": None,
                "complete": False,
                "captured_sections": [section],
                "capture_job_op_keys": sorted({
                    int(record["job_op_key"])
                }) if record.get("job_op_key") is not None else [],
            }
            attempts.append(current)
        elif current is None or current["complete"] or ids[len(current["captured_sections"])] != section:
            anomalies.append("OUT_OF_ORDER_OR_UNMATCHED_CAPTURE")
            continue
        else:
            current["captured_sections"].append(section)
            if record.get("job_op_key") is not None:
                key = int(record["job_op_key"])
                if key not in current["capture_job_op_keys"]:
                    current["capture_job_op_keys"].append(key)
                    current["capture_job_op_keys"].sort()
        if len(current["captured_sections"]) == len(ids):
            current["complete"] = True
            current["completed_at"] = stamp.isoformat()

    last_complete = next((a for a in reversed(attempts) if a["complete"]), None)
    return {
        "section_order": ids,
        "attempts": attempts,
        "complete_cycles": sum(1 for a in attempts if a["complete"]),
        "last_completed_at": last_complete["completed_at"] if last_complete else None,
        "last_attempt_complete": attempts[-1]["complete"] if attempts else None,
        "next_expected_section": (
            None if not attempts or attempts[-1]["complete"]
            else ids[len(attempts[-1]["captured_sections"])]
        ),
        "anomalies": anomalies,
        "plex_status_verified": False,
        "notifications_enabled": False,
    }

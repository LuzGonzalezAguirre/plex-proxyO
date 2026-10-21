"""Read-only EDI event assessment for revenue releases.

A historical 'out-of-date' event is evidence of past obsolescence, not proof
that a release cannot subsequently be reinstated. Adjustment is indicative.
"""
import re
from collections import defaultdict

OUTDATED = re.compile(r"new edi data caused this release to become out-of-date", re.I)
EDI_KEY = re.compile(r"EDI Key:\s*(\d+)", re.I)

def attach_edi_history(cursor, query_to_list, rows):
    history = defaultdict(list)
    keys = sorted({int(row["Release_Key"]) for row in rows})
    for start in range(0, len(keys), 70):
        batch = ",".join(map(str, keys[start:start + 70]))
        cursor.execute(f"""
            SELECT Release_Key, Change_Key, Change_Date, Note
            FROM Sales_v_Release_Change
            WHERE Release_Key IN ({batch})
            ORDER BY Release_Key, Change_Date, Change_Key
        """)
        for event in query_to_list(cursor):
            history[int(event["Release_Key"])].append(event)

    for row in rows:
        events = history.get(int(row["Release_Key"]), [])
        outdated = [e for e in events if OUTDATED.search(str(e.get("Note") or ""))]
        latest = outdated[-1] if outdated else None
        last_event = events[-1] if events else None
        subsequent_count = sum(
            (str(e.get("Change_Date") or ""), int(e.get("Change_Key") or 0)) >
            (str(latest.get("Change_Date") or ""), int(latest.get("Change_Key") or 0))
            for e in events
        ) if latest else 0
        match = EDI_KEY.search(str(latest.get("Note") or "")) if latest else None
        # Conservative: no automatic "current" classification can be inferred.
        status = ("OUTDATED_EVENT_WITH_LATER_CHANGES" if subsequent_count else
                  "OUTDATED_EVENT_NO_LATER_CHANGES") if latest else "NOT_PROVEN_CURRENT"
        row["EDI_Audit"] = {
            "Status": status,
            "Historically_Outdated": bool(latest),
            "Last_Outdated_Change_Key": latest.get("Change_Key") if latest else None,
            "Last_Outdated_Change_Date": latest.get("Change_Date") if latest else None,
            "Last_Outdated_EDI_Key": match.group(1) if match else None,
            "Change_Events": len(events),
            "Events_After_Outdated": subsequent_count,
            "Last_Event_Date": last_event.get("Change_Date") if last_event else None,
            "Adjustment_Reason": (
                "Historical EDI out-of-date; subsequent events require review"
                if subsequent_count else
                "Historical EDI out-of-date; current status not independently verified"
                if latest else "No EDI out-of-date evidence; validity not confirmed"
            ),
            "Provisional_Exclusion": bool(latest),
            "Confirmed_Current": False,
        }
        amount = row.get("Forecast_Revenue")
        row["EDI_Provisional_Adjustment"] = amount if latest and amount is not None else None
        row["Adjusted_Forecast_Provisional"] = (
            0.0 if latest else amount
        ) if amount is not None else None
    return rows

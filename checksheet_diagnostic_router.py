"""Read-only Production Checksheets diagnostic for Plex OpenAccess ODBC.

This endpoint reports requirements and visible evidence; it NEVER asserts
Plex's orange/green pending state and NEVER sends notifications.
"""
from collections import defaultdict
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel, Field
from checksheet_timer_engine import evaluate_timer

class ChecksheetDiagnosticRequest(BaseModel):
    workcenter_key: int = Field(gt=0)
    part_key: int = Field(gt=0)
    control_plan_key: int = Field(gt=0)
    job_op_key: int | None = Field(default=None, gt=0)
    operation_job_op_keys: dict[int, int] | None = None
    operation_numbers: list[int] | None = Field(default=None, max_length=30)
    include_workcenter_context: bool = True
    include_checksheet_downtime: bool = False
    timer_anchor: datetime | None = None
    evaluation_at: datetime | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None

def make_checksheet_diagnostic_router(get_connection, verify_token, pcn=306713):
    router = APIRouter()

    @router.post("/production-checksheets/diagnostic", dependencies=[Security(verify_token)])
    def production_checksheet_diagnostic(req: ChecksheetDiagnosticRequest):
        if (req.window_start is None) != (req.window_end is None):
            raise HTTPException(422, "Provide both window_start and window_end.")
        if req.window_start and (req.window_start >= req.window_end or
                                 req.window_end - req.window_start > timedelta(days=7)):
            raise HTTPException(422, "Window must be positive and at most seven days.")
        if req.window_start and (req.window_start.tzinfo or req.window_end.tzinfo):
            raise HTTPException(422, "Provide naive Plex-local datetime values; timezone mapping is not verified.")

        if (req.timer_anchor is None) != (req.evaluation_at is None):
            raise HTTPException(422, "Provide timer_anchor and evaluation_at together.")
        if req.timer_anchor and (req.timer_anchor.tzinfo or req.evaluation_at.tzinfo):
            raise HTTPException(422, "Timer datetimes must be naive Plex-local values.")
        if req.timer_anchor and req.timer_anchor > req.evaluation_at:
            raise HTTPException(422, "Timer anchor must be on or before evaluation_at.")
        if req.operation_job_op_keys is not None:
            if (not req.operation_job_op_keys or len(req.operation_job_op_keys) > 30
                or any(op <= 0 or job <= 0 for op, job in req.operation_job_op_keys.items())):
                raise HTTPException(422, "operation_job_op_keys requires 1-30 positive operation/job IDs.")
            if req.job_op_key is not None:
                raise HTTPException(422, "Use either job_op_key or operation_job_op_keys.")
        if req.operation_numbers is not None and (not req.operation_numbers or any(n <= 0 for n in req.operation_numbers)):
            raise HTTPException(422, "operation_numbers must contain 1 to 30 positive operation numbers.")

        # Identifiers are validated integers; DataDirect rejects some pyodbc
        # parameter bindings, including timestamp parameters.
        requirements_sql = f"""
SELECT po.Operation_No AS Operation_Number,
       cps.Section_Key AS Section_ID,
       im.Description AS Inspection_Type,
       im.Responsibility AS Responsible_Department,
       im.Required_Each_Shift AS Required_Per_Shift,
       im.Required_Time AS Required_Interval_Minutes,
       im.Delay_Time AS Delay_Minutes
FROM Quality_v_Control_Plan_Section AS cps
INNER JOIN Part_v_Part_Operation AS po
  ON po.Plexus_Customer_No = cps.Plexus_Customer_No
 AND po.Part_Operation_Key = cps.Operation_Key
INNER JOIN Quality_v_Inspection_Mode AS im
  ON im.Plexus_Customer_No = cps.Plexus_Customer_No
 AND im.Inspection_Mode_Key = cps.Inspection_Mode_Key
WHERE cps.Plexus_Customer_No = {int(pcn)}
  AND cps.Control_Plan_Key = {req.control_plan_key}
  AND cps.Active = 1
ORDER BY po.Operation_No, cps.Section_Key
"""
        # Limit per request and report truncation instead of silently
        # treating incomplete evidence as an absence.
        checksheets_sql = f"""
SELECT TOP 5001 c.Checksheet_No AS Checksheet_ID,
       c.Inspection_Date AS Inspection_Date,
       c.Section_Key AS Section_ID,
       c.Job_Op_Key AS Job_Operation_ID,
       c.Inspector AS Inspector_ID
FROM Quality_v_Checksheet AS c
WHERE c.Plexus_Customer_No = {int(pcn)}
  AND c.Workcenter_Key = {req.workcenter_key}
  AND c.Part_Key = {req.part_key}
  AND c.Control_Plan_Key = {req.control_plan_key}
ORDER BY c.Checksheet_No DESC
"""
        # Workcenter Log operation can differ from Control Plan operations.
        # This is historical context, NOT an authoritative active-job lookup.
        context_sql = f"""
SELECT TOP 30 wl.Job_Op_Key AS Job_Operation_ID,
       wl.Part_Key AS Part_ID,
       wl.Part_Operation_Key AS Part_Operation_ID,
       wl.Workcenter_Status_Key AS Workcenter_Status_ID,
       wl.Log_Date AS Event_Date,
       wl.Description AS Event_Description
FROM Part_v_Workcenter_Log AS wl
WHERE wl.Plexus_Customer_No = {int(pcn)}
  AND wl.Workcenter_Key = {req.workcenter_key}
ORDER BY wl.Log_Date DESC
"""
        job_sql = f"""
SELECT TOP 1
       j.Job_Key AS Job_ID,
       j.Job_No AS Job_Number,
       j.Part_Key AS Job_Part_ID,
       j.Job_Status_Key AS Job_Status_ID,
       js.Job_Status AS Job_Status,
       js.Started_Status AS Is_Started,
       js.Completed_Status AS Is_Completed,
       js.Cancelled_Status AS Is_Cancelled,
       j.Completed_Date AS Completed_Date,
       j.Finalized_Date AS Finalized_Date
FROM Part_v_Job_Op AS jo
INNER JOIN Part_v_Job AS j ON j.Job_Key = jo.Job_Key
INNER JOIN Part_v_Job_Status AS js
  ON js.Job_Status_Key = j.Job_Status_Key AND js.PCN = j.PCN
WHERE j.PCN = {int(pcn)}
  AND jo.Job_Op_Key = {{job_op_key}}
"""
        # Use job-specific history, no SQL date comparisons (OpenAccess differs
        # from Plex's SQL editor for literal datetime comparisons).
        downtime_sql = None
        if req.include_checksheet_downtime:
            if req.job_op_key is None:
                raise HTTPException(422, "job_op_key is required for downtime analysis.")
            downtime_sql = f"""
SELECT TOP 501
       wl.Job_Op_Key AS Job_Operation_ID,
       wl.Part_Key AS Part_ID,
       wl.Workcenter_Status_Key AS Workcenter_Status_ID,
       wl.Log_Date AS Event_Date,
       wl.Log_Hours AS Logged_Hours,
       wl.Description AS Event_Description
FROM Part_v_Workcenter_Log AS wl
WHERE wl.Plexus_Customer_No = {int(pcn)}
  AND wl.Workcenter_Key = {req.workcenter_key}
  AND wl.Job_Op_Key = {req.job_op_key}
ORDER BY wl.Log_Date DESC
"""

        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(requirements_sql)
            headers = [column[0] for column in cursor.description]
            requirements = [dict(zip(headers, row)) for row in cursor.fetchall()]
            cursor.execute(checksheets_sql)
            headers = [column[0] for column in cursor.description]
            visible = [dict(zip(headers, row)) for row in cursor.fetchall()]
            workcenter_events = []
            if req.include_workcenter_context:
                cursor.execute(context_sql)
                headers = [column[0] for column in cursor.description]
                workcenter_events = [dict(zip(headers, row)) for row in cursor.fetchall()]
            downtime_events = []
            if downtime_sql is not None:
                cursor.execute(downtime_sql)
                headers = [column[0] for column in cursor.description]
                downtime_events = [dict(zip(headers, row)) for row in cursor.fetchall()]
            latest_job_record = None
            if workcenter_events and workcenter_events[0]["Job_Operation_ID"] is not None:
                job_op_key = int(workcenter_events[0]["Job_Operation_ID"])
                cursor.execute(job_sql.format(job_op_key=job_op_key))
                headers = [column[0] for column in cursor.description]
                job_row = cursor.fetchone()
                if job_row is not None:
                    latest_job_record = dict(zip(headers, job_row))
        except Exception:
            # Do not expose DSN/connection details or credentials to clients.
            raise HTTPException(502, "Plex ODBC checksheet diagnostic query failed.")
        finally:
            if conn is not None:
                conn.close()

        # An event is a historical observation; the latest observed event
        # does not establish current Production/Down/Idle state.
        last_event = workcenter_events[0] if workcenter_events else None
        # Job status describes workflow stage, not whether equipment is running.
        job_stage = None
        if last_event is not None and latest_job_record is not None:
            job_stage = {
                "job_key": latest_job_record["Job_ID"],
                "job_number": latest_job_record["Job_Number"],
                "part_key": latest_job_record["Job_Part_ID"],
                "job_status_key": latest_job_record["Job_Status_ID"],
                "job_status": latest_job_record["Job_Status"],
                "is_started": bool(latest_job_record["Is_Started"]),
                "is_completed": bool(latest_job_record["Is_Completed"]),
                "is_cancelled": bool(latest_job_record["Is_Cancelled"]),
                "completed_date": (
                    latest_job_record["Completed_Date"].isoformat()
                    if latest_job_record["Completed_Date"] is not None else None
                ),
                "finalized_date": (
                    latest_job_record["Finalized_Date"].isoformat()
                    if latest_job_record["Finalized_Date"] is not None else None
                ),
                "classification": (
                    "HISTORICAL_COMPLETED_OR_CANCELLED"
                    if latest_job_record["Is_Completed"] or latest_job_record["Is_Cancelled"]
                    else "HISTORICAL_STARTED_JOB"
                    if latest_job_record["Is_Started"]
                    else "HISTORICAL_OTHER_STATUS"
                ),
                "is_currently_executing_verified": False,
            }
        workcenter_context = {
            "source": "Part_v_Workcenter_Log",
            "latest_job_stage": job_stage,
            "scope": "ALL_PARTS_IN_WORKCENTER",
            "requested_part_key": req.part_key,
            "latest_observed_part_key": last_event["Part_ID"] if last_event else None,
            "is_current_state_verified": False,
            "latest_observed_job_op_key": last_event["Job_Operation_ID"] if last_event else None,
            "latest_observed_part_operation_key": last_event["Part_Operation_ID"] if last_event else None,
            "latest_observed_workcenter_status_key": last_event["Workcenter_Status_ID"] if last_event else None,
            "latest_event_date": (
                last_event["Event_Date"].isoformat()
                if last_event and last_event["Event_Date"] is not None else None
            ),
            "latest_event_description": last_event["Event_Description"] if last_event else None,
            "events_returned": len(workcenter_events),
            "recent_job_op_keys": list(dict.fromkeys(
                event["Job_Operation_ID"] for event in workcenter_events
                if event["Job_Operation_ID"] is not None
            )),
        }
        downtime_truncated = len(downtime_events) > 500
        downtime_events = downtime_events[:500]
        # The next event determines the end of a historical Down interval.
        # This is not a claim that the downtime was automatic or that it was
        # directly caused by a specific missing checksheet.
        ordered_events = sorted(
            (event for event in downtime_events if event["Event_Date"] is not None),
            key=lambda event: event["Event_Date"],
        )
        downtime_periods = []
        for index, event in enumerate(ordered_events):
            if event["Workcenter_Status_ID"] != 5445 or (
                "checksheet" not in str(event["Event_Description"] or "").lower()
            ):
                continue
            start = event["Event_Date"]
            # Only a subsequent event from this Job in the fetched set
            # establishes a bounded period. No inferred active downtime.
            following = ordered_events[index + 1] if index + 1 < len(ordered_events) else None
            end = following["Event_Date"] if following else None
            if req.window_start is not None and not (
                start < req.window_end and (end is None or end > req.window_start)
            ):
                continue
            matching_inspections = sorted(
                (
                    record for record in visible
                    if record["Job_Operation_ID"] == req.job_op_key
                    and record["Inspection_Date"] is not None
                    and record["Inspection_Date"] >= start
                    and (end is None or record["Inspection_Date"] < end)
                ),
                key=lambda record: record["Inspection_Date"],
            )
            downtime_periods.append({
                "started_at": start.isoformat(),
                "ended_at": end.isoformat() if end else None,
                "duration_minutes": round((end - start).total_seconds() / 60, 2) if end else None,
                "end_reason": "NEXT_OBSERVED_JOB_EVENT" if end else "NOT_OBSERVED",
                "next_workcenter_status_key": following["Workcenter_Status_ID"] if following else None,
                "reason": event["Event_Description"],
                "checksheets_during_period": len(matching_inspections),
                "checksheet_ids": [record["Checksheet_ID"] for record in matching_inspections],
                "first_inspection_at": (
                    matching_inspections[0]["Inspection_Date"].isoformat() if matching_inspections else None
                ),
                "last_inspection_at": (
                    matching_inspections[-1]["Inspection_Date"].isoformat() if matching_inspections else None
                ),
                "causality_verified": False,
            })

        truncated = len(visible) > 5000
        visible = visible[:5000]
        by_section = defaultdict(list)
        for record in visible:
            if req.job_op_key is not None and record["Job_Operation_ID"] != req.job_op_key:
                continue
            stamp = record["Inspection_Date"]
            if req.window_start is not None and (
                stamp is None or stamp < req.window_start or stamp >= req.window_end
            ):
                continue
            by_section[record["Section_ID"]].append(record)

        output = []
        for requirement in requirements:
            if req.operation_numbers is not None and requirement["Operation_Number"] not in req.operation_numbers:
                continue
            section_id = requirement["Section_ID"]
            matching = by_section.get(section_id, [])
            if req.operation_job_op_keys is not None:
                # Plex can record sections from many Control Plan operations
                # using the currently executing Job_Op_Key (e.g. op 905).
                # Treat mapping values as allowed capturing job operations,
                # never demand a 1:1 section-operation correspondence.
                allowed_capturing_jobs = set(req.operation_job_op_keys.values())
                matching = [
                    record for record in matching
                    if record["Job_Operation_ID"] in allowed_capturing_jobs
                ]
            last = max(
                (r for r in matching if r["Inspection_Date"] is not None),
                key=lambda r: r["Inspection_Date"],
                default=None,
            )
            no_requirement = (
                requirement["Required_Interval_Minutes"] == -1
                and not requirement["Required_Per_Shift"]
            )
            timer = evaluate_timer(
                requirement,
                [record["Inspection_Date"] for record in matching],
                req.timer_anchor,
                req.evaluation_at,
                (not truncated and (req.job_op_key is not None or req.operation_job_op_keys is not None) and
                 (req.window_start is None or req.timer_anchor is None or req.window_start <= req.timer_anchor) and
                 (req.window_end is None or req.evaluation_at is None or req.evaluation_at <= req.window_end)),
            )
            output.append({
                **requirement,
                "Timer_Diagnostic": timer,
                "Recorded_Checksheets": len(matching),
                "Last_Checksheet_ID": last["Checksheet_ID"] if last else None,
                "Last_Inspection_Date": last["Inspection_Date"].isoformat() if last else None,
                "Evaluation": "NOT_REQUIRED" if no_requirement else "UNVERIFIED",
                "Evaluation_Reason": (
                    "Inspection mode explicitly does not require a checksheet."
                    if no_requirement else
                    "Evidence is informational; Plex pending state and ODBC freshness are unverified."
                ),
            })

        last_visible = max(
            (r for r in visible if r["Inspection_Date"] is not None),
            key=lambda r: r["Inspection_Date"], default=None
        )
        return {
            "data": {
                "workcenter_key": req.workcenter_key,
                "part_key": req.part_key,
                "control_plan_key": req.control_plan_key,
                "job_op_key": req.job_op_key,
                "operation_job_op_keys": req.operation_job_op_keys,
                "capture_job_op_scoping": "UNION_OF_SUPPLIED_CAPTURING_JOB_OP_KEYS",
                "operation_numbers": req.operation_numbers,
                "requirements_returned": len(output),
                "timer_hypothesis_only": True,
                "checksheet_downtime": {
                    "periods": downtime_periods,
                    "events_scanned": len(downtime_events),
                    "events_truncated": downtime_truncated,
                    "interval_rule": "From Down/Checksheets event to next observed event for the same Job",
                    "causality_verified": False,
                } if req.include_checksheet_downtime else None,
                "workcenter_context": workcenter_context if req.include_workcenter_context else None,
                "window_start": req.window_start.isoformat() if req.window_start else None,
                "window_end": req.window_end.isoformat() if req.window_end else None,
                "requirements": output,
                "evidence_rows_scanned": len(visible),
                "evidence_truncated": truncated,
                "latest_visible_inspection_date": (
                    last_visible["Inspection_Date"].isoformat() if last_visible else None
                ),
                "pending_status_verified": False,
                "teams_notifications_enabled": False,
                "warnings": [
                    "ODBC may not contain the latest Plex SPC checksheets.",
                    "Timer predictions are experimental and must be validated manually against Plex cards.",
                    "Recorded_Checksheets=0 does not prove that an inspection is pending.",
                    "Section operation may differ from the Job_Op_Key used when Plex records the checksheet.",
                    "Latest Workcenter Log event is historical across all parts in the Workcenter; it does not prove the job is currently active.",
                    "Job Status Production means started workflow stage, not confirmed running in Workcenter.",
                    *(
                        ["Downtime motive Checksheets is recorded history, not proof Plex automatically blocked production."]
                        if req.include_checksheet_downtime else []
                    ),
                    *(
                        ["Downtime events capped at 500; historical intervals may be incomplete."]
                        if downtime_truncated else []
                    ),
                    *(
                        ["Evidence capped at 5000 latest IDs; counts may be incomplete."]
                        if truncated else []
                    ),
                ],
            }
        }

    return router

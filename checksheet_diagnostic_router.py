"""Read-only Production Checksheets diagnostic for Plex OpenAccess ODBC.

This endpoint reports requirements and visible evidence; it NEVER asserts
Plex's orange/green pending state and NEVER sends notifications.
"""
from collections import defaultdict
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel, Field

class ChecksheetDiagnosticRequest(BaseModel):
    workcenter_key: int = Field(gt=0)
    part_key: int = Field(gt=0)
    control_plan_key: int = Field(gt=0)
    job_op_key: int | None = Field(default=None, gt=0)
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
        except Exception:
            # Do not expose DSN/connection details or credentials to clients.
            raise HTTPException(502, "Plex ODBC checksheet diagnostic query failed.")
        finally:
            if conn is not None:
                conn.close()

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
            section_id = requirement["Section_ID"]
            matching = by_section.get(section_id, [])
            last = max(
                (r for r in matching if r["Inspection_Date"] is not None),
                key=lambda r: r["Inspection_Date"],
                default=None,
            )
            no_requirement = (
                requirement["Required_Interval_Minutes"] == -1
                and not requirement["Required_Per_Shift"]
            )
            output.append({
                **requirement,
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
                    "Recorded_Checksheets=0 does not prove that an inspection is pending.",
                    *(
                        ["Evidence capped at 5000 latest IDs; counts may be incomplete."]
                        if truncated else []
                    ),
                ],
            }
        }

    return router

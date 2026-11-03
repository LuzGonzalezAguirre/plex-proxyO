"""Read-only ODBC evidence for an ordered checksheet group in one Workcenter.

Never infers current Plex status or triggers notifications. A change of Job
does not automatically invalidate historic inspections, but their applicability
to the current shift/model is NOT established by this endpoint.
"""
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel, Field
from checksheet_sequence_engine import reconstruct_sequence


class ChecksheetSequenceRequest(BaseModel):
    workcenter_key: int = Field(gt=0)
    part_key: int = Field(gt=0)
    control_plan_key: int = Field(gt=0)
    section_ids: list[int] = Field(min_length=2, max_length=30)


def make_checksheet_sequence_router(get_connection, verify_token, pcn=306713):
    router = APIRouter()

    @router.post("/production-checksheets/sequence-diagnostic",
                 dependencies=[Security(verify_token)])
    def sequence_diagnostic(req: ChecksheetSequenceRequest):
        section_ids = req.section_ids
        if len(set(section_ids)) != len(section_ids) or any(s <= 0 for s in section_ids):
            raise HTTPException(422, "section_ids must be unique positive identifiers in capture order.")
        section_sql = ", ".join(str(s) for s in section_ids)
        # Validate that every requested section is active under the selected
        # Control Plan. The input list determines sequence order, not SQL order.
        config_sql = f"""
SELECT cps.Section_Key AS Section_ID
FROM Quality_v_Control_Plan_Section AS cps
WHERE cps.Plexus_Customer_No = {int(pcn)}
  AND cps.Control_Plan_Key = {req.control_plan_key}
  AND cps.Active = 1
  AND cps.Section_Key IN ({section_sql})
"""
        evidence_sql = f"""
SELECT TOP 5001 c.Checksheet_No AS Checksheet_ID,
       c.Section_Key AS Section_ID,
       c.Job_Op_Key AS Job_Op_Key,
       c.Inspection_Date AS Inspection_Date
FROM Quality_v_Checksheet AS c
WHERE c.Plexus_Customer_No = {int(pcn)}
  AND c.Workcenter_Key = {req.workcenter_key}
  AND c.Part_Key = {req.part_key}
  AND c.Control_Plan_Key = {req.control_plan_key}
  AND c.Section_Key IN ({section_sql})
ORDER BY c.Checksheet_No DESC
"""
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(config_sql)
            configured = {int(row[0]) for row in cursor.fetchall()}
            if configured != set(section_ids):
                raise HTTPException(422, "Some sections are not active in the specified Control Plan.")
            cursor.execute(evidence_sql)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(502, "Plex ODBC sequence diagnostic query failed.")
        finally:
            if conn is not None:
                conn.close()

        truncated = len(rows) > 5000
        visible = rows[:5000]
        result = reconstruct_sequence(section_ids, [
            {
                "section_key": row["Section_ID"],
                "job_op_key": row["Job_Op_Key"],
                "inspection_date": row["Inspection_Date"],
            } for row in visible
        ])
        # Truncation can remove the start of the oldest observed attempt.
        # Never treat this evidence as authoritative current compliance.
        return {"data": {
            "workcenter_key": req.workcenter_key,
            "part_key": req.part_key,
            "control_plan_key": req.control_plan_key,
            "evidence_rows_scanned": len(visible),
            "evidence_truncated": truncated,
            "history_scope": "ALL_CAPTURING_JOBS_FOR_WC_PART_PLAN_SECTIONS",
            "sequence": result,
            "pending_status_verified": False,
            "teams_notifications_enabled": False,
            "warnings": [
                "Historical capture sequences do not establish current Plex card status.",
                "Job/shift/model changes may alter applicability of a historical cycle.",
                "Required intervals and reminder scheduling are not calculated here.",
                *(
                    ["Latest 5000 section captures reached; historical cycles may be incomplete."]
                    if truncated else []
                ),
            ],
        }}

    return router

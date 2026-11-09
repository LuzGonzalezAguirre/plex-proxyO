"""Regression tests for shared production Job Op recording (e.g. op 905)."""
import unittest
from datetime import datetime
from checksheet_diagnostic_router import (
    ChecksheetDiagnosticRequest,
    make_checksheet_diagnostic_router,
)


class FakeCursor:
    def __init__(self):
        self.description = []
        self.rows = []
        self.executed_sql = []

    def execute(self, sql):
        self.executed_sql.append(sql)
        if "Quality_v_Control_Plan_Section" in sql:
            names = ["Operation_Number", "Section_ID", "Inspection_Type",
                     "Responsible_Department", "Required_Per_Shift",
                     "Required_Interval_Minutes", "Delay_Minutes"]
            self.rows = [
                (820, 2345981, "2 Hrs", "CELL OPERATOR", 1, 120, 15),
                (830, 2345982, "2 Hrs", "CELL OPERATOR", 1, 120, 15),
            ]
        elif "Quality_v_Checksheet" in sql:
            names = ["Checksheet_ID", "Inspection_Date", "Section_ID",
                     "Job_Operation_ID", "Inspector_ID"]
            self.rows = [
                (770500, datetime(2026, 11, 2, 10), 2345981, 188522218, "A"),
                (770499, datetime(2026, 11, 2, 9), 2345982, 123456789, "B"),
            ]
        else:
            raise AssertionError("Unexpected SQL")
        self.description = [(name,) for name in names]
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self):
        self.fake_cursor = FakeCursor()

    def cursor(self):
        return self.fake_cursor

    def close(self):
        pass


class TestChecksheetCaptureScope(unittest.TestCase):
    def setUp(self):
        self.connection = FakeConnection()
        router = make_checksheet_diagnostic_router(
            lambda: self.connection, lambda: True
        )
        self.endpoint = next(
            route.endpoint for route in router.routes
            if route.path == "/production-checksheets/diagnostic"
        )

    def request(self, **overrides):
        return ChecksheetDiagnosticRequest(
            workcenter_key=75408,
            part_key=4573945,
            control_plan_key=446577,
            include_workcenter_context=False,
            **overrides,
        )

    def test_shared_production_job_op_is_used_for_section_820(self):
        data = self.endpoint(
            self.request(operation_job_op_keys={820: 188522218, 830: 188522218})
        )["data"]
        by_op = {r["Operation_Number"]: r for r in data["requirements"]}
        self.assertEqual(by_op[820]["Recorded_Checksheets"], 1)
        self.assertEqual(by_op[820]["Observed_Capture_Job_Op_Keys"], [188522218])
        self.assertEqual(by_op[830]["Recorded_Checksheets"], 0)
        self.assertEqual(by_op[830]["Observed_Capture_Job_Op_Keys"], [])
        self.assertEqual(by_op[830]["Evaluation"], "UNVERIFIED")
        self.assertEqual(by_op[820]["Evidence_Job_Scope"], "UNION_OF_CAPTURING_JOB_OP_KEYS")
        self.assertFalse(data["pending_status_verified"])
        self.assertFalse(data["teams_notifications_enabled"])

    def test_single_job_filter_is_applied_before_top_limit(self):
        self.endpoint(self.request(job_op_key=188522218))
        sql = next(s for s in self.connection.fake_cursor.executed_sql
                   if "Quality_v_Checksheet" in s)
        self.assertIn("AND c.Job_Op_Key = 188522218", sql)
        self.assertIn("SELECT TOP 5001", sql)

    def test_multiple_capture_job_filter_is_applied_before_top_limit(self):
        self.endpoint(self.request(operation_job_op_keys={
            820: 188522218, 830: 188312299, 860: 188522218
        }))
        sql = next(s for s in self.connection.fake_cursor.executed_sql
                   if "Quality_v_Checksheet" in s)
        self.assertIn("AND c.Job_Op_Key IN (188312299, 188522218)", sql)

    def test_section_filter_is_pushed_before_top_and_limits_requirements(self):
        data = self.endpoint(
            self.request(section_ids=[2345981])
        )["data"]
        sql = next(statement for statement in self.connection.fake_cursor.executed_sql
                   if "Quality_v_Checksheet" in statement)
        self.assertIn("AND c.Section_Key IN (2345981)", sql)
        self.assertIn("SELECT TOP 5001", sql)
        self.assertEqual(data["section_ids"], [2345981])
        self.assertEqual([r["Section_ID"] for r in data["requirements"]], [2345981])

    def test_section_filter_rejects_duplicate_ids(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as error:
            self.endpoint(self.request(section_ids=[2345981, 2345981]))
        self.assertEqual(error.exception.status_code, 422)

    def test_section_filter_rejects_empty_ids(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as error:
            self.endpoint(self.request(section_ids=[]))
        self.assertEqual(error.exception.status_code, 422)

    def test_unscoped_query_keeps_all_jobs_for_historical_review(self):
        self.endpoint(self.request())
        sql = next(s for s in self.connection.fake_cursor.executed_sql
                   if "Quality_v_Checksheet" in s)
        self.assertNotIn("AND c.Job_Op_Key", sql)

    def test_unscoped_evidence_is_labelled_historical(self):
        data = self.endpoint(self.request())["data"]
        by_op = {r["Operation_Number"]: r for r in data["requirements"]}
        self.assertEqual(by_op[830]["Recorded_Checksheets"], 1)
        self.assertEqual(by_op[830]["Evidence_Job_Scope"], "ALL_JOBS_HISTORICAL")
        self.assertTrue(any("historical Jobs" in w for w in data["warnings"]))


if __name__ == "__main__":
    unittest.main()

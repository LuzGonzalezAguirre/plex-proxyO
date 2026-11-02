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

    def execute(self, sql):
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
    def cursor(self):
        return FakeCursor()

    def close(self):
        pass


class TestChecksheetCaptureScope(unittest.TestCase):
    def setUp(self):
        router = make_checksheet_diagnostic_router(
            lambda: FakeConnection(), lambda: True
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

    def test_unscoped_evidence_is_labelled_historical(self):
        data = self.endpoint(self.request())["data"]
        by_op = {r["Operation_Number"]: r for r in data["requirements"]}
        self.assertEqual(by_op[830]["Recorded_Checksheets"], 1)
        self.assertEqual(by_op[830]["Evidence_Job_Scope"], "ALL_JOBS_HISTORICAL")
        self.assertTrue(any("historical Jobs" in w for w in data["warnings"]))


if __name__ == "__main__":
    unittest.main()

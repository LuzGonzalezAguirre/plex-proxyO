"""SQL-scope regression tests for read-only WC sequence endpoint."""
import unittest
from datetime import datetime
from fastapi import HTTPException
from checksheet_sequence_router import (
    ChecksheetSequenceRequest, make_checksheet_sequence_router
)

SECTIONS = [2345913, 2345914, 2345915, 2345916, 2345917]


class Cursor:
    def __init__(self, config=None, evidence=None):
        self.config = SECTIONS if config is None else config
        self.evidence = evidence if evidence is not None else []
        self.sql = []
        self.description = []

    def execute(self, sql):
        self.sql.append(sql)
        if "Quality_v_Control_Plan_Section" in sql:
            self.description = [("Section_ID",)]
            self.rows = [(section,) for section in self.config]
        elif "Quality_v_Checksheet" in sql:
            names = ["Checksheet_ID", "Section_ID", "Job_Op_Key", "Inspection_Date"]
            self.description = [(name,) for name in names]
            self.rows = self.evidence
        else:
            raise AssertionError("Unexpected SQL")

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, cursor):
        self.test_cursor = cursor

    def cursor(self):
        return self.test_cursor

    def close(self):
        pass


class TestSequenceRoute(unittest.TestCase):
    def endpoint(self, cursor):
        router = make_checksheet_sequence_router(
            lambda: Connection(cursor), lambda: True
        )
        return next(route.endpoint for route in router.routes
                    if route.path == "/production-checksheets/sequence-diagnostic")

    def req(self):
        return ChecksheetSequenceRequest(
            workcenter_key=75408, part_key=4573943, control_plan_key=446576,
            section_ids=SECTIONS
        )

    def test_query_filters_wc_part_plan_and_sections_before_top(self):
        cursor = Cursor(evidence=[
            (769041, 2345917, 188312277, datetime(2026, 10, 28, 6, 55, 4)),
            (769038, 2345916, 188312277, datetime(2026, 10, 28, 6, 54, 52)),
            (769037, 2345915, 188312277, datetime(2026, 10, 28, 6, 54, 25)),
            (769036, 2345914, 188312277, datetime(2026, 10, 28, 6, 53, 46)),
            (769035, 2345913, 188312277, datetime(2026, 10, 28, 6, 53, 36)),
        ])
        result = self.endpoint(cursor)(self.req())["data"]
        sql = cursor.sql[1]
        self.assertIn("c.Workcenter_Key = 75408", sql)
        self.assertIn("c.Part_Key = 4573943", sql)
        self.assertIn("c.Control_Plan_Key = 446576", sql)
        self.assertIn("c.Section_Key IN (2345913, 2345914, 2345915, 2345916, 2345917)", sql)
        self.assertNotIn("c.Job_Op_Key =", sql)
        self.assertEqual(result["sequence"]["complete_cycles"], 1)
        self.assertEqual(result["sequence"]["last_completed_at"], "2026-10-28T06:55:04")
        self.assertFalse(result["pending_status_verified"])

    def test_rejects_sections_not_in_plan(self):
        cursor = Cursor(config=SECTIONS[:4])
        with self.assertRaises(HTTPException) as exc:
            self.endpoint(cursor)(self.req())
        self.assertEqual(exc.exception.status_code, 422)
        self.assertEqual(len(cursor.sql), 1)

    def test_truncation_is_reported_without_verifying_pending(self):
        row = (769035, 2345913, 188312277, datetime(2026, 10, 28, 6, 53, 36))
        cursor = Cursor(evidence=[row] * 5001)
        result = self.endpoint(cursor)(self.req())["data"]
        self.assertEqual(result["evidence_rows_scanned"], 5000)
        self.assertTrue(result["evidence_truncated"])
        self.assertFalse(result["pending_status_verified"])
        self.assertFalse(result["teams_notifications_enabled"])

    def test_rejects_duplicate_sections(self):
        cursor = Cursor()
        req = self.req()
        req.section_ids = [2345913, 2345913]
        with self.assertRaises(HTTPException):
            self.endpoint(cursor)(req)
        self.assertEqual(cursor.sql, [])


if __name__ == "__main__":
    unittest.main()

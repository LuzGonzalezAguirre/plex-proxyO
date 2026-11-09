"""Conservative WC departmental classification; never assert current overdue."""
import unittest
from checksheet_workcenter_summary import (
    classify_responsibility, summarize_workcenter_group
)

SECTIONS = [2345913, 2345914, 2345915, 2345916, 2345917]


def sequence(complete):
    return {
        "attempts": [{"complete": complete}] if complete is not None else [],
        "last_attempt_complete": complete,
        "last_completed_at": "2026-10-28T06:55:04" if complete else None,
        "next_expected_section": None if complete else 2345914,
        "complete_cycles": 1 if complete else 0,
    }


def config(responsibility="CELL OPERATOR"):
    return {s: {"responsibility": responsibility} for s in SECTIONS}


class TestWorkcenterSummary(unittest.TestCase):
    def test_production_with_completed_history_remains_unverified(self):
        summary = summarize_workcenter_group(SECTIONS, config(), sequence(True))
        self.assertEqual(summary["department"], "PRODUCTION")
        self.assertEqual(summary["historical_sequence_state"],
                         "LATEST_OBSERVED_SEQUENCE_COMPLETE")
        self.assertEqual(summary["workcenter_status"], "UNVERIFIED")
        self.assertFalse(summary["alert_eligible"])

    def test_partial_history_is_not_declared_pending(self):
        summary = summarize_workcenter_group(SECTIONS, config(), sequence(False))
        self.assertEqual(summary["historical_sequence_state"],
                         "LATEST_OBSERVED_SEQUENCE_PARTIAL")
        self.assertEqual(summary["department_status"], "UNVERIFIED")
        self.assertFalse(summary["teams_notifications_enabled"])

    def test_empty_evidence_is_not_declared_overdue(self):
        summary = summarize_workcenter_group(SECTIONS, config(), sequence(None))
        self.assertEqual(summary["historical_sequence_state"], "NO_CAPTURE_OBSERVED")
        self.assertEqual(summary["workcenter_status"], "UNVERIFIED")

    def test_unknown_responsibility_requires_manual_mapping(self):
        summary = summarize_workcenter_group(SECTIONS, config(""), sequence(True))
        self.assertEqual(summary["department"], "UNASSIGNED")
        self.assertIn("RESPONSIBILITY_NOT_UNAMBIGUOUS",
                      summary["blocking_reasons"])

    def test_mixed_responsibilities_do_not_silently_assign(self):
        selected = config()
        selected[SECTIONS[-1]]["responsibility"] = "QUALITY"
        summary = summarize_workcenter_group(SECTIONS, selected, sequence(True))
        self.assertEqual(summary["department"], "MIXED")
        self.assertFalse(summary["alert_eligible"])

    def test_truncated_history_blocks_confirmed_status(self):
        summary = summarize_workcenter_group(SECTIONS, config(), sequence(True), True)
        self.assertTrue(summary["evidence_truncated"])
        self.assertIn("EVIDENCE_TRUNCATED", summary["blocking_reasons"])

    def test_explicit_quality_classification(self):
        self.assertEqual(classify_responsibility(" QUALITY "), "QUALITY")
        self.assertEqual(classify_responsibility("CELL OPERATOR"), "PRODUCTION")
        self.assertEqual(classify_responsibility("MAINTENANCE"), "UNASSIGNED")


if __name__ == "__main__":
    unittest.main()

"""Read-only WC sequence reconstruction tests using Volvo 75408 sections."""
import unittest
from datetime import datetime, timedelta
from checksheet_sequence_engine import reconstruct_sequence

SECTIONS = [2345913, 2345914, 2345915, 2345916, 2345917]


def record(section, minute, job=188312277):
    return {
        "section_key": section,
        "inspection_date": datetime(2026, 10, 28, 6, 53) + timedelta(minutes=minute),
        "job_op_key": job,
    }


class TestChecksheetSequenceEngine(unittest.TestCase):
    def test_completed_cycle_anchors_to_last_capture(self):
        result = reconstruct_sequence(SECTIONS, [
            record(2345917, 2), record(2345915, 1),
            record(2345913, 0), record(2345914, 0.2),
            record(2345916, 1.5),
        ])
        self.assertEqual(result["complete_cycles"], 1)
        self.assertEqual(result["last_completed_at"], "2026-10-28T06:55:00")
        self.assertTrue(result["last_attempt_complete"])
        self.assertFalse(result["plex_status_verified"])

    def test_partial_sequence_shows_next_without_five_alerts(self):
        result = reconstruct_sequence(SECTIONS, [
            record(2345913, 0), record(2345914, 0.2)
        ])
        self.assertEqual(result["next_expected_section"], 2345915)
        self.assertEqual(result["complete_cycles"], 0)
        self.assertFalse(result["notifications_enabled"])

    def test_historical_job_change_does_not_reset_section_sequence(self):
        result = reconstruct_sequence(SECTIONS, [
            record(2345913, 0, 188312299),
            record(2345914, 0.2, 188312299),
            record(2345915, 0.5, 188099699),
            record(2345916, 0.7, 188099699),
            record(2345917, 1.0, 188099699),
        ])
        self.assertEqual(result["complete_cycles"], 1)
        self.assertEqual(result["attempts"][0]["capture_job_op_keys"],
                         [188099699, 188312299])

    def test_out_of_order_does_not_claim_complete(self):
        result = reconstruct_sequence(SECTIONS, [
            record(2345913, 0), record(2345915, 1),
            record(2345914, 2), record(2345916, 3),
            record(2345917, 4),
        ])
        self.assertEqual(result["complete_cycles"], 0)
        self.assertEqual(result["next_expected_section"], 2345915)
        self.assertIn("OUT_OF_ORDER_OR_UNMATCHED_CAPTURE", result["anomalies"])

    def test_new_first_section_restarts_incomplete_attempt(self):
        result = reconstruct_sequence(SECTIONS, [
            record(2345913, 0), record(2345914, 1),
            record(2345913, 2), record(2345914, 3),
            record(2345915, 4), record(2345916, 5), record(2345917, 6)
        ])
        self.assertEqual(result["complete_cycles"], 1)
        self.assertEqual(len(result["attempts"]), 2)
        self.assertIn("RESTART_BEFORE_COMPLETION", result["anomalies"])

    def test_no_capture_does_not_mean_overdue(self):
        result = reconstruct_sequence(SECTIONS, [])
        self.assertIsNone(result["last_attempt_complete"])
        self.assertIsNone(result["last_completed_at"])
        self.assertIsNone(result["next_expected_section"])
        self.assertFalse(result["plex_status_verified"])


if __name__ == "__main__":
    unittest.main()

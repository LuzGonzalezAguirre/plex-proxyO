"""Run: py -3.11 -m unittest discover -s tests -p 'test_checksheet_timer.py'"""
import unittest
from datetime import datetime
from checksheet_timer_engine import evaluate_timer

class TestExperimentalChecksheetTimer(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 27, 12)
        self.rule = {"Required_Interval_Minutes": 120, "Required_Per_Shift": 0, "Delay_Minutes": 15}

    def test_before_due(self):
        result = evaluate_timer(self.rule, [], self.start, datetime(2026, 10, 27, 13), True)
        self.assertEqual(result["state"], "PREDICTED_WITHIN_WINDOW")

    def test_after_grace(self):
        result = evaluate_timer(self.rule, [], self.start, datetime(2026, 10, 27, 14, 16), True)
        self.assertEqual(result["state"], "PREDICTED_PENDING")

    def test_inspection_resets_clock_hypothetically(self):
        result = evaluate_timer(self.rule, [datetime(2026, 10, 27, 13)], self.start, datetime(2026, 10, 27, 14, 16), True)
        self.assertEqual(result["state"], "PREDICTED_WITHIN_WINDOW")

    def test_shift_plus_interval_remains_unknown(self):
        rule = {**self.rule, "Required_Per_Shift": 1}
        result = evaluate_timer(rule, [], self.start, datetime(2026, 10, 27, 16), True)
        self.assertEqual(result["state"], "UNVERIFIED")

    def test_no_anchor_remains_unknown(self):
        result = evaluate_timer(self.rule, [], None, datetime(2026, 10, 27, 16), True)
        self.assertEqual(result["state"], "UNVERIFIED")

    def test_incomplete_data_remains_unknown(self):
        result = evaluate_timer(self.rule, [], self.start, datetime(2026, 10, 27, 16), False)
        self.assertEqual(result["state"], "UNVERIFIED")

    def test_no_requirement(self):
        rule = {"Required_Interval_Minutes": -1, "Required_Per_Shift": 0, "Delay_Minutes": 0}
        result = evaluate_timer(rule, [], None, None, False)
        self.assertEqual(result["state"], "NOT_REQUIRED")

if __name__ == "__main__":
    unittest.main()

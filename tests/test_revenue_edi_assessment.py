import unittest
from revenue_edi_assessment import attach_edi_history

class Cursor:
    def execute(self, sql):
        self.sql = sql

def query_to_list(cursor):
    return [
        {"Release_Key": 1, "Change_Key": 100, "Change_Date": "2026-10-08T11:00:00",
         "Note": "New EDI data caused this Release to become out-of-date (1a) (EDI Key: 58257902)"},
        {"Release_Key": 1, "Change_Key": 101, "Change_Date": "2026-10-08T11:02:00",
         "Note": "Later automatic EDI event"},
    ]

class RevenueEdiAssessmentTests(unittest.TestCase):
    def test_later_events_remain_unverified(self):
        rows = [
            {"Release_Key": 1, "Forecast_Revenue": 64253.28},
            {"Release_Key": 2, "Forecast_Revenue": 200.0},
        ]
        attach_edi_history(Cursor(), query_to_list, rows)
        self.assertTrue(rows[0]["EDI_Audit"]["Historically_Outdated"])
        self.assertEqual(rows[0]["EDI_Audit"]["Events_After_Outdated"], 1)
        self.assertEqual(rows[0]["EDI_Audit"]["Status"], "OUTDATED_EVENT_WITH_LATER_CHANGES")
        self.assertFalse(rows[0]["EDI_Audit"]["Confirmed_Current"])
        self.assertEqual(rows[0]["EDI_Provisional_Adjustment"], 64253.28)
        self.assertFalse(rows[1]["EDI_Audit"]["Historically_Outdated"])
        self.assertFalse(rows[1]["EDI_Audit"]["Confirmed_Current"])
        self.assertEqual(rows[1]["Adjusted_Forecast_Provisional"], 200.0)

if __name__ == "__main__":
    unittest.main()

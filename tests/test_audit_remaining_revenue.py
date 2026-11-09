import unittest
from decimal import Decimal

from scripts.audit_remaining_revenue import build_audit


def row(key, revenue, release_type="Firm (862)", status="NOT_PROVEN_CURRENT"):
    return {
        "Release_Key": key, "PO_Line_Key": 100, "PO_No": "PO123",
        "Customer_No": 55, "Part_No_Rev": "100.2 Rev:1",
        "Release_Type": release_type, "Rel_Status": "Open",
        "Ship_Date": "2026-10-25", "Rel_Qty": 100, "Shipped": 0,
        "Rel_Bal": 100, "Unit_Price": 1, "Price_Key": 10,
        "Remaining_Revenue": revenue, "Forecast_Revenue": revenue,
        "Price_Issue": None,
        "Coincidence": {"Exact_Group_Size": 1, "Overlap_Group_Size": 1,
                        "Other_Release_Types": [], "Classification": "Unique"},
        "EDI_Audit": {"Status": status,
                      "Historically_Outdated": status != "NOT_PROVEN_CURRENT",
                      "Events_After_Outdated": 2 if status != "NOT_PROVEN_CURRENT" else 0},
    }


class RemainingRevenueAuditTests(unittest.TestCase):
    def test_no_removal_of_possible_duplicates(self):
        a, b = row(10, 1000), row(11, 1000)
        a["Coincidence"]["Exact_Group_Size"] = 2
        b["Coincidence"]["Exact_Group_Size"] = 2
        ledger, candidates, overlaps, matches = build_audit(
            [a, b], Decimal("1000")
        )
        self.assertEqual(len(ledger), 2)
        self.assertEqual(len(candidates), 2)
        self.assertIn("EXACT_MATCH_CANDIDATE", ledger[0]["Review_Flags"])
        self.assertEqual(overlaps[0]["Total_Remaining"], "2000")
        self.assertEqual(overlaps[0]["Potential_Extra_If_One_Only"], "1000")
        self.assertEqual(matches[0]["Absolute_Distance_To_Gap"], "0")

    def test_historical_edi_is_flag_only(self):
        data = [row(1, 5000, status="OUTDATED_EVENT_WITH_LATER_CHANGES")]
        ledger, candidates, _, _ = build_audit(data, Decimal("5000"))
        self.assertEqual(len(ledger), 1)
        self.assertEqual(candidates[0]["Remaining_Revenue"], 5000)
        self.assertIn("HISTORICAL_EDI_OUTDATED", candidates[0]["Review_Flags"])

    def test_unpriced_does_not_create_revenue_match(self):
        data = [row(1, None)]
        ledger, candidates, _, matches = build_audit(data, Decimal("5000"))
        self.assertEqual(len(ledger), 1)
        self.assertIn("UNPRICED", ledger[0]["Review_Flags"])
        self.assertEqual(candidates, [])
        self.assertEqual(matches, [])

    def test_empty_input(self):
        ledger, candidates, overlaps, matches = build_audit([], Decimal("20288.65"))
        self.assertEqual((ledger, candidates, overlaps, matches), ([], [], [], []))

    def test_pair_amount_closeness_is_diagnostic_only(self):
        a = row(1, 15000, release_type="Firm (862)")
        b = row(2, 5288.65, release_type="Forecast")
        for r in (a, b):
            r["Coincidence"]["Other_Release_Types"] = [
                "Forecast" if r["Release_Type"] != "Forecast" else "Firm (862)"
            ]
        _, candidates, _, matches = build_audit([a, b], Decimal("20288.65"))
        self.assertEqual(len(candidates), 2)
        self.assertEqual(matches[0]["Absolute_Distance_To_Gap"], "0")
        self.assertEqual(matches[0]["Count"], 2)


if __name__ == "__main__":
    unittest.main()

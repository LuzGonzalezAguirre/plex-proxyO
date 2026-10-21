import unittest
from revenue_release_audit import annotate_release_coincidences

class RevenueReleaseAuditTests(unittest.TestCase):
    def row(self, key, release_type="Firm (862)", date="2026-10-06", quantity=728):
        return {"Release_Key": key, "PO_Line_Key": 18216638,
                "Customer_No": 332211, "Part_No_Rev": "43306.3 Rev:2",
                "Ship_Date": date, "Rel_Qty": quantity, "Shipped": 0,
                "Rel_Bal": quantity, "Release_Type": release_type}

    def test_exact_matches_are_flagged_but_never_dropped(self):
        rows = [self.row(1), self.row(2), self.row(3)]
        result = annotate_release_coincidences(rows)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["Coincidence"]["Exact_Group_Size"], 3)
        self.assertEqual(result[0]["Coincidence"]["Exact_Release_Keys"], [1,2,3])
        self.assertFalse(result[0]["Coincidence"]["Confirmed_Duplicate"])

    def test_cross_type_overlap_not_exact(self):
        rows = [self.row(1), self.row(2, release_type="Forecast")]
        annotate_release_coincidences(rows)
        self.assertEqual(rows[0]["Coincidence"]["Exact_Group_Size"], 1)
        self.assertTrue(rows[0]["Coincidence"]["Review_Required"])
        self.assertEqual(rows[0]["Coincidence"]["Other_Release_Types"], ["Forecast"])

    def test_different_date_not_flagged(self):
        rows = [self.row(1), self.row(2, date="2026-10-07")]
        annotate_release_coincidences(rows)
        self.assertFalse(rows[0]["Coincidence"]["Review_Required"])

if __name__ == "__main__":
    unittest.main()

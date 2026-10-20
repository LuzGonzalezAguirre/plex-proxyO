"""Unit tests for the candidate Plex demand allocation (no live Plex connection)."""
import unittest

from main import allocate_demand_inventory


def release(key, part, day, balance):
    return {
        "Release_Key": key,
        "_Part_Key": part,
        "Ship_Date": f"2026-09-{day:02d}T00:00:00",
        "Rel_Bal": balance,
    }


class DemandAllocationTests(unittest.TestCase):
    def test_ready_then_wip_across_releases(self):
        records = [
            release(3, 4662019, 23, 1254),
            release(1, 4662019, 15, 262),
            release(2, 4662019, 16, 1254),
        ]
        result = allocate_demand_inventory(
            records, {4662019: {"ready": 1320, "wip": 386}}
        )
        self.assertEqual(
            [(r["Qty_Ready"], r["Qty_WIP"]) for r in result],
            [(0, 190), (262, 0), (1058, 196)],
        )
        self.assertTrue(all("_Part_Key" not in r and "Release_Key" not in r for r in result))

    def test_partial_ready_and_wip(self):
        rows = [release(1, 4662017, 4, 66)]
        result = allocate_demand_inventory(
            rows, {4662017: {"ready": 40, "wip": 35}}
        )
        self.assertEqual((result[0]["Qty_Ready"], result[0]["Qty_WIP"]), (40, 26))

    def test_negative_balance_never_allocates(self):
        rows = [release(1, 1, 4, -5), release(2, 1, 5, 10)]
        result = allocate_demand_inventory(
            rows, {1: {"ready": 6, "wip": 10}}
        )
        self.assertEqual(
            [(r["Qty_Ready"], r["Qty_WIP"]) for r in result],
            [(0, 0), (6, 4)],
        )


if __name__ == "__main__":
    unittest.main()

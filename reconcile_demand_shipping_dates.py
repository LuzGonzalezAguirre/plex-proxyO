"""Read-only reconciliation for remaining Warehouse Demand columns.

Focuses on October 2026 Deere releases. Prints candidate release dates,
linked shipper numbers (possibly multiple per release), and a candidate
running release balance by PO line. Running balance is diagnostic only;
it is NOT yet the definition of Plex Total Rel Due.
"""

from collections import defaultdict
from datetime import date, datetime

from main import PCN, get_connection

PO_NUMBERS = ("5500146406", "5500146407")
START = "2026-10-01"
END_EXCLUSIVE = "2026-11-01"


def rows(cursor, title, sql):
    print("\n=== " + title + " ===")
    try:
        cursor.execute(sql)
        fields = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        print(" | ".join(fields))
        for item in data:
            print(" | ".join("NULL" if value is None else str(value) for value in item))
        print("ROW_COUNT:", len(data))
        return fields, data
    except Exception as exc:
        print("QUERY FAILED:", exc)
        return [], []


def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            base = f"""
                FROM Sales_v_Release r
                JOIN Sales_v_PO_Line pol
                  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
                JOIN Sales_v_PO po
                  ON pol.PO_Key = po.PO_Key AND po.PCN = po.PCN
                WHERE r.PCN = {PCN}
                  AND po.PO_No IN ('5500146406', '5500146407')
                  AND r.Ship_Date >= '{START}'
                  AND r.Ship_Date < '{END_EXCLUSIVE}'
            """
            fields, release_rows = rows(cursor, "October releases: candidate dates and balances", f"""
                SELECT po.PO_No, r.PO_Line_Key, r.Release_Key,
                       r.Ship_Date, r.Due_Date, r.Schedule_Date,
                       r.Customer_Due_Date, r.Preliminary_Due_Date,
                       r.Ship_From, r.Quantity, r.Quantity_Shipped
                {base}
                ORDER BY po.PO_No, r.PO_Line_Key, r.Ship_Date, r.Release_Key
            """)
            rows(cursor, "October release to shipper links", f"""
                SELECT po.PO_No, r.Release_Key, s.Shipper_No,
                       s.Ship_Date AS Actual_Ship_Date, sl.Quantity
                {base}
                JOIN Sales_v_Shipper_Line_Release slr
                  ON slr.Release_Key = r.Release_Key AND slr.PCN = r.PCN
                JOIN Sales_v_Shipper_Line sl
                  ON sl.Shipper_Line_Key = slr.Shipper_Line_Key AND sl.PCN = slr.PCN
                JOIN Sales_v_Shipper s
                  ON s.Shipper_Key = sl.Shipper_Key AND s.PCN = sl.PCN
                ORDER BY po.PO_No, r.Release_Key, s.Ship_Date
            """)
            if fields:
                positions = {name: i for i, name in enumerate(fields)}
                running = defaultdict(float)
                print("\n=== Candidate cumulative outstanding balance by PO line ===")
                print("PO_No | PO_Line_Key | Release_Key | Ship_Date | Rel_Bal | Running_Bal")
                for item in release_rows:
                    get = lambda name: item[positions[name]]
                    key = (get("PO_No"), get("PO_Line_Key"))
                    balance = float(get("Quantity") or 0) - float(get("Quantity_Shipped") or 0)
                    running[key] += balance
                    print(f"{key[0]} | {key[1]} | {get('Release_Key')} | "
                          f"{get('Ship_Date')} | {balance:g} | {running[key]:g}")
                print("NOTE: cumulative totals exclude releases outside the October window.")
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

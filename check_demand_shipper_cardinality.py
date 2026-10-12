"""Read-only October Demand shipper cardinality reconciliation.

Checks if a one-to-many shipper relationship would duplicate release rows.
No business data modifications and no secrets are printed.
"""
from main import PCN, get_connection

START = "2026-10-01"
END = "2026-11-01"


def display(cursor, title, sql):
    print(f"\n=== {title} ===")
    try:
        cursor.execute(sql)
        print(" | ".join(c[0] for c in cursor.description))
        data = cursor.fetchall()
        for record in data:
            print(" | ".join("NULL" if v is None else str(v) for v in record))
        print("Rows:", len(data))
    except Exception as exc:
        print("QUERY FAILED:", exc)


def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            display(cursor, "Shipper links per October release (multiple only)", f"""
                SELECT slr.Release_Key, COUNT(*) AS Link_Count,
                       COUNT(DISTINCT s.Shipper_No) AS Shipper_Count
                FROM Sales_v_Shipper_Line_Release AS slr
                JOIN Sales_v_Shipper_Line AS sl
                  ON sl.Shipper_Line_Key = slr.Shipper_Line_Key
                 AND sl.PCN = slr.PCN
                JOIN Sales_v_Shipper AS s
                  ON s.Shipper_Key = sl.Shipper_Key AND s.PCN = sl.PCN
                JOIN Sales_v_Release AS r
                  ON r.Release_Key = slr.Release_Key AND r.PCN = slr.PCN
                WHERE r.PCN = {PCN}
                  AND r.Ship_Date >= '{START}'
                  AND r.Ship_Date < '{END}'
                GROUP BY slr.Release_Key
                HAVING COUNT(*) > 1
                ORDER BY Link_Count DESC
            """)
            display(cursor, "October requested date candidates (sample)", f"""
                SELECT TOP 20 r.Release_Key, po.PO_No,
                       r.Ship_Date, r.Due_Date,
                       r.Preliminary_Due_Date, r.Schedule_Date,
                       r.Customer_Due_Date
                FROM Sales_v_Release AS r
                JOIN Sales_v_PO_Line AS pol
                  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
                JOIN Sales_v_PO AS po
                  ON pol.PO_Key = po.PO_Key AND r.PCN = po.PCN
                WHERE r.PCN = {PCN}
                  AND r.Ship_Date >= '{START}'
                  AND r.Ship_Date < '{END}'
                ORDER BY r.Ship_Date, r.Release_Key
            """)
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

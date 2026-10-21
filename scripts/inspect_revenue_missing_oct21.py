"""Read-only reconciliation of Plex Demand releases missing from Revenue Forecast.
Run: py -3.11 scripts/inspect_revenue_missing_oct21.py
"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import get_connection

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.Release_Key, po.PO_No, po.Customer_No,
                   r.Ship_Date, r.Quantity, r.Quantity_Shipped,
                   rs.Release_Status, rt.Release_Type,
                   r.PO_Line_Key, p.Part_No, p.Revision
            FROM Sales_v_Release AS r
            INNER JOIN Sales_v_PO_Line AS pol
              ON pol.PO_Line_Key = r.PO_Line_Key AND pol.PCN = r.PCN
            INNER JOIN Sales_v_PO AS po
              ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
            LEFT JOIN Sales_v_Release_Status AS rs
              ON rs.Release_Status_Key = r.Release_Status_Key AND rs.PCN = r.PCN
            LEFT JOIN Sales_v_Release_Type AS rt
              ON rt.Release_Type_Key = r.Release_Type_Key AND rt.PCN = r.PCN
            LEFT JOIN Part_v_Part AS p
              ON p.Part_Key = pol.Part_Key AND p.Plexus_Customer_No = pol.PCN
            WHERE r.PCN = 306713
              AND po.PO_No IN ('30908', '6903')
              AND r.Ship_Date >= '2026-10-20'
              AND r.Ship_Date < '2026-10-23'
            ORDER BY po.PO_No, r.Ship_Date, r.Release_Key
        """)
        print(" | ".join(col[0] for col in cursor.description))
        count = 0
        for record in cursor.fetchall():
            count += 1
            print(" | ".join(str(value) for value in record))
        print("Rows:", count)
        print("No records were changed.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

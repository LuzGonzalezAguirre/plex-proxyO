"""Read-only checks for remaining Warehouse Demand report columns.

Inspects shipper relationships, candidate dates and release sequences for
two Deere POs. Does not update Plex or print secrets.
"""
from main import PCN, get_connection

PO_NUMBERS = ("5500146407", "5500146406")


def query(cursor, title, sql):
    print(f"\n=== {title} ===")
    try:
        cursor.execute(sql)
        print(" | ".join(c[0] for c in cursor.description))
        for row in cursor.fetchall():
            print(" | ".join("NULL" if value is None else str(value) for value in row))
    except Exception as exc:
        print("QUERY FAILED:", exc)


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            query(cur, "Release dates, keys and quantities", f"""
                SELECT TOP 25 po.PO_No, r.Release_Key, r.Release_No,
                       r.Ship_Date, r.Due_Date, r.Schedule_Date,
                       r.Customer_Due_Date, r.Preliminary_Due_Date,
                       r.Ship_From, r.Quantity, r.Quantity_Shipped,
                       pol.Part_Key
                FROM Sales_v_Release r
                JOIN Sales_v_PO_Line pol
                  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
                JOIN Sales_v_PO po
                  ON pol.PO_Key = po.PO_Key AND pol.PCN = po.PCN
                WHERE r.PCN = {PCN}
                  AND po.PO_No IN ('5500146407', '5500146406')
                ORDER BY r.Ship_Date, r.Release_Key
            """)
            query(cur, "Shipper link multiplicity by release", f"""
                SELECT TOP 25 po.PO_No, r.Release_Key,
                       COUNT(slr.Shipper_Line_Key) AS Shipper_Link_Rows
                FROM Sales_v_Release r
                JOIN Sales_v_PO_Line pol
                  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
                JOIN Sales_v_PO po
                  ON pol.PO_Key = po.PO_Key AND pol.PCN = po.PCN
                LEFT JOIN Sales_v_Shipper_Line_Release slr
                  ON slr.Release_Key = r.Release_Key AND slr.PCN = r.PCN
                WHERE r.PCN = {PCN}
                  AND po.PO_No IN ('5500146407', '5500146406')
                GROUP BY po.PO_No, r.Release_Key
                ORDER BY po.PO_No, r.Release_Key
            """)
            query(cur, "Actual shipper numbers by release (sample)", f"""
                SELECT TOP 30 po.PO_No, r.Release_Key,
                       s.Shipper_No, s.Ship_Date AS Actual_Ship_Date,
                       sl.Quantity AS Shipper_Line_Quantity
                FROM Sales_v_Release r
                JOIN Sales_v_PO_Line pol
                  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
                JOIN Sales_v_PO po
                  ON pol.PO_Key = po.PO_Key AND pol.PCN = po.PCN
                JOIN Sales_v_Shipper_Line_Release slr
                  ON slr.Release_Key = r.Release_Key AND slr.PCN = r.PCN
                JOIN Sales_v_Shipper_Line sl
                  ON sl.Shipper_Line_Key = slr.Shipper_Line_Key
                 AND sl.PCN = slr.PCN
                JOIN Sales_v_Shipper s
                  ON s.Shipper_Key = sl.Shipper_Key AND s.PCN = sl.PCN
                WHERE r.PCN = {PCN}
                  AND po.PO_No IN ('5500146407', '5500146406')
                ORDER BY r.Release_Key, s.Ship_Date
            """)
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

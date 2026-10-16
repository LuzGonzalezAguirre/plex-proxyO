"""Read-only reconciliation of active container stock for two Deere parts.

Produces location-level aggregates and allocation summaries without serial
numbers or mutations. Candidate finished-goods classification is diagnostic,
not a validated Plex Qty Ready definition.
"""
from main import get_connection, PCN

PART_KEYS = (4662018, 4662019)


def run(cursor, label, query):
    print("\n=== " + label + " ===")
    try:
        cursor.execute(query)
        print(" | ".join(c[0] for c in cursor.description))
        for row in cursor.fetchall():
            print(" | ".join("NULL" if x is None else str(x) for x in row))
    except Exception as exc:
        print("QUERY FAILED:", exc)


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            run(cur, "Active stock by part/location/status", f"""
                SELECT c.Part_Key, c.Location, c.Container_Status,
                       COUNT(*) AS Containers, SUM(c.Quantity) AS Pieces
                FROM Part_v_Container c
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN (4662018, 4662019)
                  AND c.Active = 1 AND c.Quantity > 0
                GROUP BY c.Part_Key, c.Location, c.Container_Status
                ORDER BY c.Part_Key, c.Location, c.Container_Status
            """)
            run(cur, "Active containers with shipping status policy", f"""
                SELECT c.Part_Key, c.Container_Status, cs.Allow_Ship,
                       cs.Defective, cs.Shop_Floor_Only,
                       COUNT(*) AS Containers, SUM(c.Quantity) AS Pieces
                FROM Part_v_Container c
                LEFT JOIN Part_v_Container_Status cs
                  ON cs.Plexus_Customer_No = c.Plexus_Customer_No
                 AND cs.Container_Status = c.Container_Status
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN (4662018, 4662019)
                  AND c.Active = 1 AND c.Quantity > 0
                GROUP BY c.Part_Key, c.Container_Status,
                         cs.Allow_Ship, cs.Defective, cs.Shop_Floor_Only
                ORDER BY c.Part_Key, c.Container_Status
            """)
            run(cur, "Allocation totals by part and PO line (diagnostic)", f"""
                SELECT c.Part_Key, a.PO_Line_Key,
                       COUNT(*) AS Allocation_Rows,
                       SUM(c.Quantity) AS Container_Pieces
                FROM Part_v_Inventory_Allocation a
                JOIN Part_v_Container c
                  ON a.Container_Serial_No = c.Serial_No AND a.PCN = c.Plexus_Customer_No
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN (4662018, 4662019)
                  AND c.Active = 1 AND c.Quantity > 0
                GROUP BY c.Part_Key, a.PO_Line_Key
                ORDER BY c.Part_Key, a.PO_Line_Key
            """)
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

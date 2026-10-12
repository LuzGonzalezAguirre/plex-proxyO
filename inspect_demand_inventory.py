"""Read-only Demand inventory diagnostic for two Deere customer parts.

Aggregates quantities without disclosing container serial numbers. Does not
change data or apply speculative Qty Ready / Qty WIP calculations.
"""

from main import PCN, get_connection

CUSTOMER_PARTS = ("DZ133883", "DZ133884")


def execute(cursor, label, statement):
    print(f"\n=== {label} ===")
    try:
        cursor.execute(statement)
        print(" | ".join(column[0] for column in cursor.description))
        for row in cursor.fetchall():
            print(" | ".join("NULL" if value is None else str(value) for value in row))
    except Exception as exc:
        print(f"QUERY FAILED: {exc}")


def main():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            parts = ", ".join("'" + part + "'" for part in CUSTOMER_PARTS)
            execute(cursor, "Customer part mappings (deduplicated)", f"""
                SELECT DISTINCT cp.Customer_Part_No, p.Part_Key, p.Part_No, p.Revision
                FROM Sales_v_PO AS po
                JOIN Sales_v_PO_Line AS pol
                  ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
                JOIN Part_v_Customer_Part AS cp
                  ON pol.Customer_Part_Key = cp.Customer_Part_Key
                 AND cp.Plexus_Customer_No = pol.PCN
                JOIN Part_v_Part AS p
                  ON pol.Part_Key = p.Part_Key
                 AND p.Plexus_Customer_No = pol.PCN
                WHERE po.PCN = {PCN}
                  AND po.Customer_No = 772808
                  AND cp.Customer_Part_No IN ({parts})
            """)
            execute(cursor, "Container quantities by customer part, status and active flag", f"""
                SELECT p.Part_No, p.Revision, c.Container_Status, c.Active,
                       COUNT(*) AS Container_Count, SUM(c.Quantity) AS Qty
                FROM Part_v_Container AS c
                JOIN Part_v_Part AS p
                  ON c.Part_Key = p.Part_Key
                 AND c.Plexus_Customer_No = p.Plexus_Customer_No
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN (
                    SELECT pol.Part_Key
                    FROM Sales_v_PO AS po
                    JOIN Sales_v_PO_Line AS pol
                      ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
                    JOIN Part_v_Customer_Part AS cp
                      ON pol.Customer_Part_Key = cp.Customer_Part_Key
                     AND cp.Plexus_Customer_No = pol.PCN
                    WHERE po.PCN = {PCN} AND po.Customer_No = 772808
                      AND cp.Customer_Part_No IN ({parts})
                  )
                GROUP BY p.Part_No, p.Revision, c.Container_Status, c.Active
                ORDER BY p.Part_No, c.Container_Status, c.Active
            """)
            execute(cursor, "Container status shipping policy", f"""
                SELECT cs.Container_Status, cs.Allow_Ship, cs.Defective,
                       cs.Scrap, cs.Customer_Staged, cs.Shop_Floor_Only
                FROM Part_v_Container_Status AS cs
                WHERE cs.Plexus_Customer_No = {PCN}
            """)
            execute(cursor, "Container quantities by location (top 40)", f"""
                SELECT TOP 40 p.Part_No, c.Location, c.Container_Status, c.Active,
                       COUNT(*) AS Container_Count, SUM(c.Quantity) AS Qty
                FROM Part_v_Container AS c
                JOIN Part_v_Part AS p
                  ON c.Part_Key = p.Part_Key
                 AND c.Plexus_Customer_No = p.Plexus_Customer_No
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN (
                    SELECT pol.Part_Key
                    FROM Sales_v_PO AS po
                    JOIN Sales_v_PO_Line AS pol
                      ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
                    JOIN Part_v_Customer_Part AS cp
                      ON pol.Customer_Part_Key = cp.Customer_Part_Key
                     AND cp.Plexus_Customer_No = pol.PCN
                    WHERE po.PCN = {PCN} AND po.Customer_No = 772808
                      AND cp.Customer_Part_No IN ({parts})
                  )
                GROUP BY p.Part_No, c.Location, c.Container_Status, c.Active
                ORDER BY p.Part_No, Qty DESC
            """)
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

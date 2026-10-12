"""Read-only validation of Plex Shippable routing flags for Demand.

Inspects routing operations for two Deere parts and aggregates active
containers by operation and both shipping eligibility flags. Does not
modify Plex data or treat the candidate quantities as final KPIs.
"""
from main import PCN, get_connection

PART_KEYS = "4662018, 4662019"


def show(cursor, title, sql):
    print(f"\n=== {title} ===")
    try:
        cursor.execute(sql)
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
            show(cursor, "Routing operations and Shippable flag", f"""
                SELECT Part_Key, Part_Operation_Key, Operation_No,
                       Description, Active, Shippable,
                       Previous_Part_Operation_Key, Next_Part_Operation_Key,
                       Routing_Version_Key
                FROM Part_v_Part_Operation
                WHERE Plexus_Customer_No = {PCN}
                  AND Part_Key IN ({PART_KEYS})
                ORDER BY Part_Key, Operation_No
            """)
            show(cursor, "Active container inventory by routing and shipping policy", f"""
                SELECT c.Part_Key, c.Part_Operation_Key, po.Operation_No,
                       po.Active AS Operation_Active, po.Shippable,
                       c.Container_Status, cs.Allow_Ship, cs.Defective,
                       COUNT(*) AS Containers, SUM(c.Quantity) AS Pieces
                FROM Part_v_Container AS c
                LEFT JOIN Part_v_Part_Operation AS po
                  ON c.Part_Operation_Key = po.Part_Operation_Key
                 AND c.Part_Key = po.Part_Key
                 AND c.Plexus_Customer_No = po.Plexus_Customer_No
                LEFT JOIN Part_v_Container_Status AS cs
                  ON c.Container_Status = cs.Container_Status
                 AND c.Plexus_Customer_No = cs.Plexus_Customer_No
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Part_Key IN ({PART_KEYS})
                  AND c.Active = 1 AND c.Quantity > 0
                GROUP BY c.Part_Key, c.Part_Operation_Key, po.Operation_No,
                         po.Active, po.Shippable, c.Container_Status,
                         cs.Allow_Ship, cs.Defective
                ORDER BY c.Part_Key, po.Operation_No, c.Container_Status
            """)
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

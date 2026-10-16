"""Read-only operation metadata and container stage inspection for Demand.

Checks actual part-operation columns and aggregates active containers by
Part_Operation_Key. No serial numbers, changes or inferred stage labels.
"""

from main import PCN, get_connection

PART_KEYS = (4662018, 4662019)
VIEWS = (
    "Part_v_Part_Operation",
    "Part_v_Part_Operation_e",
    "Part_v_Workcenter",
    "Part_v_Container",
)


def show(cursor, label, sql):
    print("\n=== " + label + " ===")
    try:
        cursor.execute(sql)
        print(" | ".join(col[0] for col in cursor.description))
        for row in cursor.fetchall():
            print(" | ".join("NULL" if x is None else str(x) for x in row))
    except Exception as exc:
        print("QUERY FAILED:", exc)


def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            for view in VIEWS:
                print("\n=== COLUMNS: " + view + " ===")
                try:
                    cursor.execute("SELECT TOP 0 * FROM " + view)
                    print(", ".join(column[0] for column in cursor.description))
                except Exception as exc:
                    print("NOT AVAILABLE:", exc)

            show(cursor, "Active containers by actual operation and location", f"""
                SELECT Part_Key, Part_Operation_Key, Location, Container_Status,
                       COUNT(*) AS Container_Count, SUM(Quantity) AS Pieces
                FROM Part_v_Container
                WHERE Plexus_Customer_No = {PCN}
                  AND Part_Key IN (4662018, 4662019)
                  AND Active = 1 AND Quantity > 0
                GROUP BY Part_Key, Part_Operation_Key, Location, Container_Status
                ORDER BY Part_Key, Part_Operation_Key, Location
            """)
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

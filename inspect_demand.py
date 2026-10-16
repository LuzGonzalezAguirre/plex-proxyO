"""Inspect Plex view column names used by Warehouse Demand.

Read-only metadata inspection. Uses the existing Plex DSN configuration.
Does not retrieve production rows or modify database records.
"""

from main import get_connection

VIEWS = (
    "Sales_v_Release",
    "Sales_v_PO",
    "Sales_v_PO_Line",
    "Sales_v_Customer",
    "Part_v_Container",
    "Part_v_Part",
)

KEYWORDS = (
    "ship", "ready", "wip", "inventory", "quantity",
    "customer", "release", "carrier", "date", "location",
    "name", "available",
)


def main() -> None:
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            for view in VIEWS:
                print(f"\n{'=' * 55}\nVIEW: {view}")
                try:
                    cursor.execute(f"SELECT TOP 0 * FROM {view}")
                    columns = [column[0] for column in cursor.description]
                    print(f"Total columns: {len(columns)}")
                    for column in columns:
                        if any(keyword in column.lower() for keyword in KEYWORDS):
                            print(f"  {column}")
                except Exception as exc:
                    print(f"NOT AVAILABLE: {exc}")
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

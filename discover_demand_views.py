"""Discover Plex ODBC views related to Warehouse Demand, read-only.

Uses ODBC catalog metadata only; prints matching view/table names and
selected column names. No business rows are retrieved.
"""

from main import get_connection

VIEW_WORDS = (
    "customer", "inventory", "stock", "container",
    "shipment", "shipper", "shipping", "release", "warehouse",
)
COLUMN_WORDS = (
    "customer", "name", "ship", "carrier", "ready", "avail",
    "wip", "quantity", "location", "date", "release", "balance",
)
MAX_RESULTS = 120


def main() -> None:
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            print("Discovering ODBC catalog entries...")
            try:
                catalog = cursor.tables()
                matches = []
                for row in catalog:
                    name = str(row.table_name)
                    if any(term in name.lower() for term in VIEW_WORDS):
                        matches.append((name, str(row.table_type)))
                print(f"Matching catalog objects: {len(matches)}")
                for name, kind in sorted(set(matches))[:MAX_RESULTS]:
                    print(f"  {name} [{kind}]")
                if len(matches) > MAX_RESULTS:
                    print(f"  ... showing first {MAX_RESULTS} names only")
            except Exception as exc:
                print(f"ODBC catalog listing unavailable: {exc}")
                print("Catalog listing is optional for this driver.")

            for view in ("Sales_v_Release", "Sales_v_PO", "Part_v_Container"):
                print(f"\n{'=' * 55}\nFULL COLUMNS: {view}")
                try:
                    cursor.execute(f"SELECT TOP 0 * FROM {view}")
                    print(", ".join(col[0] for col in cursor.description))
                except Exception as exc:
                    print(f"Unavailable: {exc}")
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

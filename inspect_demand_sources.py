"""Read-only targeted Plex ODBC discovery for Warehouse Demand.

Shows column metadata for customer/inventory reference views and lists likely
inventory, shipping, customer and container-status sources. No business rows
are fetched, and no database records are changed.
"""

from main import get_connection

INSPECT_VIEWS = (
    "Common_v_Customer",
    "Common_v_Customer_e",
    "Accelerated_Customer_v",
    "Part_v_Container",
    "Part_v_Part",
    "Sales_v_Release",
    "Sales_v_PO",
)

CANDIDATE_RULES = {
    "customer_master": lambda n: (
        "customer" in n and
        ("common_v_customer" in n or "accelerated_customer" in n)
        and not n.endswith("_e")
    ),
    "inventory_or_stock": lambda n: (
        ("inventory" in n or "stock" in n or "on_hand" in n)
        and (n.startswith("part_") or n.startswith("sales_"))
        and not n.endswith("_e")
    ),
    "container_status": lambda n: (
        "container" in n and
        ("status" in n or "location" in n)
        and not n.endswith("_e")
    ),
    "shipping": lambda n: (
        n.startswith("sales_") and
        ("shipper" in n or "shipment" in n)
        and not n.endswith("_e")
    ),
}
MAX_NAMES_PER_GROUP = 35


def main() -> None:
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            try:
                names = sorted({
                    str(row.table_name)
                    for row in cursor.tables()
                })
            except Exception as exc:
                print(f"ODBC catalog listing unavailable: {exc}")
                names = []

            for group, match in CANDIDATE_RULES.items():
                selected = [name for name in names if match(name.lower())]
                print(f"\n{'=' * 60}")
                print(f"CANDIDATES: {group} ({len(selected)})")
                for name in selected[:MAX_NAMES_PER_GROUP]:
                    print(f"  {name}")
                if len(selected) > MAX_NAMES_PER_GROUP:
                    print(f"  ... {len(selected) - MAX_NAMES_PER_GROUP} more")

            for view in INSPECT_VIEWS:
                print(f"\n{'=' * 60}")
                print(f"COLUMNS: {view}")
                try:
                    cursor.execute(f"SELECT TOP 0 * FROM {view}")
                    print(", ".join(col[0] for col in cursor.description))
                except Exception as exc:
                    print(f"NOT AVAILABLE: {exc}")
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

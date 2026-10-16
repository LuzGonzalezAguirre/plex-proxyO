"""Validate Warehouse Demand customer mapping and inspect inventory/shipper keys.

Read-only. Uses existing Plex DSN and PCN from main.py. Queries a small,
fixed customer sample and catalog-only metadata for candidate views.
No credentials or production serial numbers are printed.
"""

from main import get_connection, PCN

CUSTOMER_NUMBERS = (772808, 332342, 332165, 332211)
COLUMN_VIEWS = (
    "Part_v_Container_Status",
    "Part_v_Current_Inventory_Code",
    "Part_v_Inventory_Allocation",
    "Part_v_Inventory_Snapshot",
    "Part_v_Customer_Inventory",
    "Part_v_MRP_Demand_Inventory",
    "Sales_v_Shipper",
    "Sales_v_Shipper_Line",
    "Sales_v_Shipper_Line_Release",
)


def execute_preview(cursor, label: str, sql: str) -> None:
    print(f"\n--- {label} ---")
    try:
        cursor.execute(sql)
        names = [c[0] for c in cursor.description]
        print(" | ".join(names))
        for row in cursor.fetchall():
            print(" | ".join(str(value) if value is not None else "NULL" for value in row))
    except Exception as exc:
        print(f"QUERY FAILED: {exc}")


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            for view in COLUMN_VIEWS:
                print(f"\n=== COLUMNS: {view} ===")
                try:
                    cur.execute(f"SELECT TOP 0 * FROM {view}")
                    print(", ".join(col[0] for col in cur.description))
                except Exception as exc:
                    print(f"UNAVAILABLE: {exc}")

            customers = ", ".join(str(n) for n in CUSTOMER_NUMBERS)
            execute_preview(
                cur,
                "Customer master rows",
                "SELECT Customer_No, Name, Customer_Code "
                "FROM Common_v_Customer "
                f"WHERE Plexus_Customer_No = {PCN} "
                f"AND Customer_No IN ({customers})",
            )
            execute_preview(
                cur,
                "Customer name join cardinality (one row per PO customer expected)",
                "SELECT po.Customer_No, COUNT(*) AS Rows_After_Join "
                "FROM Sales_v_PO AS po "
                "LEFT JOIN Common_v_Customer AS cust "
                "ON cust.Customer_No = po.Customer_No "
                "AND cust.Plexus_Customer_No = po.PCN "
                f"WHERE po.PCN = {PCN} AND po.Customer_No IN ({customers}) "
                "GROUP BY po.Customer_No",
            )
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

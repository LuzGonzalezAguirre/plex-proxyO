"""Inspect pricing columns for Revenue Forecast. Metadata only, read-only."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import get_connection

VIEWS = (
    "Sales_v_Release",
    "Sales_v_PO_Line",
    "Sales_v_PO",
    "Sales_v_PO_Line_Price",
    "Sales_v_PO_Price",
    "Part_v_Part",
    "Accounting_v_AR_Invoice_Dist",
)

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        for name in VIEWS:
            print("\nVIEW:", name)
            try:
                cols = list(cursor.columns(table=name))
                if not cols:
                    print("  No columns returned / view not available")
                for col in cols:
                    col_name = str(col.column_name)
                    if any(word in col_name.lower() for word in (
                        "price", "cost", "amount", "currency", "quantity",
                        "part_key", "po_line", "release_key", "invoice_link",
                        "revision", "customer"
                    )):
                        print(" ", col_name, str(col.type_name))
            except Exception as exc:
                print("  Error:", type(exc).__name__, str(exc)[:200])
    finally:
        conn.close()

if __name__ == "__main__":
    main()

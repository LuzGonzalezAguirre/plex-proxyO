"""Read-only diagnostic of possible repeated Plex releases.

Run in plex-proxy directory:
    py -3.11 scripts/inspect_revenue_release_history.py

Verifies four Volvo 43306.3 releases dated 2026-10-06.
Does not delete or alter Plex records.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import get_connection

RELEASE_KEYS = (617870276, 617870146, 617870250, 617870274)
KEYS = ", ".join(str(key) for key in RELEASE_KEYS)

def print_query(cursor, title, sql):
    print("\n=== " + title + " ===")
    cursor.execute(sql)
    cols = [c[0] for c in cursor.description]
    print(" | ".join(cols))
    rows = cursor.fetchall()
    for row in rows:
        print(" | ".join(str(value) for value in row))
    print("Rows:", len(rows))

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        print_query(cursor, "Release details (Plex)", f"""
            SELECT r.Release_Key, r.PO_Line_Key, r.Ship_Date,
                   r.Quantity, r.Quantity_Shipped,
                   r.Release_Status_Key, r.Release_Type_Key
            FROM Sales_v_Release AS r
            WHERE r.PCN = 306713 AND r.Release_Key IN ({KEYS})
            ORDER BY r.Release_Key
        """)
        print_query(cursor, "Release change history", f"""
            SELECT ch.Release_Key, ch.Change_Key, ch.Change_Date,
                   ch.Change_By, ch.Note
            FROM Sales_v_Release_Change AS ch
            WHERE ch.Release_Key IN ({KEYS})
            ORDER BY ch.Release_Key, ch.Change_Date, ch.Change_Key
        """)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

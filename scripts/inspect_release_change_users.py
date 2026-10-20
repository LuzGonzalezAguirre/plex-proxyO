"""Inspect Plex users to resolve Sales_v_Release_Change.Change_By.

Run: py -3.11 scripts/inspect_release_change_users.py
Read-only metadata. Do not print any user personal data.
"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import get_connection

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        matches = []
        for table in cursor.tables():
            name = str(table.table_name)
            if "user" in name.lower() and any(term in name.lower() for term in ("plex", "user", "employee")):
                matches.append((str(table.table_schem or ""), name))
        print("Candidate user views:", len(matches))
        for schema, name in matches[:35]:
            print("\nVIEW:", f"{schema}.{name}" if schema else name)
            try:
                for c in cursor.columns(table=name, schema=schema or None):
                    n = str(c.column_name)
                    if any(word in n.lower() for word in ("user", "employee", "name", "first", "last", "active")):
                        print("  ", n, str(c.type_name))
            except Exception as exc:
                print("  Column inspection failed:", type(exc).__name__)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

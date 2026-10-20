"""Read-only Plex ODBC discovery for Release Change History.

Run from the plex-proxyO root:
    python scripts/inspect_release_history.py

Does not print credentials or modify data.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import get_connection

TERMS = ("release", "history", "audit", "change")
FIELD_TERMS = ("change", "user", "note", "release", "date", "created", "modified")

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        candidates = []
        for entry in cursor.tables():
            name = str(entry.table_name)
            if "release" in name.lower() and any(
                term in name.lower() for term in ("history", "audit", "change")
            ):
                candidates.append((str(entry.table_schem or ""), name))
        print("Candidate history views:", len(candidates))
        for schema, name in candidates[:40]:
            print("\nVIEW:", f"{schema}.{name}" if schema else name)
            try:
                cols = list(cursor.columns(table=name, schema=schema or None))
                for col in cols:
                    col_name = str(col.column_name)
                    if any(term in col_name.lower() for term in FIELD_TERMS):
                        print("   ", col_name, str(col.type_name))
            except Exception as exc:
                print("   Could not inspect columns:", type(exc).__name__)
        if not candidates:
            print("No matching history view; inspect Plex Data Source Explorer for release audit tables.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

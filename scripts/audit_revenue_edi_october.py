"""Read-only October revenue release EDI audit.

Usage:
  py -3.11 scripts/audit_revenue_edi_october.py
  py -3.11 scripts/audit_revenue_edi_october.py --start 2026-10-01 --end 2026-10-31

Exports CSV files in ./revenue_edi_audit/ for review. No Plex writes.
"""
import argparse
import csv
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import get_connection

PCN = 306713
OUTDATED = re.compile(r"new edi data caused this release to become out-of-date", re.I)
EDI_KEY = re.compile(r"EDI Key:\s*(\d+)", re.I)

def fetch(cursor, sql):
    cursor.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, record)) for record in cursor.fetchall()]

def iso(value):
    if value is None:
        return ""
    return str(value)[:10]

def write_csv(path, rows, columns):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default="2026-10-31")
    parser.add_argument("--output", default="revenue_edi_audit")
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start or (end-start).days > 180:
        parser.error("The range must be between 0 and 180 days")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Query only needed fields; perform date filtering in Python like Demand.
        releases = fetch(cursor, f"""
            SELECT r.Release_Key, r.PO_Line_Key, r.Ship_Date,
                   r.Quantity AS Rel_Qty,
                   ISNULL(r.Quantity_Shipped,0) AS Shipped,
                   r.Quantity - ISNULL(r.Quantity_Shipped,0) AS Rel_Bal,
                   ISNULL(rt.Release_Type,'') AS Release_Type,
                   ISNULL(rs.Release_Status,'') AS Release_Status,
                   po.Customer_No, ISNULL(po.PO_No,'') AS PO_No
            FROM Sales_v_Release AS r
            INNER JOIN Sales_v_PO_Line AS pol
              ON pol.PO_Line_Key = r.PO_Line_Key AND pol.PCN = r.PCN
            INNER JOIN Sales_v_PO AS po
              ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
            LEFT JOIN Sales_v_Release_Type AS rt
              ON rt.Release_Type_Key = r.Release_Type_Key AND rt.PCN = r.PCN
            LEFT JOIN Sales_v_Release_Status AS rs
              ON rs.Release_Status_Key = r.Release_Status_Key AND rs.PCN = r.PCN
            WHERE r.PCN = {PCN}
        """)
        releases = [r for r in releases if args.start <= iso(r["Ship_Date"]) <= args.end]
        groups = defaultdict(list)
        overlaps = defaultdict(set)
        for row in releases:
            exact_key = (row["PO_Line_Key"], iso(row["Ship_Date"]), str(row["Rel_Qty"]),
                         str(row["Shipped"]), str(row["Rel_Bal"]), row["Release_Type"])
            groups[exact_key].append(row)
            overlaps[(row["PO_Line_Key"], iso(row["Ship_Date"]))].add(row["Release_Type"])
        candidates = []
        for row in releases:
            exact_key = (row["PO_Line_Key"], iso(row["Ship_Date"]), str(row["Rel_Qty"]),
                         str(row["Shipped"]), str(row["Rel_Bal"]), row["Release_Type"])
            same_po_date = overlaps[(row["PO_Line_Key"], iso(row["Ship_Date"]))]
            if len(groups[exact_key]) > 1 or len(same_po_date) > 1:
                candidates.append(row)
        keys = sorted({int(row["Release_Key"]) for row in candidates})
        history = defaultdict(list)
        for offset in range(0, len(keys), 70):
            batch = ",".join(map(str, keys[offset:offset + 70]))
            changes = fetch(cursor, f"""
                SELECT ch.Release_Key, ch.Change_Key, ch.Change_Date,
                       ch.Change_By, ch.Note
                FROM Sales_v_Release_Change AS ch
                WHERE ch.Release_Key IN ({batch})
                ORDER BY ch.Release_Key, ch.Change_Date, ch.Change_Key
            """)
            for change in changes:
                history[int(change["Release_Key"])].append(change)
        details = []
        confirmed = 0
        ambiguous = 0
        for row in candidates:
            key = int(row["Release_Key"])
            events = history.get(key, [])
            obsolete_events = [event for event in events if OUTDATED.search(str(event["Note"] or ""))]
            # No out-of-date note is NOT positive evidence of current validity.
            state = "EDI_OUTDATED_CONFIRMED" if obsolete_events else "NOT_PROVEN_CURRENT"
            if obsolete_events:
                confirmed += 1
            else:
                ambiguous += 1
            latest = max(obsolete_events, key=lambda e: (str(e["Change_Date"]), int(e["Change_Key"]))) if obsolete_events else None
            edi_match = EDI_KEY.search(str(latest["Note"] or "")) if latest else None
            details.append({
                "Release_Key": key,
                "PO_Line_Key": row["PO_Line_Key"],
                "Customer_No": row["Customer_No"],
                "PO_No": row["PO_No"],
                "Ship_Date": iso(row["Ship_Date"]),
                "Release_Type": row["Release_Type"],
                "Release_Status": row["Release_Status"],
                "Rel_Qty": row["Rel_Qty"],
                "Shipped": row["Shipped"],
                "Rel_Bal": row["Rel_Bal"],
                "Classification": state,
                "Outdated_Change_Key": latest["Change_Key"] if latest else "",
                "Outdated_Change_Date": latest["Change_Date"] if latest else "",
                "EDI_Key": edi_match.group(1) if edi_match else "",
                "Change_Events": len(events),
            })
        columns = list(details[0]) if details else [
            "Release_Key", "PO_Line_Key", "Customer_No", "PO_No", "Ship_Date",
            "Release_Type", "Release_Status", "Rel_Qty", "Shipped", "Rel_Bal",
            "Classification", "Outdated_Change_Key", "Outdated_Change_Date",
            "EDI_Key", "Change_Events",
        ]
        write_csv(out/"release_candidates.csv", details, columns)
        event_rows = []
        for key in keys:
            for event in history.get(key, []):
                event_rows.append({
                    "Release_Key": key, "Change_Key": event["Change_Key"],
                    "Change_Date": event["Change_Date"], "Change_By": event["Change_By"],
                    "Note": event["Note"],
                })
        write_csv(out/"release_changes.csv", event_rows,
                  ["Release_Key", "Change_Key", "Change_Date", "Change_By", "Note"])
        print("Date range:", args.start, "to", args.end)
        print("Total releases:", len(releases))
        print("Coincidence candidates:", len(candidates))
        print("EDI outdated confirmed:", confirmed)
        print("Not proven current:", ambiguous)
        print("History events:", len(event_rows))
        print("Outputs:", out.resolve())
        print("Note: no data deleted, no financial totals changed.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

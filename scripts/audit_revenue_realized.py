"""Read-only October revenue reconciliation: release states vs posted invoice accounts.

This is NOT an invoice-to-release match. Ship_Date and Invoice_Date differ.
Usage:
  py -3.11 scripts/audit_revenue_realized.py --start 2026-10-01 --end 2026-10-21
Output:
  revenue_realized_audit/release_status_summary.csv
  revenue_realized_audit/invoice_accounts.csv
  revenue_realized_audit/release_status_detail.csv
No database writes.
"""
import argparse
import csv
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import get_connection

PCN = 306713
ALLOWED_TYPES = {"firm (862)", "forecast", "hold"}
ACTIVE_STATES = {"open", "scheduled"}

def rows(cursor, query):
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, values)) for values in cursor.fetchall()]

def save(path, data, columns):
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default="2026-10-21")
    parser.add_argument("--output", default="revenue_realized_audit")
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start or (end - start).days > 180:
        parser.error("Invalid date range (maximum 180 days)")
    exclusive = (end + timedelta(days=1)).isoformat()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    connection = get_connection()
    try:
        cursor = connection.cursor()
        release_data = rows(cursor, f"""
            SELECT r.Release_Key, r.PO_Line_Key, r.Ship_Date,
                   r.Quantity AS Rel_Qty, ISNULL(r.Quantity_Shipped, 0) AS Shipped,
                   ISNULL(rs.Release_Status, '') AS Release_Status,
                   ISNULL(rt.Release_Type, '') AS Release_Type,
                   po.Customer_No, ISNULL(po.PO_No, '') AS PO_No
            FROM Sales_v_Release AS r
            INNER JOIN Sales_v_PO_Line AS pol
              ON pol.PO_Line_Key = r.PO_Line_Key AND pol.PCN = r.PCN
            INNER JOIN Sales_v_PO AS po
              ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
            LEFT JOIN Sales_v_Release_Status AS rs
              ON rs.Release_Status_Key = r.Release_Status_Key AND rs.PCN = r.PCN
            LEFT JOIN Sales_v_Release_Type AS rt
              ON rt.Release_Type_Key = r.Release_Type_Key AND rt.PCN = r.PCN
            WHERE r.PCN = {PCN}
        """)
        selected = [r for r in release_data if r["Ship_Date"] is not None
                    and args.start <= str(r["Ship_Date"])[:10] <= args.end]
        group = defaultdict(lambda: {"Releases": 0, "Rel_Qty": Decimal(0),
                                      "Shipped_Qty": Decimal(0)})
        details = []
        for r in selected:
            state = str(r["Release_Status"] or "").strip()
            kind = str(r["Release_Type"] or "").strip()
            eligible = state.lower() in ACTIVE_STATES and kind.lower() in ALLOWED_TYPES
            key = (state, kind, "Active" if eligible else "Outside active scope")
            item = group[key]
            item["Releases"] += 1
            item["Rel_Qty"] += Decimal(str(r["Rel_Qty"] or 0))
            item["Shipped_Qty"] += Decimal(str(r["Shipped"] or 0))
            details.append({
                "Release_Key": r["Release_Key"],
                "PO_Line_Key": r["PO_Line_Key"],
                "Customer_No": r["Customer_No"],
                "PO_No": r["PO_No"],
                "Ship_Date": str(r["Ship_Date"])[:10],
                "Release_Status": state,
                "Release_Type": kind,
                "Rel_Qty": r["Rel_Qty"],
                "Shipped_Qty": r["Shipped"],
                "Scope": key[2],
            })
        summary = [{
            "Release_Status": state, "Release_Type": kind, "Scope": scope,
            "Releases": value["Releases"], "Rel_Qty": value["Rel_Qty"],
            "Shipped_Qty": value["Shipped_Qty"],
        } for (state, kind, scope), value in sorted(group.items())]
        invoice_data = rows(cursor, f"""
            SELECT d.Account_No, COUNT(*) AS Distribution_Lines,
                   SUM(d.Credit - d.Debit) AS Net_Revenue
            FROM Accounting_v_AR_Invoice AS i
            INNER JOIN Accounting_v_AR_Invoice_Dist AS d
              ON i.Invoice_Link = d.Invoice_Link
             AND i.Plexus_Customer_No = d.Plexus_Customer_No
            WHERE i.Plexus_Customer_No = {PCN}
              AND i.Invoice_Date >= '{start.isoformat()}'
              AND i.Invoice_Date < '{exclusive}'
              AND i.Void = 0
              AND d.Account_No IN ('41000-420', '41000-470')
            GROUP BY d.Account_No
            ORDER BY d.Account_No
        """)
        save(out / "release_status_detail.csv", details,
             ["Release_Key","PO_Line_Key","Customer_No","PO_No","Ship_Date",
              "Release_Status","Release_Type","Rel_Qty","Shipped_Qty","Scope"])
        save(out / "release_status_summary.csv", summary,
             ["Release_Status","Release_Type","Scope","Releases","Rel_Qty","Shipped_Qty"])
        save(out / "invoice_accounts.csv", invoice_data,
             ["Account_No","Distribution_Lines","Net_Revenue"])
        print(f"Ship_Date: {start} to {end} | releases: {len(selected)}")
        for entry in summary:
            print(f"{entry['Release_Status']:<12} {entry['Release_Type']:<14} "
                  f"{entry['Scope']:<21} releases={entry['Releases']:<5} "
                  f"quantity={entry['Rel_Qty']} shipped={entry['Shipped_Qty']}")
        invoiced = sum((Decimal(str(r["Net_Revenue"] or 0)) for r in invoice_data), Decimal(0))
        print(f"Invoice_Date: {start} to {end} | posted account net revenue: {invoiced}")
        print("Warning: invoice revenue is NOT matched to releases; do not add it to release forecast yet.")
        print(f"Read-only exports saved to {out.resolve()}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()

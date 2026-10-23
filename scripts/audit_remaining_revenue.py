"""Read-only reconciliation of Remaining Revenue against Plex release-level data.

Uses the EXACT active-release and unit-price selection of the production
/revenue-forecast route; does not independently join Sales_v_Price.
The output describes today's Plex state, NOT an historical 2026-10-21 snapshot.

Usage:
    py -3.11 scripts/audit_remaining_revenue.py --start 2026-10-01 --end 2026-10-31 --gap 20288.65

Output:
    remaining_revenue_audit/release_ledger.csv
    remaining_revenue_audit/review_candidates.csv
    remaining_revenue_audit/overlap_groups.csv
    remaining_revenue_audit/amount_matches.csv
    remaining_revenue_audit/summary.txt

Neither SQL nor this script changes any Plex or platformSSI data.
"""
import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from itertools import combinations
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

ZERO = Decimal("0")
CENTS = Decimal("0.01")
LEDGER_COLUMNS = [
    "Release_Key", "PO_Line_Key", "PO_No", "Customer_No", "Part_No_Rev",
    "Release_Type", "Rel_Status", "Ship_Date", "Rel_Qty", "Shipped",
    "Rel_Bal", "Unit_Price", "Price_Key", "Price_Issue",
    "Forecast_Revenue", "Remaining_Revenue", "Coincidence",
    "Exact_Group_Size", "Overlap_Group_Size", "Other_Release_Types",
    "EDI_Status", "EDI_Last_Outdated_Date", "EDI_Changes_After_Outdated",
    "Review_Flags",
]


def amount(value):
    return Decimal(str(value)) if value is not None else ZERO


def write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_audit(rows, reference_gap):
    """Retain all releases; flag candidates without changing their revenue."""
    release_counts = Counter(str(r["Release_Key"]) for r in rows)
    groups = defaultdict(list)
    ledger = []
    for r in rows:
        coincidence = r.get("Coincidence") or {}
        edi = r.get("EDI_Audit") or {}
        flags = []
        if release_counts[str(r["Release_Key"])] > 1:
            flags.append("REPEATED_RELEASE_KEY")
        if coincidence.get("Exact_Group_Size", 1) > 1:
            flags.append("EXACT_MATCH_CANDIDATE")
        if coincidence.get("Other_Release_Types"):
            flags.append("CROSS_TYPE_OVERLAP")
        if edi.get("Historically_Outdated"):
            flags.append("HISTORICAL_EDI_OUTDATED")
        if r.get("Remaining_Revenue") is None:
            flags.append("UNPRICED")
        if amount(r.get("Rel_Bal")) < ZERO:
            flags.append("NEGATIVE_BALANCE")

        item = {key: r.get(key) for key in LEDGER_COLUMNS}
        item.update({
            "Coincidence": coincidence.get("Classification", ""),
            "Exact_Group_Size": coincidence.get("Exact_Group_Size", 1),
            "Overlap_Group_Size": coincidence.get("Overlap_Group_Size", 1),
            "Other_Release_Types": "|".join(coincidence.get("Other_Release_Types") or []),
            "EDI_Status": edi.get("Status", "NOT_CHECKED"),
            "EDI_Last_Outdated_Date": edi.get("Last_Outdated_Change_Date", ""),
            "EDI_Changes_After_Outdated": edi.get("Events_After_Outdated", ""),
            "Review_Flags": "|".join(flags),
        })
        ledger.append(item)
        groups[(str(r["PO_Line_Key"]), str(r["Ship_Date"]))].append(item)

    overlap_groups = []
    for (po_line, ship_date), entries in groups.items():
        if len(entries) < 2:
            continue
        types = sorted({str(e["Release_Type"]) for e in entries})
        priced = [amount(e["Remaining_Revenue"]) for e in entries
                  if e["Remaining_Revenue"] is not None]
        overlap_groups.append({
            "PO_Line_Key": po_line,
            "Ship_Date": ship_date,
            "Release_Keys": "|".join(str(e["Release_Key"]) for e in entries),
            "Release_Types": "|".join(types),
            "Releases": len(entries),
            "Total_Remaining": str(sum(priced, ZERO)),
            "Potential_Extra_If_One_Only": str(
                sum(priced, ZERO) - max(priced) if priced else ZERO
            ),
            "Note": "Candidate only: multiple legitimate releases can share a PO/date",
        })
    overlap_groups.sort(
        key=lambda g: abs(amount(g["Potential_Extra_If_One_Only"]) - reference_gap)
    )
    candidates = [r for r in ledger if r["Review_Flags"] and
                  r["Review_Flags"] != "UNPRICED"]
    candidates.sort(
        key=lambda r: (abs(amount(r["Remaining_Revenue"]) - reference_gap),
                       str(r["Release_Key"]))
    )

    # Closest single- and two-release amounts for tracing the reported gap.
    # These are arithmetic leads ONLY, never evidence of invalid releases.
    priced_candidates = [
        c for c in candidates
        if c["Remaining_Revenue"] is not None and amount(c["Remaining_Revenue"]) > ZERO
    ]
    matches = []
    for group in combinations(priced_candidates, 1):
        subtotal = sum((amount(x["Remaining_Revenue"]) for x in group), ZERO)
        matches.append({
            "Release_Keys": str(group[0]["Release_Key"]),
            "Count": 1,
            "Candidate_Remaining": str(subtotal),
            "Absolute_Distance_To_Gap": str(abs(subtotal - reference_gap)),
            "Reason": group[0]["Review_Flags"],
        })
    for group in combinations(priced_candidates, 2):
        if str(group[0]["Release_Key"]) == str(group[1]["Release_Key"]):
            continue
        subtotal = sum((amount(x["Remaining_Revenue"]) for x in group), ZERO)
        matches.append({
            "Release_Keys": "|".join(str(x["Release_Key"]) for x in group),
            "Count": 2,
            "Candidate_Remaining": str(subtotal),
            "Absolute_Distance_To_Gap": str(abs(subtotal - reference_gap)),
            "Reason": "Arithmetic lead only; both releases require independent review",
        })
    matches.sort(key=lambda x: amount(x["Absolute_Distance_To_Gap"]))
    return ledger, candidates, overlap_groups, matches[:25]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default="2026-10-31")
    parser.add_argument("--customer", type=int)
    parser.add_argument("--gap", type=Decimal, default=Decimal("20288.65"),
                        help="Approximate previously observed excess; not an adjustment")
    parser.add_argument("--output", default="remaining_revenue_audit")
    args = parser.parse_args()

    from main import get_connection, query_to_list
    from revenue_forecast_router import (
        RevenueForecastRequest, make_revenue_forecast_router,
    )
    from revenue_edi_assessment import attach_edi_history

    request = RevenueForecastRequest(
        start_date=args.start, end_date=args.end, customer_no=args.customer,
        release_status="Active", debug=True,
    )
    router = make_revenue_forecast_router(get_connection, query_to_list, lambda: None)
    endpoint = next(route.endpoint for route in router.routes
                    if route.path == "/revenue-forecast")
    # Call the same production calculation directly. No test data or extra pricing.
    result = endpoint(request)
    rows = result["data"]

    edi_warning = ""
    if rows:
        conn = get_connection()
        try:
            attach_edi_history(conn.cursor(), query_to_list, rows)
        except Exception as exc:
            edi_warning = f"EDI history unavailable: {exc}"
        finally:
            conn.close()

    ledger, candidates, overlaps, matches = build_audit(rows, args.gap)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "release_ledger.csv", ledger, LEDGER_COLUMNS)
    write_csv(out / "review_candidates.csv", candidates, LEDGER_COLUMNS)
    write_csv(
        out / "overlap_groups.csv", overlaps,
        ["PO_Line_Key", "Ship_Date", "Release_Keys", "Release_Types",
         "Releases", "Total_Remaining", "Potential_Extra_If_One_Only", "Note"],
    )
    write_csv(
        out / "amount_matches.csv", matches,
        ["Release_Keys", "Count", "Candidate_Remaining",
         "Absolute_Distance_To_Gap", "Reason"],
    )

    unpriced = sum(row["Remaining_Revenue"] is None for row in ledger)
    remaining = sum((amount(r["Remaining_Revenue"]) for r in ledger), ZERO)
    report = [
        f"Generated at (computer local time): {datetime.now().astimezone().isoformat()}",
        f"Ship_Date window: {args.start} through {args.end} inclusive",
        f"Customer filter: {args.customer if args.customer is not None else 'all'}",
        f"Active releases retrieved: {len(ledger)}",
        f"Unique Release_Key values: {len({str(r['Release_Key']) for r in ledger})}",
        f"Remaining Revenue from current proxy logic: {remaining.quantize(CENTS)}",
        f"Unpriced releases (excluded from monetary sum): {unpriced}",
        f"Review-flagged releases: {len(candidates)}",
        f"Same PO-line/ship-date groups: {len(overlaps)}",
        f"Prior observed excess reference: {args.gap.quantize(CENTS)}",
        edi_warning,
        "",
        "IMPORTANT: Current Plex state is NOT an as-of-2026-10-21 snapshot.",
        "The reference amount cannot be exactly reconciled without a same-time",
        "Plex report and matching scope (dates/customer/currency).",
        "Possible duplicates, crossed release types, and historical EDI events",
        "are indicators for manual investigation, NOT authorization to delete.",
        "Never subtract candidate amounts from the KPI automatically.",
        "",
        "Nearest arithmetic matches (not confirmed duplicates):",
    ]
    report.extend(
        f"  {m['Release_Keys']}: {m['Candidate_Remaining']}"
        f" (distance {m['Absolute_Distance_To_Gap']})"
        for m in matches[:10]
    )
    (out / "summary.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    print(f"Read-only audit CSV files: {out.resolve()}")


if __name__ == "__main__":
    main()

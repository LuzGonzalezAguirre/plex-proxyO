"""Read-only release overlap detector for revenue forecast.

Coincidence is not proof of duplication. Preserve every release in official KPIs.
"""
from collections import defaultdict

def annotate_release_coincidences(rows):
    exact = defaultdict(list)
    overlap = defaultdict(list)
    for row in rows:
        exact_key = (row["PO_Line_Key"], row["Ship_Date"], row["Rel_Qty"],
                     row["Shipped"], row["Rel_Bal"], row["Release_Type"])
        overlap_key = (row["Customer_No"], row["Part_No_Rev"], row["Ship_Date"])
        exact[exact_key].append(row)
        overlap[overlap_key].append(row)
    for row in rows:
        exact_key = (row["PO_Line_Key"], row["Ship_Date"], row["Rel_Qty"],
                     row["Shipped"], row["Rel_Bal"], row["Release_Type"])
        overlap_key = (row["Customer_No"], row["Part_No_Rev"], row["Ship_Date"])
        twins = exact[exact_key]
        peers = overlap[overlap_key]
        other_types = sorted({p["Release_Type"] for p in peers
                              if p["Release_Type"] != row["Release_Type"]})
        row["Coincidence"] = {
            "Exact_Group_Size": len(twins),
            "Exact_Release_Keys": sorted(int(p["Release_Key"]) for p in twins),
            "Overlap_Group_Size": len(peers),
            "Other_Release_Types": other_types,
            "Classification": (
                "Exact-match candidate" if len(twins) > 1 else
                "Firm/Forecast overlap candidate" if other_types else "Unique in selected range"
            ),
            "Review_Required": len(twins) > 1 or bool(other_types),
            "Confirmed_Duplicate": False,
        }
    return rows

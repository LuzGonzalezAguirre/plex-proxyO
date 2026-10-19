"""Plex Delivery Performance: ODBC-safe extraction and Python attribution.

Plex DataDirect OpenAccess does not support the multi-CTE query used by common
SQL Server dialects. Fetch releases and shipper lines with simple SELECTs and
perform chronology, exact EDI pair matching, and To Promise classification in
Python.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel

PCN = 306713
PACIFIC = ZoneInfo("America/Los_Angeles")


class DeliveryRequest(BaseModel):
    start_date: date
    end_date: date


def build_release_query(window_begin: str, window_end: str) -> str:
    return f"""
SELECT r.PCN, r.PO_Line_Key, r.Release_Key, r.Release_No,
       r.Due_Date, r.Ship_Date AS Planned_Ship_Date,
       r.Quantity AS Scheduled, r.Quantity_Shipped,
       po.Customer_No, c.Name AS Customer_Name, po.PO_No,
       p.Part_No, p.Revision, p.Name AS Part_Name
FROM Sales_v_Release AS r
INNER JOIN Sales_v_PO_Line AS pol
  ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
INNER JOIN Sales_v_PO AS po
  ON pol.PO_Key = po.PO_Key AND r.PCN = po.PCN
INNER JOIN Part_v_Part AS p
  ON pol.Part_Key = p.Part_Key AND r.PCN = p.Plexus_Customer_No
LEFT JOIN Common_v_Customer AS c
  ON po.Customer_No = c.Customer_No AND po.PCN = c.Plexus_Customer_No
WHERE r.PCN = {PCN}
  AND r.Ship_Date >= '{window_begin}'
  AND r.Ship_Date < '{window_end}'
  AND r.Quantity_Shipped > 0
"""


def build_shipper_query(window_begin: str, window_end: str) -> str:
    return f"""
SELECT sl.Release_Key, sl.Shipper_Key, sl.Quantity AS Physical_Quantity,
       s.Ship_Date AS Actual_Ship_Date
FROM Sales_v_Shipper_Line AS sl
INNER JOIN Sales_v_Shipper AS s
  ON sl.Shipper_Key = s.Shipper_Key AND sl.PCN = s.PCN
INNER JOIN Sales_v_Release AS r
  ON sl.Release_Key = r.Release_Key AND sl.PCN = r.PCN
WHERE sl.PCN = {PCN}
  AND r.Ship_Date >= '{window_begin}'
  AND r.Ship_Date < '{window_end}'
  AND r.Quantity_Shipped > 0
"""


def as_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def local_ship_date(value):
    stamp = as_datetime(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(PACIFIC).date()


def attribute_delivery(releases, shipments, start_date, end_date):
    """Output one row per eligible release, matching Plex release-mean OTD.

    Exact balance pairs are limited to unique 1:1 matches on the same PO line.
    Unmatched shipment quantities remain unattributed and are not forced Late.
    """
    by_id = {int(r["Release_Key"]): dict(r) for r in releases}
    by_release = defaultdict(list)
    for index, line in enumerate(shipments):
        rid = int(line["Release_Key"])
        if rid in by_id:
            by_release[rid].append((as_datetime(line["Actual_Ship_Date"]),
                                    int(line["Shipper_Key"]), index,
                                    float(line["Physical_Quantity"] or 0)))

    balance = {}
    base_allocations = defaultdict(list)
    extra_allocations = defaultdict(list)
    for rid, row in by_id.items():
        shipped = float(row.get("Quantity_Shipped") or 0)
        lines = sorted(by_release[rid], key=lambda item: (item[0], item[1], item[2]))
        physical = sum(item[3] for item in lines)
        balance[rid] = shipped - physical
        assigned = 0.0
        for stamp, _, _, qty in lines:
            if qty <= 0:
                continue
            portion = max(0.0, min(qty, shipped - assigned))
            if portion:
                base_allocations[rid].append((stamp, portion))
                assigned += portion
            if qty > portion:
                extra_allocations[rid].append((stamp, qty - portion))

    source_candidates = defaultdict(list)
    target_candidates = defaultdict(list)
    ids_by_line = defaultdict(list)
    for rid, row in by_id.items():
        ids_by_line[(row["PCN"], row["PO_Line_Key"])].append(rid)
    for ids in ids_by_line.values():
        sources = [rid for rid in ids if balance[rid] < 0]
        targets = [rid for rid in ids if balance[rid] > 0]
        for src in sources:
            for dst in targets:
                if abs(balance[src] + balance[dst]) < 0.00001:
                    source_candidates[src].append(dst)
                    target_candidates[dst].append(src)
    for src, targets in source_candidates.items():
        if len(targets) == 1 and len(target_candidates[targets[0]]) == 1:
            base_allocations[targets[0]].extend(extra_allocations[src])

    result = []
    for rid, row in by_id.items():
        promise = as_datetime(row["Planned_Ship_Date"]).date()
        scheduled = float(row.get("Scheduled") or 0)
        if not (start_date <= promise <= end_date and scheduled > 0):
            continue
        early = on_time = late = 0.0
        for ship_stamp, qty in base_allocations[rid]:
            actual = local_ship_date(ship_stamp)
            if actual < promise:
                early += qty
            elif actual == promise:
                on_time += qty
            else:
                late += qty
        shipped = float(row.get("Quantity_Shipped") or 0)
        row.update({
            "Early": early,
            "On_Time": on_time,
            "Late": late,
            "Quantity_Difference": balance[rid],
            "Unattributed_Quantity": shipped - (early + on_time + late),
            "Delivery_Performance_Raw": 100.0 * (early + on_time) / scheduled,
        })
        result.append(row)
    return sorted(result, key=lambda row: (as_datetime(row["Planned_Ship_Date"]), row["Release_Key"]))


def make_delivery_performance_router(get_connection, query_to_list, verify_token):
    router = APIRouter()

    @router.post("/delivery-performance", dependencies=[Security(verify_token)])
    def delivery_performance(req: DeliveryRequest):
        if req.end_date < req.start_date or (req.end_date - req.start_date).days > 180:
            raise HTTPException(status_code=400, detail="Date interval must be 0-180 days")
        window_begin = (req.start_date - timedelta(days=62)).isoformat()
        window_end = (req.end_date + timedelta(days=94)).isoformat()
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(build_release_query(window_begin, window_end))
            releases = query_to_list(cursor)
            cursor.execute(build_shipper_query(window_begin, window_end))
            shipments = query_to_list(cursor)
            return {"data": attribute_delivery(releases, shipments, req.start_date, req.end_date)}
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            if conn is not None:
                conn.close()

    return router

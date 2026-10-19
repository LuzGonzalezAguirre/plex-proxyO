"""Read-only Plex delivery performance: To Promise / Volume.

One row per shipped release; source/target EDI balances are matched only when
there is a unique opposite balance on the same PO line. Shipment timestamps are
treated as UTC for the provisional Pacific display-date normalization.
"""
from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel

PCN = 306713


class DeliveryRequest(BaseModel):
    start_date: date
    end_date: date


def build_query(begin: str, end: str, window_begin: str, window_end: str) -> str:
    """Date inputs are validated ISO dates produced from Pydantic date objects."""
    return f"""
WITH ReleaseBalances AS (
    SELECT r.PCN, r.PO_Line_Key, r.Release_Key, r.Release_No,
           r.Due_Date, r.Ship_Date AS Promise_Ship_Date,
           r.Quantity AS Scheduled, r.Quantity_Shipped,
           r.Quantity_Shipped - COALESCE(SUM(sl.Quantity), 0) AS Quantity_Difference
    FROM Sales_v_Release AS r
    LEFT JOIN Sales_v_Shipper_Line AS sl
      ON r.Release_Key = sl.Release_Key AND r.PCN = sl.PCN
    WHERE r.PCN = {PCN}
      AND r.Ship_Date >= '{window_begin}'
      AND r.Ship_Date < '{window_end}'
      AND r.Quantity_Shipped > 0
    GROUP BY r.PCN, r.PO_Line_Key, r.Release_Key, r.Release_No,
             r.Due_Date, r.Ship_Date, r.Quantity, r.Quantity_Shipped
),
ShipmentSequence AS (
    SELECT b.PCN, b.Release_Key, b.Quantity_Shipped,
           sl.Shipper_Key, sl.Shipper_Line_Key, sl.Quantity AS Physical_Quantity,
           s.Ship_Date AS Actual_Ship_Date,
           SUM(sl.Quantity) OVER (
               PARTITION BY b.Release_Key
               ORDER BY s.Ship_Date, sl.Shipper_Key, sl.Shipper_Line_Key
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS Cumulative_Quantity
    FROM ReleaseBalances AS b
    INNER JOIN Sales_v_Shipper_Line AS sl
      ON b.Release_Key = sl.Release_Key AND b.PCN = sl.PCN
    INNER JOIN Sales_v_Shipper AS s
      ON sl.Shipper_Key = s.Shipper_Key AND sl.PCN = s.PCN
),
Allocation AS (
    SELECT seq.Release_Key, seq.Actual_Ship_Date, seq.Physical_Quantity,
           CASE
             WHEN seq.Cumulative_Quantity <= seq.Quantity_Shipped THEN seq.Physical_Quantity
             WHEN seq.Cumulative_Quantity - seq.Physical_Quantity >= seq.Quantity_Shipped THEN 0
             ELSE seq.Quantity_Shipped - (seq.Cumulative_Quantity - seq.Physical_Quantity)
           END AS Original_Allocation
    FROM ShipmentSequence AS seq
),
CandidatePairs AS (
    SELECT src.Release_Key AS Source_Release_Key,
           dst.Release_Key AS Target_Release_Key,
           COUNT(*) OVER (PARTITION BY src.Release_Key) AS Source_Matches,
           COUNT(*) OVER (PARTITION BY dst.Release_Key) AS Target_Matches
    FROM ReleaseBalances AS src
    INNER JOIN ReleaseBalances AS dst
      ON src.PCN = dst.PCN
     AND src.PO_Line_Key = dst.PO_Line_Key
     AND src.Release_Key <> dst.Release_Key
     AND src.Quantity_Difference < 0
     AND dst.Quantity_Difference > 0
     AND src.Quantity_Difference + dst.Quantity_Difference = 0
),
ExactPairs AS (
    SELECT pair.Source_Release_Key, pair.Target_Release_Key
    FROM CandidatePairs AS pair
    WHERE pair.Source_Matches = 1 AND pair.Target_Matches = 1
),
AttributedShipments AS (
    SELECT a.Release_Key AS Target_Release_Key, a.Actual_Ship_Date,
           a.Original_Allocation AS Allocated_Quantity
    FROM Allocation AS a
    WHERE a.Original_Allocation > 0
    UNION ALL
    SELECT pair.Target_Release_Key, a.Actual_Ship_Date,
           a.Physical_Quantity - a.Original_Allocation AS Allocated_Quantity
    FROM Allocation AS a
    INNER JOIN ExactPairs AS pair ON a.Release_Key = pair.Source_Release_Key
    WHERE a.Physical_Quantity > a.Original_Allocation
),
NormalizedShipments AS (
    SELECT a.Target_Release_Key, a.Allocated_Quantity,
           CASE
             WHEN a.Actual_Ship_Date >= '2026-03-08 10:00:00'
              AND a.Actual_Ship_Date < '2026-11-01 09:00:00'
             THEN DATEADD(hour, -7, a.Actual_Ship_Date)
             ELSE DATEADD(hour, -8, a.Actual_Ship_Date)
           END AS Local_Actual_Ship_Date
    FROM AttributedShipments AS a
),
DeliveryTotals AS (
    SELECT b.PCN, b.PO_Line_Key, b.Release_Key, b.Release_No, b.Due_Date,
           b.Promise_Ship_Date, b.Scheduled, b.Quantity_Shipped,
           b.Quantity_Difference,
           COALESCE(SUM(CASE WHEN CAST(a.Local_Actual_Ship_Date AS DATE)
             < CAST(b.Promise_Ship_Date AS DATE) THEN a.Allocated_Quantity ELSE 0 END), 0) AS Early,
           COALESCE(SUM(CASE WHEN CAST(a.Local_Actual_Ship_Date AS DATE)
             = CAST(b.Promise_Ship_Date AS DATE) THEN a.Allocated_Quantity ELSE 0 END), 0) AS On_Time,
           COALESCE(SUM(CASE WHEN CAST(a.Local_Actual_Ship_Date AS DATE)
             > CAST(b.Promise_Ship_Date AS DATE) THEN a.Allocated_Quantity ELSE 0 END), 0) AS Late
    FROM ReleaseBalances AS b
    LEFT JOIN NormalizedShipments AS a ON b.Release_Key = a.Target_Release_Key
    WHERE b.Promise_Ship_Date >= '{begin}' AND b.Promise_Ship_Date < '{end}'
      AND b.Scheduled > 0
    GROUP BY b.PCN, b.PO_Line_Key, b.Release_Key, b.Release_No, b.Due_Date,
             b.Promise_Ship_Date, b.Scheduled, b.Quantity_Shipped, b.Quantity_Difference
)
SELECT t.Release_Key, t.Release_No, t.PO_Line_Key,
       t.Promise_Ship_Date AS Planned_Ship_Date, t.Due_Date,
       t.Scheduled, t.Quantity_Shipped,
       t.Early, t.On_Time, t.Late, t.Quantity_Difference,
       t.Quantity_Shipped - (t.Early + t.On_Time + t.Late) AS Unattributed_Quantity,
       100.0 * (t.Early + t.On_Time) / t.Scheduled AS Delivery_Performance_Raw,
       po.Customer_No, c.Name AS Customer_Name, po.PO_No,
       p.Part_No, p.Revision, p.Name AS Part_Name
FROM DeliveryTotals AS t
INNER JOIN Sales_v_PO_Line AS pol
  ON t.PO_Line_Key = pol.PO_Line_Key AND t.PCN = pol.PCN
INNER JOIN Sales_v_PO AS po
  ON pol.PO_Key = po.PO_Key AND t.PCN = po.PCN
INNER JOIN Part_v_Part AS p
  ON pol.Part_Key = p.Part_Key AND t.PCN = p.Plexus_Customer_No
LEFT JOIN Common_v_Customer AS c
  ON po.Customer_No = c.Customer_No AND po.PCN = c.Plexus_Customer_No
ORDER BY t.Promise_Ship_Date, t.Release_Key
"""


def make_delivery_performance_router(get_connection, query_to_list, verify_token):
    router = APIRouter()

    @router.post("/delivery-performance", dependencies=[Security(verify_token)])
    def delivery_performance(req: DeliveryRequest):
        if req.end_date < req.start_date or (req.end_date - req.start_date).days > 180:
            raise HTTPException(status_code=400, detail="Date interval must be 0-180 days")
        begin = req.start_date.isoformat()
        end = (req.end_date + timedelta(days=1)).isoformat()
        # Extra releases on either side allow EDI quantities to cross the selected interval.
        window_begin = (req.start_date - timedelta(days=62)).isoformat()
        window_end = (req.end_date + timedelta(days=93)).isoformat()
        query = build_query(begin, end, window_begin, window_end)
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            return {"data": query_to_list(cursor)}
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            if conn is not None:
                conn.close()

    return router

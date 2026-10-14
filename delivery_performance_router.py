"""Read-only Plex delivery release and shipper-line extraction.

The join intentionally returns one row per shipment line (or one unsent release).
Do not aggregate scheduled quantities before grouping by Release_Key.
"""
from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel

PCN = 306713

class DeliveryRequest(BaseModel):
    start_date: date
    end_date: date

def make_delivery_performance_router(get_connection, query_to_list, verify_token):
    router = APIRouter()

    @router.post("/delivery-performance", dependencies=[Security(verify_token)])
    def delivery_performance(req: DeliveryRequest):
        if req.end_date < req.start_date or (req.end_date - req.start_date).days > 180:
            raise HTTPException(status_code=400, detail="Date interval must be 0-180 days")
        begin = req.start_date.isoformat()
        end = (req.end_date + timedelta(days=1)).isoformat()
        query = f"""
            SELECT
                r.Release_Key, r.Release_No, r.PO_Line_Key,
                r.Ship_Date AS Planned_Ship_Date, r.Due_Date,
                r.Quantity AS Scheduled, r.Quantity_Shipped,
                r.Note AS Release_Note,
                rs.Release_Status, po.Customer_No, po.PO_No,
                p.Part_No, p.Revision, p.Name AS Part_Name,
                sl.Shipper_Line_Key, sl.Quantity AS Shipped_Quantity,
                s.Shipper_No, s.Ship_Date AS Actual_Ship_Date,
                s.Shipper_Status_Key
            FROM Sales_v_Release AS r
            INNER JOIN Sales_v_PO_Line AS pol
                ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
            INNER JOIN Sales_v_PO AS po
                ON pol.PO_Key = po.PO_Key AND r.PCN = po.PCN
            INNER JOIN Part_v_Part AS p
                ON pol.Part_Key = p.Part_Key AND r.PCN = p.Plexus_Customer_No
            LEFT JOIN Sales_v_Release_Status AS rs
                ON r.Release_Status_Key = rs.Release_Status_Key AND r.PCN = rs.PCN
            LEFT JOIN Sales_v_Shipper_Line AS sl
                ON r.Release_Key = sl.Release_Key AND r.PCN = sl.PCN
            LEFT JOIN Sales_v_Shipper AS s
                ON sl.Shipper_Key = s.Shipper_Key AND sl.PCN = s.PCN
            WHERE r.PCN = {PCN}
              AND r.Due_Date >= '{begin}'
              AND r.Due_Date < '{end}'
              AND r.Quantity > 0
              AND (rs.Release_Status = 'Open' OR r.Quantity_Shipped > 0)
            ORDER BY r.Release_Key, sl.Shipper_Line_Key
        """
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            return {"data": query_to_list(cursor)}
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc))
        finally:
            if conn is not None:
                conn.close()

    return router

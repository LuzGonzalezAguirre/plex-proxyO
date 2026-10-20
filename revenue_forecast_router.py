"""Revenue forecast from Plex customer releases and PO-line prices.

A price must be valid for the release ship date. Do not multiply releases by
all price records: choose precisely one applicable price per PO line/release.
"""
from datetime import datetime, timedelta
from decimal import Decimal
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel

class RevenueForecastRequest(BaseModel):
    start_date: str
    end_date: str
    customer_no: int | None = None
    release_status: str = "All"

def make_revenue_forecast_router(get_connection, query_to_list, verify_token, pcn=306713):
    router = APIRouter()

    @router.post("/revenue-forecast", dependencies=[Security(verify_token)])
    def revenue_forecast(req: RevenueForecastRequest):
        try:
            start = datetime.strptime(req.start_date, "%Y-%m-%d")
            end = datetime.strptime(req.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Dates must use YYYY-MM-DD")
        if end < start or (end - start).days > 180:
            raise HTTPException(status_code=400, detail="Date range must be 0-180 days")
        if req.release_status not in ("All", "Open", "History"):
            raise HTTPException(status_code=400, detail="Invalid release status")

        customer_filter = (f"AND po.Customer_No = {int(req.customer_no)}"
                           if req.customer_no is not None else "")
        status_filter = ("AND rs.Release_Status = 'Open'" if req.release_status == "Open"
                         else "AND rs.Release_Status IN ('Closed', 'Shipped')"
                         if req.release_status == "History" else "")
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT r.Release_Key, r.PO_Line_Key, pol.Part_Key,
                       po.Customer_No, ISNULL(po.PO_No, '') AS PO_No,
                       ISNULL(p.Part_No, '') AS Part_No,
                       ISNULL(p.Revision, '') AS Revision,
                       ISNULL(rt.Release_Type, 'Firm (862)') AS Release_Type,
                       r.Ship_Date, r.Quantity AS Rel_Qty,
                       ISNULL(r.Quantity_Shipped, 0) AS Shipped,
                       r.Quantity - ISNULL(r.Quantity_Shipped, 0) AS Rel_Bal,
                       ISNULL(rs.Release_Status, '') AS Rel_Status,
                       pol.Price_Conversion
                FROM Sales_v_Release AS r
                INNER JOIN Sales_v_PO_Line AS pol
                  ON pol.PO_Line_Key = r.PO_Line_Key AND pol.PCN = r.PCN
                INNER JOIN Sales_v_PO AS po
                  ON po.PO_Key = pol.PO_Key AND po.PCN = pol.PCN
                LEFT JOIN Part_v_Part AS p
                  ON p.Part_Key = pol.Part_Key AND p.Plexus_Customer_No = pol.PCN
                LEFT JOIN Sales_v_Release_Type AS rt
                  ON rt.Release_Type_Key = r.Release_Type_Key AND rt.PCN = r.PCN
                LEFT JOIN Sales_v_Release_Status AS rs
                  ON rs.Release_Status_Key = r.Release_Status_Key AND rs.PCN = r.PCN
                WHERE r.PCN = {pcn}
                  {customer_filter}
                  {status_filter}
                ORDER BY r.Ship_Date, r.Release_Key
            """)
            releases = query_to_list(cursor)
            selected = [r for r in releases if r["Ship_Date"] is not None
                        and req.start_date <= str(r["Ship_Date"])[:10] <= req.end_date]
            if not selected:
                return {"data": []}

            po_keys = sorted({int(r["PO_Line_Key"]) for r in selected
                              if r.get("PO_Line_Key") is not None})
            prices = {}
            for offset in range(0, len(po_keys), 80):
                key_list = ", ".join(map(str, po_keys[offset:offset + 80]))
                cursor.execute(f"""
                    SELECT PO_Line_Key, Price_Key, Price, Active,
                           Effective_Date, Expiration_Date,
                           Breakpoint_Quantity, Currency_Key, Unit,
                           Primary_Price
                    FROM Sales_v_Price
                    WHERE PCN = {pcn} AND PO_Line_Key IN ({key_list})
                """)
                for row in query_to_list(cursor):
                    prices.setdefault(int(row["PO_Line_Key"]), []).append(row)

            result = []
            for row in selected:
                ship_date = str(row["Ship_Date"])[:10]
                quantity = Decimal(str(row["Rel_Qty"] or 0))
                balance = Decimal(str(row["Rel_Bal"] or 0))
                applicable = []
                for price in prices.get(int(row["PO_Line_Key"]), []):
                    if price["Active"] != 1 or price["Price"] is None:
                        continue
                    if price["Effective_Date"] and str(price["Effective_Date"])[:10] > ship_date:
                        continue
                    if price["Expiration_Date"] and str(price["Expiration_Date"])[:10] < ship_date:
                        continue
                    # A breakpoint price requires validated tier semantics.
                    if Decimal(str(price["Breakpoint_Quantity"] or 0)) != 0:
                        continue
                    applicable.append(price)
                applicable.sort(key=lambda p: (
                    str(p["Effective_Date"] or ""),
                    int(p["Primary_Price"] or 0),
                    int(p["Price_Key"])
                ), reverse=True)
                chosen = applicable[0] if applicable else None
                # Preserve the native PO price without silently converting units.
                # Non-unitary Price_Conversion is flagged as unresolved.
                conversion = Decimal(str(row["Price_Conversion"] or 1))
                currency = int(chosen["Currency_Key"] or 0) if chosen else None
                valid = chosen is not None and conversion == 1 and currency == 0
                unit_price = Decimal(str(chosen["Price"])) if valid else None
                result.append({
                    "Release_Key": int(row["Release_Key"]),
                    "PO_Line_Key": int(row["PO_Line_Key"]),
                    "Customer_No": row["Customer_No"],
                    "PO_No": row["PO_No"],
                    "Part_No_Rev": (row["Part_No"] + " Rev:" + row["Revision"]),
                    "Release_Type": row["Release_Type"],
                    "Ship_Date": ship_date,
                    "Rel_Status": row["Rel_Status"],
                    "Rel_Qty": float(quantity),
                    "Shipped": float(row["Shipped"] or 0),
                    "Rel_Bal": float(balance),
                    "Unit_Price": float(unit_price) if valid else None,
                    "Forecast_Revenue": float(quantity * unit_price) if valid else None,
                    "Remaining_Revenue": float(balance * unit_price) if valid else None,
                    "Price_Key": int(chosen["Price_Key"]) if chosen else None,
                    "Price_Source": "PO" if valid else "Unresolved",
                    "Price_Issue": (None if valid else
                        "No applicable PO price" if chosen is None else
                        "Price conversion or currency requires verification"),
                })
            return {"data": result}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            if conn is not None:
                conn.close()

    return router

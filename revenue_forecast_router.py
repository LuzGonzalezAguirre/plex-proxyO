"""Revenue forecast from Plex customer releases and PO-line prices.

A price must be valid for the release ship date. Do not multiply releases by
all price records: choose precisely one applicable price per PO line/release.
"""
from datetime import datetime, timedelta
from decimal import Decimal
from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel
from revenue_release_audit import annotate_release_coincidences

class RevenueForecastRequest(BaseModel):
    start_date: str
    end_date: str
    customer_no: int | None = None
    release_status: str = "Active"
    debug: bool = False

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
        if req.release_status not in ("Active", "All", "Open", "LegacyAll"):
            raise HTTPException(status_code=400, detail="Revenue Forecast only supports active releases")

        customer_filter = (f"AND po.Customer_No = {int(req.customer_no)}"
                           if req.customer_no is not None else "")
        # Revenue Forecast uses active demand only, irrespective of legacy status.
        # Explicitly exclude historical, shipped and cancelled releases.
        status_filter = ("" if req.release_status == "LegacyAll" and req.debug else
                         "AND rs.Release_Status IN ('Open', 'Scheduled')"
                         " AND rt.Release_Type IN ('Firm (862)', 'Forecast', 'HOLD')")
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
                        and req.start_date <= str(r["Ship_Date"])[:10] <= req.end_date
                        and (req.release_status == "LegacyAll" and req.debug or (
                            str(r["Rel_Status"] or "").strip().lower() in ("open", "scheduled")
                            and str(r["Release_Type"] or "").strip().lower() in ("firm (862)", "forecast", "hold")))]
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
                candidate_debug = []
                for price in prices.get(int(row["PO_Line_Key"]), []):
                    reasons = []
                    if price["Active"] != 1:
                        reasons.append("inactive")
                    if price["Price"] is None:
                        reasons.append("missing_price")
                    if price["Effective_Date"] and str(price["Effective_Date"])[:10] > ship_date:
                        reasons.append("not_yet_effective")
                    if price["Expiration_Date"] and str(price["Expiration_Date"])[:10] < ship_date:
                        reasons.append("expired")
                    if Decimal(str(price["Breakpoint_Quantity"] or 0)) != 0:
                        reasons.append("tier_not_supported")
                    if req.debug:
                        candidate_debug.append({
                            "Price_Key": price["Price_Key"],
                            "Price": float(price["Price"]) if price["Price"] is not None else None,
                            "Active": price["Active"],
                            "Effective_Date": price["Effective_Date"],
                            "Expiration_Date": price["Expiration_Date"],
                            "Breakpoint_Quantity": price["Breakpoint_Quantity"],
                            "Unit": price["Unit"],
                            "Currency_Key": price["Currency_Key"],
                            "Primary_Price": price["Primary_Price"],
                            "Selection_Status": "eligible" if not reasons else "excluded",
                            "Exclusion_Reasons": reasons,
                        })
                    if not reasons:
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
                price_unit = str(chosen["Unit"] or "").strip().lower() if chosen else ""
                valid = (chosen is not None and conversion == 1 and currency == 0
                         and price_unit in ("ea", "each"))
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
                        "Price unit, conversion or currency requires verification"),
                    **({"Debug": {
                        "Field_Sources": {
                            "Release_Key": "Sales_v_Release.Release_Key",
                            "PO_Line_Key": "Sales_v_Release.PO_Line_Key",
                            "Part_Key": "Sales_v_PO_Line.Part_Key",
                            "Customer_No": "Sales_v_PO.Customer_No",
                            "PO_No": "Sales_v_PO.PO_No",
                            "Part_No_Rev": "Part_v_Part.Part_No + Revision",
                            "Release_Type": "Sales_v_Release_Type.Release_Type",
                            "Ship_Date": "Sales_v_Release.Ship_Date",
                            "Rel_Qty": "Sales_v_Release.Quantity",
                            "Shipped": "Sales_v_Release.Quantity_Shipped",
                            "Rel_Bal": "Quantity - Quantity_Shipped",
                            "Unit_Price": "Sales_v_Price.Price",
                            "Price_Key": "Sales_v_Price.Price_Key",
                            "Price_Conversion": "Sales_v_PO_Line.Price_Conversion",
                        },
                        "Price_Conversion_Raw": row["Price_Conversion"],
                        "Price_Conversion_Applied": float(conversion),
                        "Currency_Key": currency,
                        "Price_Unit": chosen["Unit"] if chosen else None,
                        "Candidate_Count": len(candidate_debug),
                        "Eligible_Count": len(applicable),
                        "Candidates": candidate_debug,
                        "Selected_Price_Key": chosen["Price_Key"] if chosen else None,
                        "Selection_Rule": "Active, effective <= ship date, unexpired, zero breakpoint; newest effective date, primary, price key",
                        "Forecast_Formula": "Sales_v_Release.Quantity * Sales_v_Price.Price",
                        "Remaining_Formula": "(Quantity - Quantity_Shipped) * Sales_v_Price.Price",
                        "Revenue_Price_Validated": valid,
                    }} if req.debug else {}),
                })
            if req.debug:
                annotate_release_coincidences(result)
            return {"data": result}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            if conn is not None:
                conn.close()

    @router.post("/shipping-revenue-summary", dependencies=[Security(verify_token)])
    def shipping_revenue_summary(req: RevenueForecastRequest):
        try:
            start = datetime.strptime(req.start_date, "%Y-%m-%d")
            end = datetime.strptime(req.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Dates must use YYYY-MM-DD")
        if end < start or (end - start).days > 180:
            raise HTTPException(status_code=400, detail="Date range must be 0-180 days")
        exclusive = (end + timedelta(days=1)).strftime("%Y-%m-%d")
        customer_clause = (f"AND i.Customer_No = {int(req.customer_no)}"
                           if req.customer_no is not None else "")
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT d.Account_No, COUNT(*) AS Total_Lines,
                       SUM(d.Credit - d.Debit) AS Revenue
                FROM Accounting_v_AR_Invoice AS i
                INNER JOIN Accounting_v_AR_Invoice_Dist AS d
                  ON i.Invoice_Link = d.Invoice_Link
                  AND i.Plexus_Customer_No = d.Plexus_Customer_No
                WHERE i.Plexus_Customer_No = {pcn}
                  AND i.Invoice_Date >= '{req.start_date}'
                  AND i.Invoice_Date < '{exclusive}'
                  AND i.Void = 0
                  AND d.Account_No IN ('41000-420', '41000-470')
                  {customer_clause}
                GROUP BY d.Account_No ORDER BY d.Account_No
            """)
            return {"data": query_to_list(cursor)}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            if conn is not None:
                conn.close()

    @router.post("/shipping-revenue-daily", dependencies=[Security(verify_token)])
    def shipping_revenue_daily(req: RevenueForecastRequest):
        """Invoice revenue by invoice date; totals reconcile with the account summary."""
        try:
            start = datetime.strptime(req.start_date, "%Y-%m-%d")
            end = datetime.strptime(req.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Dates must use YYYY-MM-DD")
        if end < start or (end - start).days > 180:
            raise HTTPException(status_code=400, detail="Date range must be 0-180 days")
        exclusive = (end + timedelta(days=1)).strftime("%Y-%m-%d")
        customer_clause = (f"AND i.Customer_No = {int(req.customer_no)}"
                           if req.customer_no is not None else "")
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT CAST(i.Invoice_Date AS DATE) AS Invoice_Date,
                       d.Account_No,
                       COUNT(*) AS Total_Lines,
                       SUM(d.Credit - d.Debit) AS Revenue
                FROM Accounting_v_AR_Invoice AS i
                INNER JOIN Accounting_v_AR_Invoice_Dist AS d
                  ON i.Invoice_Link = d.Invoice_Link
                  AND i.Plexus_Customer_No = d.Plexus_Customer_No
                WHERE i.Plexus_Customer_No = {pcn}
                  AND i.Invoice_Date >= '{req.start_date}'
                  AND i.Invoice_Date < '{exclusive}'
                  AND i.Void = 0
                  AND d.Account_No IN ('41000-420', '41000-470')
                  {customer_clause}
                GROUP BY CAST(i.Invoice_Date AS DATE), d.Account_No
                ORDER BY Invoice_Date, d.Account_No
            """)
            return {"data": query_to_list(cursor)}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            if conn is not None:
                conn.close()

    return router

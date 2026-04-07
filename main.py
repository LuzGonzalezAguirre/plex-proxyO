import os
import pyodbc
import pandas as pd
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

DSN      = os.getenv("PLEX_DSN")
USERNAME = os.getenv("PLEX_USERNAME")
PASSWORD = os.getenv("PLEX_PASSWORD")
SECRET   = os.getenv("PROXY_SECRET")
PCN      = 306713

app = FastAPI(title="Plex ODBC Proxy", version="1.0.0")
security = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.credentials != SECRET:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials


def get_connection():
    conn_str = (
        f"DSN={DSN};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=60)


def query_to_list(cursor) -> list[dict]:
    if not cursor.description:
        return []
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    result = []
    for row in rows:
        record = {}
        for i, col in enumerate(cols):
            val = row[i]
            if hasattr(val, 'isoformat'):
                val = val.isoformat()
            record[col] = val
        result.append(record)
    return result


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        return {"status": "ok", "plex": "connected", "pcn": PCN}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ─── Part revisions ───────────────────────────────────────────────────────────

class PartRevisionsRequest(BaseModel):
    part_no: str


@app.post("/part-revisions", dependencies=[Security(verify_token)])
def part_revisions(req: PartRevisionsRequest):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT Part_No, Revision, Name AS Part_Name
            FROM Part_v_Part
            WHERE Plexus_Customer_No = {PCN}
              AND Part_No = '{req.part_no}'
            ORDER BY Revision DESC
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── BOM Hierarchy ────────────────────────────────────────────────────────────

class BomRequest(BaseModel):
    part_no: str
    revision: str
    max_levels: int = 10


@app.post("/bom-hierarchy", dependencies=[Security(verify_token)])
def bom_hierarchy(req: BomRequest):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT Part_Key, Part_No, Revision, Name
            FROM Part_v_Part
            WHERE Plexus_Customer_No = {PCN}
              AND Part_No   = '{req.part_no}'
              AND Revision  = '{req.revision}'
        """)
        root_row = cursor.fetchone()
        if not root_row:
            conn.close()
            return {"data": []}

        root_key, root_no, root_rev, root_name = root_row[0], root_row[1], root_row[2], root_row[3]
        rows_collected = []
        current_level  = [(root_key, root_no, root_rev, root_name, root_no)]

        for level in range(1, req.max_levels + 1):
            if not current_level:
                break
            parent_keys = [str(item[0]) for item in current_level]
            in_clause   = f"({', '.join(parent_keys)})"

            cursor.execute(f"""
                SELECT b.Part_Key, b.Sort_Order, b.Quantity, b.Note,
                       cp.Part_Key, cp.Part_No, cp.Revision, cp.Name, cp.Unit
                FROM Part_v_BOM b
                INNER JOIN Part_v_Part cp
                    ON b.Component_Part_Key = cp.Part_Key
                   AND b.Plexus_Customer_No = cp.Plexus_Customer_No
                WHERE b.Plexus_Customer_No = {PCN}
                  AND b.Active   = 1
                  AND b.Part_Key IN {in_clause}
                ORDER BY b.Part_Key, b.Sort_Order, cp.Part_No
            """)
            child_rows = cursor.fetchall()
            if not child_rows:
                break

            parent_map = {item[0]: item for item in current_level}
            next_level = []

            for cr in child_rows:
                parent_key = cr[0]
                bom_qty    = float(cr[2]) if cr[2] is not None else 0.0
                note       = cr[3] or ''
                comp_key   = cr[4]
                comp_no    = cr[5]
                comp_rev   = cr[6]
                comp_name  = cr[7]
                comp_unit  = cr[8] or ''

                parent_info = parent_map.get(parent_key)
                parent_path = parent_info[4] if parent_info else ''
                bom_path    = f"{parent_path} > {comp_no}"

                rows_collected.append({
                    "level":              level,
                    "original_part_no":   f"{root_no} Rev:{root_rev}",
                    "original_part_name": root_name,
                    "part_no_rev":        f"{comp_no} Rev:{comp_rev}",
                    "part_name":          comp_name,
                    "quantity":           round(bom_qty, 6),
                    "unit":               comp_unit,
                    "note":               note,
                    "bom_path":           bom_path,
                })

                already_seen = any(item[0] == comp_key for item in next_level)
                if not already_seen:
                    next_level.append((comp_key, comp_no, comp_rev, comp_name, bom_path))

            current_level = next_level

        conn.close()
        return {"data": rows_collected}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── BOM CTB (Clear To Build) ─────────────────────────────────────────────────

class BomCtbRequest(BaseModel):
    part_no: str
    revision: str
    need: int = 500
    max_levels: int = 10


@app.post("/bom-ctb", dependencies=[Security(verify_token)])
def bom_ctb(req: BomCtbRequest):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT Part_Key, Part_No, Revision, Name
            FROM Part_v_Part
            WHERE Plexus_Customer_No = {PCN}
              AND Part_No  = '{req.part_no}'
              AND Revision = '{req.revision}'
        """)
        root_row = cursor.fetchone()
        if not root_row:
            conn.close()
            return {"data": []}

        root_key, root_no, root_rev, root_name = root_row[0], root_row[1], root_row[2], root_row[3]
        rows_collected = []
        seen_keys      = set()
        current_level  = [(root_key, root_no, root_rev, root_name, root_no, 1.0)]

        for level in range(1, req.max_levels + 1):
            if not current_level:
                break
            parent_keys = [str(item[0]) for item in current_level]
            in_clause   = f"({', '.join(parent_keys)})"

            cursor.execute(f"""
                SELECT b.Part_Key, b.Sort_Order, b.Quantity, b.Note,
                       cp.Part_Key, cp.Part_No, cp.Revision, cp.Name, cp.Unit
                FROM Part_v_BOM b
                INNER JOIN Part_v_Part cp
                    ON b.Component_Part_Key = cp.Part_Key
                   AND b.Plexus_Customer_No = cp.Plexus_Customer_No
                WHERE b.Plexus_Customer_No = {PCN}
                  AND b.Active   = 1
                  AND b.Part_Key IN {in_clause}
                ORDER BY b.Part_Key, b.Sort_Order, cp.Part_No
            """)
            child_rows = cursor.fetchall()
            if not child_rows:
                break

            parent_map = {item[0]: item for item in current_level}
            next_level = []

            for cr in child_rows:
                parent_key  = cr[0]
                bom_qty     = float(cr[2]) if cr[2] is not None else 0.0
                note        = cr[3] or ''
                comp_key    = cr[4]
                comp_no     = cr[5]
                comp_rev    = cr[6]
                comp_name   = cr[7]
                comp_unit   = cr[8] or ''

                parent_info    = parent_map.get(parent_key)
                parent_path    = parent_info[4] if parent_info else ''
                parent_qty_acc = parent_info[5] if parent_info else 1.0
                bom_path       = f"{parent_path} > {comp_no}"
                qty_accumulated = bom_qty * parent_qty_acc

                rows_collected.append({
                    "_comp_key": comp_key,
                    "_qty_acc":  qty_accumulated,
                    "level":             level,
                    "root_part_no_rev":  f"{root_no} Rev:{root_rev}",
                    "part_no_rev":       f"{comp_no} Rev:{comp_rev}",
                    "part_name":         comp_name,
                    "bom_qty":           round(bom_qty, 6),
                    "unit":              comp_unit,
                    "note":              note,
                    "bom_path":          bom_path,
                })

                if comp_key not in seen_keys:
                    seen_keys.add(comp_key)
                    next_level.append((comp_key, comp_no, comp_rev, comp_name, bom_path, qty_accumulated))

            current_level = next_level

        if not rows_collected:
            conn.close()
            return {"data": []}

        # Inventario — una sola query para todos los componentes
        all_comp_keys = list({row["_comp_key"] for row in rows_collected})
        inv_in_clause = f"({', '.join(str(k) for k in all_comp_keys)})"

        cursor.execute(f"""
            SELECT c.Part_Key,
                   SUM(c.Quantity) AS Total_Qty,
                   SUM(CASE WHEN c.Location NOT LIKE '%TJR%' OR c.Location IS NULL
                            THEN c.Quantity ELSE 0 END) AS WIP,
                   SUM(CASE WHEN c.Location LIKE '%TJR%'
                            THEN c.Quantity ELSE 0 END) AS INV
            FROM Part_v_Container c
            WHERE c.Plexus_Customer_No = {PCN}
              AND c.Quantity > 0
              AND c.Active   = 1
              AND c.Part_Key IN {inv_in_clause}
            GROUP BY c.Part_Key
        """)
        inv_rows = cursor.fetchall()
        conn.close()

        inv_map = {}
        for ir in inv_rows:
            inv_map[ir[0]] = {
                "total": float(ir[1]) if ir[1] else 0.0,
                "wip":   float(ir[2]) if ir[2] else 0.0,
                "inv":   float(ir[3]) if ir[3] else 0.0,
            }

        final_rows = []
        for row in rows_collected:
            comp_key  = row["_comp_key"]
            qty_acc   = row["_qty_acc"]
            inv       = inv_map.get(comp_key, {"total": 0.0, "wip": 0.0, "inv": 0.0})
            ohymv     = round(qty_acc * req.need, 2)
            ohnv      = inv["total"]
            note_low  = (row.get("note") or "").lower()
            is_virtual = "phantom" in note_low or "embedded" in note_low
            ctb       = "Yes" if (is_virtual or ohnv >= ohymv) else "No"

            final_rows.append({
                "level":           row["level"],
                "root_part_no_rev": row["root_part_no_rev"],
                "part_no_rev":     row["part_no_rev"],
                "part_name":       row["part_name"],
                "bom_qty":         row["bom_qty"],
                "unit":            row["unit"],
                "need":            req.need,
                "ohymv":           ohymv,
                "wip":             round(inv["wip"], 2),
                "inv":             round(inv["inv"], 2),
                "ohnv":            round(ohnv, 2),
                "ctb":             ctb,
                "bom_path":        row["bom_path"],
                "note":            row["note"],
            })

        final_rows.sort(key=lambda x: x["bom_path"])
        return {"data": final_rows}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Demand ───────────────────────────────────────────────────────────────────

class DemandRequest(BaseModel):
    customer_no: Optional[int] = None
    release_status: str = "Open"


CUSTOMER_CASE = """
    CASE
        WHEN po.Customer_No = 332159 THEN 'Autocar'
        WHEN po.Customer_No IN (768299, 768300) THEN 'Capacity'
        WHEN po.Customer_No = 338766 THEN 'Claas'
        WHEN po.Customer_No = 332165 THEN 'Cummins'
        WHEN po.Customer_No = 332169 THEN 'Elkamet'
        WHEN po.Customer_No = 332170 THEN 'Elkhart'
        WHEN po.Customer_No = 773112 THEN 'Girtz'
        WHEN po.Customer_No = 332183 THEN 'JLG'
        WHEN po.Customer_No = 332185 THEN 'Kautex'
        WHEN po.Customer_No = 332205 THEN 'SSI-Plainfield'
        WHEN po.Customer_No = 332211 THEN 'Volvo'
        ELSE 'Customer ' + CAST(po.Customer_No AS VARCHAR)
    END
"""

ALL_CUSTOMERS = "(332159, 768299, 768300, 338766, 332165, 332169, 332170, 773112, 332183, 332185, 332205, 332211)"
MULTI_CUSTOMER = {768299: (768299, 768300)}


@app.post("/demand", dependencies=[Security(verify_token)])
def demand(req: DemandRequest):
    try:
        if req.customer_no is None:
            customer_filter = f"AND po.Customer_No IN {ALL_CUSTOMERS}"
        elif req.customer_no in MULTI_CUSTOMER:
            nos = ', '.join(str(n) for n in MULTI_CUSTOMER[req.customer_no])
            customer_filter = f"AND po.Customer_No IN ({nos})"
        else:
            customer_filter = f"AND po.Customer_No = {req.customer_no}"

        if req.release_status == "Open":
            status_filter  = "AND rs.Release_Status = 'Open'"
            balance_filter = "AND (r.Quantity - ISNULL(r.Quantity_Shipped, 0)) > 0"
            order_dir      = "ASC"
        elif req.release_status == "History":
            status_filter  = "AND rs.Release_Status IN ('Closed', 'Shipped')"
            balance_filter = ""
            order_dir      = "DESC"
        else:
            status_filter  = ""
            balance_filter = ""
            order_dir      = "ASC"

        query = f"""
            SELECT
                {CUSTOMER_CASE}                                         AS Customer,
                ISNULL(po.PO_No, '')                                    AS PO_Rel,
                ISNULL(pos.PO_Status, 'Open')                           AS PO_Status,
                ISNULL(r.Ship_To, '')                                   AS Ship_To_Carrier,
                p.Part_No + '.' + p.Revision                           AS Part_No_Rev,
                ISNULL(cp.Customer_Part_No, '')                         AS Cust_Part,
                r.Quantity - ISNULL(r.Quantity_Shipped, 0)             AS Qty_Ready,
                ISNULL(wip.WIP_Quantity, 0)                             AS Qty_WIP,
                r.Ship_Date                                             AS Ship_Date,
                r.Due_Date                                              AS Due_Date,
                r.Quantity                                              AS Rel_Qty,
                ISNULL(r.Quantity_Shipped, 0)                           AS Shipped,
                r.Quantity - ISNULL(r.Quantity_Shipped, 0)             AS Rel_Bal,
                ISNULL(rs.Release_Status, 'Open')                       AS Rel_Status,
                ISNULL(rt.Release_Type, 'Firm (862)')                   AS Rel_Type
            FROM Sales_v_Release AS r
            LEFT JOIN Sales_v_PO_Line AS pol
                ON r.PO_Line_Key = pol.PO_Line_Key AND r.PCN = pol.PCN
            LEFT JOIN Sales_v_PO AS po
                ON pol.PO_Key = po.PO_Key AND r.PCN = po.PCN
            LEFT JOIN Sales_v_PO_Status AS pos
                ON po.PO_Status_Key = pos.PO_Status_Key AND r.PCN = pos.PCN
            LEFT JOIN Sales_v_Release_Status AS rs
                ON r.Release_Status_Key = rs.Release_Status_Key AND r.PCN = rs.PCN
            LEFT JOIN Sales_v_Release_Type AS rt
                ON r.Release_Type_Key = rt.Release_Type_Key AND r.PCN = rt.PCN
            LEFT JOIN Part_v_Part AS p
                ON pol.Part_Key = p.Part_Key AND r.PCN = p.Plexus_Customer_No
            LEFT JOIN Part_v_Customer_Part AS cp
                ON pol.Customer_Part_Key = cp.Customer_Part_Key AND r.PCN = cp.Plexus_Customer_No
            LEFT JOIN (
                SELECT c.Part_Key, c.Plexus_Customer_No,
                       SUM(CASE WHEN c.Location NOT LIKE '%TIJ%' OR c.Location IS NULL
                                THEN c.Quantity ELSE 0 END) AS WIP_Quantity
                FROM Part_v_Container AS c
                WHERE c.Plexus_Customer_No = {PCN}
                  AND c.Quantity > 0
                GROUP BY c.Part_Key, c.Plexus_Customer_No
            ) AS wip
                ON p.Part_Key = wip.Part_Key AND p.Plexus_Customer_No = wip.Plexus_Customer_No
            WHERE r.PCN = {PCN}
              {customer_filter}
              {status_filter}
              {balance_filter}
            ORDER BY Customer, r.Ship_Date {order_dir}, p.Part_No
        """

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Shift range helper ───────────────────────────────────────────────────────

def get_shift_range(report_date: str) -> tuple[str, str]:
    """
    Turno 6AM-6AM. Plex guarda en hora servidor (UTC+3 aprox).
    6AM local = 09:00 en servidor. Rango: +9h a +33h del report_date.
    """
    from datetime import datetime, timedelta
    day         = datetime.strptime(report_date, "%Y-%m-%d")
    shift_start = day + timedelta(hours=9)
    shift_end   = day + timedelta(hours=33)
    return shift_start.strftime("%Y-%m-%d %H:%M:%S"), shift_end.strftime("%Y-%m-%d %H:%M:%S")


TULC_WORKCENTERS = {"TULC Ensamble Final"}

VOLVO_PARTS       = {"43301", "43302", "43303", "43304", "43305", "43306", "43291", "45294"}
VOLVO_WORKCENTERS = {"HM Ensamble Final 2"}
CUMMINS_WORKCENTERS = {"HM Ensamble de Servicio", "HM Empaque"}
ALL_PROD_WORKCENTERS = VOLVO_WORKCENTERS | CUMMINS_WORKCENTERS | TULC_WORKCENTERS
WC_LIST = "', '".join(ALL_PROD_WORKCENTERS)


class DailyProductionRequest(BaseModel):
    report_date: str  # YYYY-MM-DD


# ─── Daily Production ─────────────────────────────────────────────────────────

@app.post("/daily-production", dependencies=[Security(verify_token)])
def daily_production(req: DailyProductionRequest):
    try:
        shift_start, shift_end = get_shift_range(req.report_date)
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                p.Part_No,
                wc.Name                                          AS Workcenter,
                SUM(pe.Quantity)                                AS Quantity,
                ROUND(SUM(pe.Quantity * ISNULL(pc.Cost, 0)), 2) AS Extended_Cost
            FROM Part_v_Production_e AS pe
                LEFT JOIN Part_v_Part_e AS p
                    ON pe.Part_Key = p.Part_Key
                    AND p.Plexus_Customer_No = {PCN}
                LEFT JOIN Part_v_Workcenter AS wc
                    ON pe.Workcenter_Key = wc.Workcenter_Key
                    AND wc.Plexus_Customer_No = {PCN}
                LEFT JOIN Part_v_Part_Cost AS pc
                    ON pe.Part_Key = pc.Part_Key
                    AND pc.PCN = {PCN}
                    AND pc.Cost_Model_Key = 5689
            WHERE pe.Record_Date >= '{shift_start}'
              AND pe.Record_Date <  '{shift_end}'
              AND pe.Plexus_Customer_No = {PCN}
              AND wc.Name IN ('{WC_LIST}')
            GROUP BY p.Part_No, wc.Name
        """)
        rows = query_to_list(cursor)
        conn.close()

        volvo_qty    = 0
        cummins_qty  = 0
        tulc_qty     = 0
        volvo_cost   = 0.0
        cummins_cost = 0.0
        tulc_cost    = 0.0

        for row in rows:
            part_no = str(row["Part_No"]  or "").strip().split(".")[0]
            wc_name = str(row["Workcenter"] or "")
            qty     = float(row["Quantity"]      or 0)
            cost    = float(row["Extended_Cost"] or 0)

            if wc_name in TULC_WORKCENTERS:
                tulc_qty  += qty
                tulc_cost += cost
            elif part_no in VOLVO_PARTS:
                volvo_qty  += qty
                volvo_cost += cost
            else:
                cummins_qty  += qty
                cummins_cost += cost

        return {
            "date":    req.report_date,
            "volvo":   {"quantity": int(volvo_qty),   "cogp_cost": round(volvo_cost,   2)},
            "cummins": {"quantity": int(cummins_qty), "cogp_cost": round(cummins_cost, 2)},
            "tulc":    {"quantity": int(tulc_qty),    "cogp_cost": round(tulc_cost,    2)},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── Scrap COGP % ─────────────────────────────────────────────────────────────

@app.post("/scrap-cogp", dependencies=[Security(verify_token)])
def scrap_cogp(req: DailyProductionRequest):
    try:
        shift_start, shift_end = get_shift_range(req.report_date)
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                wc.Name                        AS Workcenter_Name,
                ROUND(SUM(s.Quantity),      0) AS Scrap_Qty,
                ROUND(SUM(s.Extended_Cost), 2) AS Scrap_Cost
            FROM Part_v_Scrap s
            INNER JOIN Part_v_Workcenter wc
                ON s.Workcenter_Key        = wc.Workcenter_Key
                AND s.Plexus_Customer_No   = wc.Plexus_Customer_No
            INNER JOIN Part_v_Part_e p
                ON s.Part_Key = p.Part_Key
            WHERE s.Plexus_Customer_No = {PCN}
              AND s.Scrap_Date >= '{shift_start}'
              AND s.Scrap_Date <  '{shift_end}'
              AND wc.Name IN ('{WC_LIST}')
              AND LEFT(LTRIM(p.Part_No), 5) IN (
                  '43301','43302','43303','43304','43305',
                  '43306','43291','45294','43400','43413','43422'
              )
            GROUP BY wc.Name
        """)
        rows = query_to_list(cursor)
        conn.close()

        volvo_qty    = 0
        cummins_qty  = 0
        volvo_cost   = 0.0
        cummins_cost = 0.0
        tulc_scrap_qty  = 0
        tulc_scrap_cost = 0.0

        for row in rows:
            wc   = row["Workcenter_Name"] or ""
            qty  = float(row["Scrap_Qty"]  or 0)
            cost = float(row["Scrap_Cost"] or 0)
            if wc in VOLVO_WORKCENTERS:
                volvo_qty  += int(qty)
                volvo_cost += cost
            elif wc in TULC_WORKCENTERS:
                tulc_scrap_qty  += int(qty)
                tulc_scrap_cost += cost
            else:
                cummins_qty  += int(qty)
                cummins_cost += cost

        return {
            "date":    req.report_date,
            "volvo":   {"scrap_qty": volvo_qty,   "scrap_cost": round(volvo_cost,   2)},
            "cummins": {"scrap_qty": cummins_qty, "scrap_cost": round(cummins_cost, 2)},
            "tulc": {"scrap_qty":  tulc_scrap_qty,"scrap_cost": round(tulc_scrap_cost, 2)},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Earned Labor Hours ───────────────────────────────────────────────────────

@app.post("/earned-labor-hours", dependencies=[Security(verify_token)])
def earned_labor_hours(req: DailyProductionRequest):
    try:
        shift_start, shift_end = get_shift_range(req.report_date)
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                ROUND(SUM(
                    pe.Quantity *
                    CASE
                        WHEN wc.Name = 'TULC Encapsulado Final'     AND pop.Operation_No = '337'  THEN 0.018800
                        WHEN wc.Name = 'TULC Ensamble de Cable'     AND pop.Operation_No = '122'  THEN 0.041667
                        WHEN wc.Name = 'TULC Ensamble de Sensor'    AND pop.Operation_No = '312'  THEN 0.000000
                        WHEN wc.Name = 'TULC Ensamble Final'        AND pop.Operation_No = '420'  THEN 0.161017
                        WHEN wc.Name = 'TULC Soldadura de Sensores' AND pop.Operation_No = '342'  THEN 0.007881
                        WHEN wc.Name = 'HM Dobladora Unison'        AND pop.Operation_No = '585'  THEN 0.044450
                        WHEN wc.Name = 'HM Empaque'                 AND pop.Operation_No = '1000' THEN 0.016696
                        WHEN wc.Name = 'HM Ensamble Final 2'        AND pop.Operation_No = '905'  THEN 0.275000
                        WHEN wc.Name = 'HM Ensamble Final 3'        AND pop.Operation_No = '980'  THEN 0.100000
                        WHEN wc.Name = 'HM Ensamble Frontal 2'      AND pop.Operation_No = '715'  THEN 0.050000
                        WHEN wc.Name = 'HM Ensamble Frontal 3'      AND pop.Operation_No = '905'  THEN 0.050000
                        WHEN wc.Name = 'HM Proto 1'                 AND pop.Operation_No = '505'  THEN 0.007294
                        WHEN wc.Name = 'HM Soldadura de Siphon'     AND pop.Operation_No = '525'  THEN 0.022217
                        ELSE 0
                    END
                ), 2) AS Earned_Labor_Hours
            FROM Part_v_Production_e AS pe
                LEFT JOIN Part_v_Workcenter AS wc
                    ON pe.Workcenter_Key       = wc.Workcenter_Key
                    AND pe.Plexus_Customer_No  = wc.Plexus_Customer_No
                LEFT JOIN Part_v_Part_Operation AS pop
                    ON pe.Part_Operation_Key   = pop.Part_Operation_Key
                    AND pe.Plexus_Customer_No  = pop.Plexus_Customer_No
            WHERE pe.Record_Date >= '{shift_start}'
              AND pe.Record_Date <  '{shift_end}'
              AND pe.Plexus_Customer_No = {PCN}
        """)
        row    = cursor.fetchone()
        conn.close()
        base   = float(row[0]) if row and row[0] is not None else 0.0
        earned = round(base * 1.01274, 2)
        return {"date": req.report_date, "earned_labor_hours": earned}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Yield por cliente ────────────────────────────────────────────────────────

@app.post("/yield-by-client", dependencies=[Security(verify_token)])
def yield_by_client(req: DailyProductionRequest):
    try:
        shift_start, shift_end = get_shift_range(req.report_date)
        conn   = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT wc.Name AS Workcenter, SUM(pe.Quantity) AS Quantity
            FROM Part_v_Production_e pe
            INNER JOIN Part_v_Workcenter wc
                ON pe.Workcenter_Key       = wc.Workcenter_Key
                AND pe.Plexus_Customer_No  = wc.Plexus_Customer_No
            WHERE pe.Plexus_Customer_No = {PCN}
              AND pe.Record_Date >= '{shift_start}'
              AND pe.Record_Date <  '{shift_end}'
              AND wc.Name IN ('{WC_LIST}')
            GROUP BY wc.Name
        """)
        prod_rows = query_to_list(cursor)

        cursor.execute(f"""
            SELECT wc.Name AS Workcenter, SUM(s.Quantity) AS Scrap_Qty
            FROM Part_v_Scrap s
            INNER JOIN Part_v_Workcenter wc
                ON s.Workcenter_Key       = wc.Workcenter_Key
                AND s.Plexus_Customer_No  = wc.Plexus_Customer_No
            WHERE s.Plexus_Customer_No = {PCN}
              AND s.Scrap_Date >= '{shift_start}'
              AND s.Scrap_Date <  '{shift_end}'
              AND wc.Name IN ('{WC_LIST}')
              AND s.Part_Key IN (
                  SELECT DISTINCT pe.Part_Key
                  FROM Part_v_Production_e pe
                  WHERE pe.Plexus_Customer_No = {PCN}
                    AND pe.Record_Date >= '{shift_start}'
                    AND pe.Record_Date <  '{shift_end}'
              )
            GROUP BY wc.Name
        """)
        scrap_rows = query_to_list(cursor)
        conn.close()

        result = {
            "volvo":   {"production": 0, "scrap": 0, "yield_pct": 100.0},
            "cummins": {"production": 0, "scrap": 0, "yield_pct": 100.0},
            "tulc":    {"production": 0, "scrap": 0, "yield_pct": 100.0},
            "total":   {"production": 0, "scrap": 0, "yield_pct": 100.0},
        }

        for row in prod_rows:
            wc_name = row["Workcenter"] or ""
            qty     = int(float(row["Quantity"] or 0))
            if wc_name in VOLVO_WORKCENTERS:
                result["volvo"]["production"]   += qty
            elif wc_name in CUMMINS_WORKCENTERS:
                result["cummins"]["production"] += qty
            elif wc_name in TULC_WORKCENTERS:
                result["tulc"]["production"]    += qty
            result["total"]["production"] += qty

        for row in scrap_rows:
            wc_name = row["Workcenter"] or ""
            qty     = int(float(row["Scrap_Qty"] or 0))
            if wc_name in VOLVO_WORKCENTERS:
                result["volvo"]["scrap"]   += qty
            elif wc_name in CUMMINS_WORKCENTERS:
                result["cummins"]["scrap"] += qty
            elif wc_name in TULC_WORKCENTERS:
                result["tulc"]["scrap"]    += qty
            result["total"]["scrap"] += qty

        for client in ["volvo", "cummins", "tulc", "total"]:
            prod  = result[client]["production"]
            scrap = result[client]["scrap"]
            total = prod + scrap
            result[client]["yield_pct"] = round((prod / total * 100), 2) if total > 0 else 100.0

        return {"date": req.report_date, **result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ─── Production Range (para tabla semanal/mensual) ───────────────────────────

class ProductionRangeRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date:   str  # YYYY-MM-DD (inclusive)


# ─── Production Range (para tabla semanal/mensual) ───────────────────────────

class ProductionRangeRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date:   str  # YYYY-MM-DD (inclusive)


@app.post("/production-range", dependencies=[Security(verify_token)])
def production_range(req: ProductionRangeRequest):
    try:
        from datetime import datetime, timedelta

        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end   = datetime.strptime(req.end_date,   "%Y-%m-%d")

        if (end - start).days > 120:
            raise HTTPException(status_code=400, detail="Rango máximo 120 días.")

        results = []
        current = start

        conn   = get_connection()
        cursor = conn.cursor()

        while current <= end:
            date_str               = current.strftime("%Y-%m-%d")
            shift_start, shift_end = get_shift_range(date_str)

            # ── Producción ────────────────────────────────────────────────────
            cursor.execute(f"""
                SELECT
                    wc.Name          AS Workcenter,
                    SUM(pe.Quantity) AS Quantity
                FROM Part_v_Production_e AS pe
                    INNER JOIN Part_v_Workcenter AS wc
                        ON pe.Workcenter_Key      = wc.Workcenter_Key
                        AND pe.Plexus_Customer_No = wc.Plexus_Customer_No
                WHERE pe.Record_Date       >= '{shift_start}'
                  AND pe.Record_Date        < '{shift_end}'
                  AND pe.Plexus_Customer_No = {PCN}
                  AND wc.Name IN ('{WC_LIST}')
                GROUP BY wc.Name
            """)
            prod_rows = query_to_list(cursor)

            # ── Scrap (solo producto terminado) ───────────────────────────────
            cursor.execute(f"""
                SELECT
                    wc.Name                        AS Workcenter_Name,
                    ROUND(SUM(s.Quantity),      0) AS Scrap_Qty,
                    ROUND(SUM(s.Extended_Cost), 2) AS Scrap_Cost
                FROM Part_v_Scrap s
                    INNER JOIN Part_v_Workcenter wc
                        ON s.Workcenter_Key      = wc.Workcenter_Key
                        AND s.Plexus_Customer_No = wc.Plexus_Customer_No
                    INNER JOIN Part_v_Part_e p
                        ON s.Part_Key = p.Part_Key
                WHERE s.Plexus_Customer_No = {PCN}
                  AND s.Scrap_Date >= '{shift_start}'
                  AND s.Scrap_Date <  '{shift_end}'
                  AND wc.Name IN ('{WC_LIST}')
                  AND LEFT(LTRIM(p.Part_No), 5) IN (
                      '43301','43302','43303','43304','43305',
                      '43306','43291','45294','43400','43413','43422'
                  )
                GROUP BY wc.Name
            """)
            scrap_rows = query_to_list(cursor)

            # ── Clasificar ────────────────────────────────────────────────────
            volvo_qty      = 0
            cummins_qty    = 0
            volvo_scrap_qty    = 0
            cummins_scrap_qty  = 0
            volvo_scrap_cost   = 0.0
            cummins_scrap_cost = 0.0
            tulc_qty       = 0
            tulc_scrap_qty = 0
            tulc_scrap_cost = 0.0

            for row in prod_rows:
                wc  = row["Workcenter"] or ""
                qty = float(row["Quantity"] or 0)
                if wc in VOLVO_WORKCENTERS:
                    volvo_qty   += qty
                elif wc in TULC_WORKCENTERS:
                    tulc_qty += qty
                elif wc in CUMMINS_WORKCENTERS:
                    cummins_qty += qty

            for row in scrap_rows:
                wc   = row["Workcenter_Name"] or ""
                qty  = float(row["Scrap_Qty"]  or 0)
                cost = float(row["Scrap_Cost"] or 0)
                if wc in VOLVO_WORKCENTERS:
                    volvo_scrap_qty  += int(qty)
                    volvo_scrap_cost += cost
                elif wc in TULC_WORKCENTERS:
                    tulc_scrap_qty  += int(qty)
                    tulc_scrap_cost += cost
                elif wc in CUMMINS_WORKCENTERS:
                    cummins_scrap_qty  += int(qty)
                    cummins_scrap_cost += cost

            results.append({
                "date": date_str,
                "volvo": {
                    "quantity":   int(volvo_qty),
                    "cogp_cost":  0.0,
                    "scrap_qty":  volvo_scrap_qty,
                    "scrap_cost": round(volvo_scrap_cost, 2),
                },
                "cummins": {
                    "quantity":   int(cummins_qty),
                    "cogp_cost":  0.0,
                    "scrap_qty":  cummins_scrap_qty,
                    "scrap_cost": round(cummins_scrap_cost, 2),
                },
                "tulc": {
                    "quantity":   int(tulc_qty),
                    "cogp_cost":  0.0,
                    "scrap_qty":  tulc_scrap_qty,
                    "scrap_cost": round(tulc_scrap_cost, 2),
},
            })

            current += timedelta(days=1)

        conn.close()
        return {"start_date": req.start_date, "end_date": req.end_date, "days": results}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── Maintenance KPIs ─────────────────────────────────────────────────────────

class MaintenanceKPIRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date:   str  # YYYY-MM-DD (exclusive)


@app.post("/maintenance-kpis", dependencies=[Security(verify_token)])
def maintenance_kpis(req: MaintenanceKPIRequest):
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                ROUND(SUM(CASE WHEN wl.Workcenter_Status_Key = 5448 THEN wl.Log_Hours ELSE 0 END), 2) AS Operating_Hours,
                ROUND(SUM(CASE WHEN wl.Workcenter_Status_Key IN (5445, 5449) THEN wl.Log_Hours ELSE 0 END), 2) AS Downtime_Hours,
                ROUND(SUM(CASE WHEN wl.Workcenter_Status_Key = 5445 THEN wl.Log_Hours ELSE 0 END), 2) AS Down_Hours,
                ROUND(SUM(CASE WHEN wl.Workcenter_Status_Key = 5449 THEN wl.Log_Hours ELSE 0 END), 2) AS Setup_Hours,
                ROUND(SUM(CASE WHEN wl.Workcenter_Status_Key = 5446 THEN wl.Log_Hours ELSE 0 END), 2) AS Idle_Hours,
                SUM(CASE WHEN wl.Workcenter_Status_Key = 5445 THEN 1 ELSE 0 END) AS Total_Failures,
                ROUND(
                    SUM(CASE WHEN wl.Workcenter_Status_Key = 5445 THEN wl.Log_Hours ELSE 0 END) /
                    NULLIF(SUM(CASE WHEN wl.Workcenter_Status_Key = 5445 THEN 1 ELSE 0 END), 0)
                , 2) AS MTTR_Hours,
                ROUND(
                    SUM(CASE WHEN wl.Workcenter_Status_Key = 5448 THEN wl.Log_Hours ELSE 0 END) /
                    NULLIF(SUM(CASE WHEN wl.Workcenter_Status_Key = 5445 THEN 1 ELSE 0 END), 0)
                , 2) AS MTBF_Hours,
                ROUND(
                    SUM(CASE WHEN wl.Workcenter_Status_Key = 5448 THEN wl.Log_Hours ELSE 0 END) * 100.0 /
                    NULLIF(
                        SUM(CASE WHEN wl.Workcenter_Status_Key IN (5448, 5445, 5449) THEN wl.Log_Hours ELSE 0 END)
                    , 0)
                , 2) AS Availability_Pct
            FROM Part_v_Workcenter_Log wl
            WHERE wl.Plexus_Customer_No = {PCN}
              AND wl.Log_Date >= '{req.start_date}'
              AND wl.Log_Date <  '{req.end_date}'
              AND wl.Log_Hours > 0
        """)
        row = cursor.fetchone()
        conn.close()
        if not row:
            return {"data": None}
        cols = [d[0] for d in cursor.description] if cursor.description else []
        # cursor ya cerrado — reconstruir desde row directamente
        keys = [
            "operating_hours", "downtime_hours", "down_hours", "setup_hours",
            "idle_hours", "total_failures", "mttr_hours", "mtbf_hours", "availability_pct"
        ]
        result = {}
        for i, key in enumerate(keys):
            val = row[i]
            result[key] = float(val) if val is not None else None
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Downtime Reasons ─────────────────────────────────────────────────────────

@app.post("/maintenance-downtime-reasons", dependencies=[Security(verify_token)])
def maintenance_downtime_reasons(req: MaintenanceKPIRequest):
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                ISNULL(we.Description, 'Sin Razón') AS Reason,
                COUNT(*) AS Total_Events,
                ROUND(SUM(wl.Log_Hours), 2) AS Total_Hours
            FROM Part_v_Workcenter_Log wl
            LEFT JOIN Part_v_Workcenter_Event we
                ON wl.Workcenter_Event_Key = we.Workcenter_Event_Key
                AND wl.Plexus_Customer_No = we.Plexus_Customer_No
            WHERE wl.Plexus_Customer_No = {PCN}
              AND wl.Log_Date >= '{req.start_date}'
              AND wl.Log_Date <  '{req.end_date}'
              AND wl.Workcenter_Status_Key IN (5445, 5449)
              AND wl.Log_Hours > 0
            GROUP BY we.Description
            ORDER BY Total_Hours DESC
        """)
        rows = query_to_list(cursor)
        conn.close()
        grand_total = sum(float(r["Total_Hours"] or 0) for r in rows)
        result = []
        for r in rows:
            hrs = float(r["Total_Hours"] or 0)
            result.append({
                "reason":       r["Reason"],
                "total_events": int(r["Total_Events"] or 0),
                "total_hours":  hrs,
                "percentage":   round(hrs / grand_total * 100, 2) if grand_total > 0 else 0,
            })
        return {"data": result, "grand_total_hours": round(grand_total, 2)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Downtime Reason Detail ───────────────────────────────────────────────────

class MaintenanceDetailRequest(BaseModel):
    start_date: str
    end_date:   str
    reason:     str


@app.post("/maintenance-downtime-detail", dependencies=[Security(verify_token)])
def maintenance_downtime_detail(req: MaintenanceDetailRequest):
    try:
        reason_filter = req.reason.replace("'", "''")
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                wl.Log_Date,
                wl.Log_Hours,
                ws.Description  AS Status,
                ISNULL(we.Description, 'Sin Razón') AS Reason,
                wl.Description  AS Notes,
                wc.Name         AS Workcenter,
                sh.Shift,
                p.Part_No,
                po.Operation_No,
                po.Description  AS Operation_Description,
                jo.Job_No
            FROM Part_v_Workcenter_Log wl
            LEFT JOIN Part_v_Workcenter_Status ws
                ON wl.Workcenter_Status_Key = ws.Workcenter_Status_Key
                AND wl.Plexus_Customer_No = ws.Plexus_Customer_No
            LEFT JOIN Part_v_Workcenter_Event we
                ON wl.Workcenter_Event_Key = we.Workcenter_Event_Key
                AND wl.Plexus_Customer_No = we.Plexus_Customer_No
            LEFT JOIN Part_v_Workcenter wc
                ON wl.Workcenter_Key = wc.Workcenter_Key
                AND wl.Plexus_Customer_No = wc.Plexus_Customer_No
            LEFT JOIN Common_v_Shift sh
                ON wl.Shift_Key = sh.Shift_Key
                AND wl.Plexus_Customer_No = sh.Plexus_Customer_No
            LEFT JOIN Part_v_Part_e p
                ON wl.Part_Key = p.Part_Key
                AND wl.Plexus_Customer_No = p.Plexus_Customer_No
            LEFT JOIN Part_v_Part_Operation po
                ON wl.Part_Operation_Key = po.Part_Operation_Key
                AND wl.Plexus_Customer_No = po.Plexus_Customer_No
            LEFT JOIN Part_v_Job_e jo
                ON wl.Job_Op_Key = jo.Job_Key
                AND wl.Plexus_Customer_No = jo.PCN
            WHERE wl.Plexus_Customer_No = {PCN}
              AND wl.Log_Date >= '{req.start_date}'
              AND wl.Log_Date <  '{req.end_date}'
              AND wl.Workcenter_Status_Key IN (5445, 5449)
              AND wl.Log_Hours > 0
              AND ISNULL(we.Description, 'Sin Razón') = '{reason_filter}'
            ORDER BY wl.Log_Date
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/maintenance-downtime-by-month", dependencies=[Security(verify_token)])
def maintenance_downtime_by_month(req: MaintenanceKPIRequest):
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                YEAR(wl.Log_Date)                           AS Year,
                MONTH(wl.Log_Date)                          AS Month,
                DAY(wl.Log_Date)                            AS Day,
                ISNULL(we.Description, 'Sin Razon')         AS Reason,
                COUNT(*)                                    AS Total_Events,
                ROUND(SUM(wl.Log_Hours), 2)                 AS Total_Hours
            FROM Part_v_Workcenter_Log wl
            LEFT JOIN Part_v_Workcenter_Event we
                ON wl.Workcenter_Event_Key = we.Workcenter_Event_Key
                AND wl.Plexus_Customer_No  = we.Plexus_Customer_No
            WHERE wl.Plexus_Customer_No = {PCN}
              AND wl.Log_Date >= '{req.start_date}'
              AND wl.Log_Date <  '{req.end_date}'
              AND wl.Workcenter_Status_Key IN (5445, 5449)
              AND wl.Log_Hours > 0
            GROUP BY
                YEAR(wl.Log_Date),
                MONTH(wl.Log_Date),
                DAY(wl.Log_Date),
                we.Description
            ORDER BY Year, Month, Day, Total_Hours DESC
        """)
        rows = query_to_list(cursor)
        conn.close()

        result = []
        for r in rows:
            year  = int(r["Year"]  or 0)
            month = int(r["Month"] or 0)
            day   = int(r["Day"]   or 0)
            result.append({
                "date":         f"{year:04d}-{month:02d}-{day:02d}",  # "2026-03-15"
                "reason":       r["Reason"],
                "total_events": int(r["Total_Events"]  or 0),
                "total_hours":  float(r["Total_Hours"] or 0),
            })
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class WorkRequestsRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date:   str  # YYYY-MM-DD (inclusive)


@app.post("/work-requests", dependencies=[Security(verify_token)])
def work_requests(req: WorkRequestsRequest):
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                wr.Work_Request_No,
                wr.Description,
                wr.Request_Date,
                wr.Due_Date,
                wr.Completed_Date,
                ws.Work_Request_Status,
                wt.Work_Request_Type,
                u.First_Name + ' ' + u.Last_Name AS Assigned_To,
                eq.Equipment_ID,
                eq.Description  AS Equipment_Description,
                eq.Equipment_Group,
                d.Name          AS Department_Name,
                CASE
                    WHEN wr.Completed_Production_Hours > 0
                        THEN wr.Completed_Production_Hours
                    ELSE wr.Scheduled_Hours
                END             AS Maintenance_Hours,
                f.Failure,
                ft.Failure_Type,
                fa.Failure_Action
            FROM Maintenance_v_Work_Request AS wr
            LEFT JOIN Plexus_Control_v_Plexus_User_e AS u
                ON wr.Assigned_To = u.Plexus_User_No
            LEFT JOIN Maintenance_v_Work_Request_Status AS ws
                ON wr.Work_Request_Status_Key = ws.Work_Request_Status_Key
            LEFT JOIN Maintenance_v_Work_Request_Type AS wt
                ON wr.Work_Request_Type_Key = wt.Work_Request_Type_Key
            LEFT JOIN Maintenance_v_Work_Request_Failure AS wrf
                ON wr.Work_Request_Key = wrf.Work_Request_Key
            LEFT JOIN Maintenance_v_Failure AS f
                ON wrf.Failure_Key = f.Failure_Key
            LEFT JOIN Maintenance_v_Failure_Type AS ft
                ON wrf.Failure_Type_Key = ft.Failure_Type_Key
            LEFT JOIN Maintenance_v_Failure_Action AS fa
                ON wrf.Failure_Action_Key = fa.Failure_Action_Key
            LEFT JOIN Maintenance_v_Equipment AS eq
                ON wr.Equipment_Key = eq.Equipment_Key
            LEFT JOIN Part_v_Workcenter AS wc
                ON wr.Workcenter_Key = wc.Workcenter_Key
            LEFT JOIN Common_v_Department AS d
                ON wc.Department_No = d.Department_No
            WHERE wr.Plexus_Customer_No = {PCN}
              AND CAST(wr.Request_Date AS DATE) >= '{req.start_date}'
              AND CAST(wr.Request_Date AS DATE) <= '{req.end_date}'
        """)
        rows = query_to_list(cursor)
        conn.close()

        # Normalizar nulos y tipos
        result = []
        for r in rows:
            result.append({
                "work_request_no":       r["Work_Request_No"],
                "description":           r["Description"] or "",
                "request_date":          r["Request_Date"].isoformat() if hasattr(r["Request_Date"], "isoformat") else str(r["Request_Date"] or ""),
                "due_date":              r["Due_Date"].isoformat() if hasattr(r["Due_Date"], "isoformat") else str(r["Due_Date"] or ""),
                "completed_date":        r["Completed_Date"].isoformat() if r["Completed_Date"] and hasattr(r["Completed_Date"], "isoformat") else None,
                "status":                r["Work_Request_Status"] or "Unknown",
                "type":                  r["Work_Request_Type"]   or "Unknown",
                "assigned_to":           r["Assigned_To"]         or "Unassigned",
                "equipment_id":          r["Equipment_ID"]        or "",
                "equipment_description": r["Equipment_Description"] or "",
                "equipment_group":       r["Equipment_Group"]     or "Other",
                "department":            r["Department_Name"]     or "Unknown",
                "maintenance_hours":     float(r["Maintenance_Hours"] or 0),
                "failure":               r["Failure"]             or "",
                "failure_type":          r["Failure_Type"]        or "",
                "failure_action":        r["Failure_Action"]      or "",
            })
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
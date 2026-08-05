import os
import pyodbc
import pandas as pd
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from oee_router import router as oee_router_router

load_dotenv()

DSN      = os.getenv("PLEX_DSN")
USERNAME = os.getenv("PLEX_USERNAME")
PASSWORD = os.getenv("PLEX_PASSWORD")
SECRET   = os.getenv("PROXY_SECRET")
PCN      = 306713

app = FastAPI(title="Plex ODBC Proxy", version="1.0.0")
security = HTTPBearer()
app.include_router(oee_router_router)


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

        # ── PASO 1: Parte raíz ───────────────────────────────────────────────
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

        root_key, root_no, root_rev, root_name = (
            root_row[0], root_row[1], root_row[2], root_row[3]
        )
        rows_collected = []
        seen_keys      = set()
        current_level  = [(root_key, root_no, root_rev, root_name, root_no, 1.0)]

        # ── PASO 2: Recorrer BOM nivel a nivel ───────────────────────────────
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
                parent_key      = cr[0]
                bom_qty         = float(cr[2]) if cr[2] is not None else 0.0
                note            = cr[3] or ''
                comp_key        = cr[4]
                comp_no         = cr[5]
                comp_rev        = cr[6]
                comp_name       = cr[7]
                comp_unit       = cr[8] or ''

                parent_info     = parent_map.get(parent_key)
                parent_path     = parent_info[4] if parent_info else ''
                parent_qty_acc  = parent_info[5] if parent_info else 1.0
                bom_path        = f"{parent_path} > {comp_no}"
                qty_accumulated = bom_qty * parent_qty_acc

                rows_collected.append({
                    "_comp_key":  comp_key,
                    "_comp_no":   comp_no,
                    "_comp_rev":  comp_rev,
                    "_qty_acc":   qty_accumulated,
                    "level":            level,
                    "root_part_no_rev": f"{root_no} Rev:{root_rev}",
                    "part_no_rev":      f"{comp_no} Rev:{comp_rev}",
                    "part_name":        comp_name,
                    "bom_qty":          round(bom_qty, 6),
                    "unit":             comp_unit,
                    "note":             note,
                    "bom_path":         bom_path,
                })

                if comp_key not in seen_keys:
                    seen_keys.add(comp_key)
                    next_level.append((
                        comp_key, comp_no, comp_rev, comp_name,
                        bom_path, qty_accumulated
                    ))

            current_level = next_level

        if not rows_collected:
            conn.close()
            return {"data": []}

        # ── PASO 2.5: Resolver revisión activa por Part_No ───────────────────
        # Regla: usar la revisión con Part_Status = 'Active' de mayor número.
        # Si un componente no tiene ninguna revisión Active → excluirlo.
        all_comp_nos = list({row["_comp_no"] for row in rows_collected})
        if len(all_comp_nos) == 1:
            nos_in_clause = f"('{all_comp_nos[0]}')"
        else:
            nos_in_clause = "(" + ", ".join(f"'{n}'" for n in all_comp_nos) + ")"

        cursor.execute(f"""
            SELECT Part_No, Revision, Part_Key
            FROM Part_v_Part
            WHERE Plexus_Customer_No = {PCN}
              AND Part_No IN {nos_in_clause}
              AND Part_Status = 'Active'
        """)
        rev_rows = cursor.fetchall()

        # Mapa part_no → (revision_activa_mas_alta, part_key)
        active_rev_map: dict = {}
        for rr in rev_rows:
            pno, rev, pkey = rr[0], rr[1], rr[2]
            if pno not in active_rev_map or str(rev) > str(active_rev_map[pno][0]):
                active_rev_map[pno] = (rev, pkey)

        # Excluir componentes sin revisión activa
        rows_collected = [
            row for row in rows_collected
            if row["_comp_no"] in active_rev_map
        ]

        if not rows_collected:
            conn.close()
            return {"data": []}

        # Anotar revisión activa en cada fila
        for row in rows_collected:
            comp_no    = row["_comp_no"]
            bom_rev    = row["_comp_rev"]
            chosen_rev, chosen_key = active_rev_map[comp_no]
            row["_inv_key"]          = chosen_key
            row["active_revision"]   = chosen_rev
            row["is_latest_revision"] = (str(bom_rev) == str(chosen_rev))

        # ── PASO 3: Inventario con revisión activa ───────────────────────────
        # WH  = locaciones TJR o TJ WH
        # WIP = todo lo que NO sea WH
        all_inv_keys  = list({row["_inv_key"] for row in rows_collected})
        inv_in_clause = f"({', '.join(str(k) for k in all_inv_keys)})"

        cursor.execute(f"""
            SELECT c.Part_Key,
                   SUM(c.Quantity) AS Total_Qty,
                   SUM(CASE WHEN (c.Location NOT LIKE '%TJR%' AND c.Location NOT LIKE '%TJ WH%')
                                  OR c.Location IS NULL
                            THEN c.Quantity ELSE 0 END) AS WIP,
                   SUM(CASE WHEN c.Location LIKE '%TJR%'
                              OR c.Location LIKE '%TJ WH%'
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

        inv_map: dict = {}
        for ir in inv_rows:
            inv_map[ir[0]] = {
                "total": float(ir[1]) if ir[1] else 0.0,
                "wip":   float(ir[2]) if ir[2] else 0.0,
                "inv":   float(ir[3]) if ir[3] else 0.0,
            }

        # ── PASO 4: Ensamblar respuesta final ────────────────────────────────
        final_rows = []
        for row in rows_collected:
            inv_key  = row["_inv_key"]
            qty_acc  = row["_qty_acc"]
            inv      = inv_map.get(inv_key, {"total": 0.0, "wip": 0.0, "inv": 0.0})
            ohymv    = round(qty_acc * req.need, 2)
            ohnv     = inv["total"]

            note_low   = (row.get("note") or "").lower()
            is_virtual = "phantom" in note_low or "embedded" in note_low
            # CTB se basa en WH (inv), no en OH total
            ctb = "Yes" if (is_virtual or inv["inv"] >= ohymv) else "No"

            final_rows.append({
                "level":             row["level"],
                "root_part_no_rev":  row["root_part_no_rev"],
                "part_no_rev":       row["part_no_rev"],
                "part_name":         row["part_name"],
                "bom_qty":           row["bom_qty"],
                "unit":              row["unit"],
                "need":              req.need,
                "ohymv":             ohymv,
                "wip":               round(inv["wip"], 2),
                "inv":               round(inv["inv"], 2),
                "ohnv":              round(ohnv, 2),
                "ctb":               ctb,
                "bom_path":          row["bom_path"],
                "note":              row["note"],
                "is_latest_revision": row["is_latest_revision"],
                "active_revision":   row["active_revision"],
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


def get_shift_window(start_date: str, end_date: str, offset_hours: int = 6) -> tuple[str, str]:
    """
    Ventana de turno [start_date + offset_hours, end_date + 1 dia + offset_hours)
    para rangos multi-dia (a diferencia de get_shift_range, que es para un solo dia).
    Calculado en Python e inyectado como string literal en el SQL -- DATEADD(...)
    dentro de un WHERE no funciona en el driver ODBC de Plex aunque evaluado de
    forma aislada (SELECT DATEADD(...)) si de un valor correcto (confirmado
    empiricamente contra Part_v_Workcenter_Log).
    """
    from datetime import datetime, timedelta
    start = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(hours=offset_hours)
    end   = datetime.strptime(end_date,   "%Y-%m-%d") + timedelta(days=1, hours=offset_hours)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


TULC_WORKCENTERS = {"TULC Ensamble Final"}

VOLVO_PARTS       = {"43301", "43302", "43303", "43304", "43305", "43306", "43291", "45294"}
VOLVO_WORKCENTERS = {"HM Ensamble Final 2"}
CUMMINS_WORKCENTERS = {"HM Ensamble Final 3", "HM Ensamble Frontal 3"}
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
        shift_start, shift_end = get_shift_window(req.start_date, req.end_date)
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
              AND wl.Log_Date >= '{shift_start}'
              AND wl.Log_Date <  '{shift_end}'
              AND wl.Log_Hours > 0
        """)
        row = cursor.fetchone()
        conn.close()
        if not row:
            return {"data": None}
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
        shift_start, shift_end = get_shift_window(req.start_date, req.end_date)
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
              AND wl.Log_Date >= '{shift_start}'
              AND wl.Log_Date <  '{shift_end}'
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
    reason:     Optional[str] = None


@app.post("/maintenance-downtime-detail", dependencies=[Security(verify_token)])
def maintenance_downtime_detail(req: MaintenanceDetailRequest):
    try:
        shift_start, shift_end = get_shift_window(req.start_date, req.end_date)

        reason_clause = ""
        if req.reason:
            reason_filter = req.reason.replace("'", "''")
            reason_clause = f"AND ISNULL(we.Description, 'Sin Razón') = '{reason_filter}'"

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
              AND wl.Log_Date >= '{shift_start}'
              AND wl.Log_Date <  '{shift_end}'
              AND wl.Workcenter_Status_Key IN (5445, 5449)
              AND wl.Log_Hours > 0
              {reason_clause}
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
        shift_start, shift_end = get_shift_window(req.start_date, req.end_date)
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
              AND wl.Log_Date >= '{shift_start}'
              AND wl.Log_Date <  '{shift_end}'
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
                "date":         f"{year:04d}-{month:02d}-{day:02d}",
                "reason":       r["Reason"],
                "total_events": int(r["Total_Events"]  or 0),
                "total_hours":  float(r["Total_Hours"] or 0),
            })
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

WR_DATE_FIELDS = {
    "Request_Date":   "wr.Request_Date",
    "Due_Date":       "wr.Due_Date",
    "Completed_Date": "wr.Completed_Date",
}


class WorkRequestsRequest(BaseModel):
    start_date: str                                  # YYYY-MM-DD
    end_date:   str                                  # YYYY-MM-DD (inclusive)
    work_request_type_key: Optional[int] = None      # None = todos los tipos
    date_field: str = "Request_Date"                 # columna que filtra el rango


@app.post("/work-requests", dependencies=[Security(verify_token)])
def work_requests(req: WorkRequestsRequest):
    try:
        from datetime import datetime

        date_col = WR_DATE_FIELDS.get(req.date_field)
        if date_col is None:
            raise HTTPException(
                status_code=400,
                detail=f"date_field invalido. Validos: {sorted(WR_DATE_FIELDS)}",
            )

        start_dt = datetime.strptime(req.start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(req.end_date,   "%Y-%m-%d")
        if end_dt < start_dt:
            raise HTTPException(status_code=400, detail="end_date anterior a start_date.")
        if (end_dt - start_dt).days > 400:
            raise HTTPException(status_code=400, detail="Rango maximo 400 dias.")

        end_inclusive = f"{req.end_date} 23:59:59"

        type_clause = ""
        if req.work_request_type_key is not None:
            type_clause = f"AND wr.Work_Request_Type_Key = {int(req.work_request_type_key)}"

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
                eq.Description      AS Equipment_Description,
                eq.Equipment_Group,
                wc.Name             AS Workcenter,
                wc.Workcenter_Group AS Workcenter_Group,
                d.Name              AS Department_Name,
                ROUND(ISNULL(wr.Scheduled_Hours, 0), 2)  AS Scheduled_Hours,
                ROUND(ISNULL(el.Duration, 0), 2)         AS Maintenance_Hours,
                f.Failure,
                ft.Failure_Type,
                fa.Failure_Action
            FROM Maintenance_v_Work_Request AS wr
            INNER JOIN Maintenance_v_Work_Request_Type AS wt
                ON wr.Work_Request_Type_Key = wt.Work_Request_Type_Key
            LEFT JOIN Plexus_Control_v_Plexus_User_e AS u
                ON wr.Assigned_To          = u.Plexus_User_No
               AND wr.Plexus_Customer_No   = u.Plexus_Customer_No
            LEFT JOIN Maintenance_v_Work_Request_Status AS ws
                ON wr.Work_Request_Status_Key = ws.Work_Request_Status_Key
            LEFT JOIN Maintenance_v_Work_Request_Failure AS wrf
                ON wr.Work_Request_Key = wrf.Work_Request_Key
            LEFT JOIN Maintenance_v_Failure AS f
                ON wrf.Failure_Key = f.Failure_Key
            LEFT JOIN Maintenance_v_Failure_Type AS ft
                ON wrf.Failure_Type_Key = ft.Failure_Type_Key
            LEFT JOIN Maintenance_v_Failure_Action AS fa
                ON wrf.Failure_Action_Key = fa.Failure_Action_Key
            LEFT JOIN Maintenance_v_Equipment AS eq
                ON wr.Equipment_Key        = eq.Equipment_Key
               AND wr.Plexus_Customer_No   = eq.Plexus_Customer_No
            LEFT JOIN Part_v_Workcenter AS wc
                ON wr.Workcenter_Key       = wc.Workcenter_Key
               AND wr.Plexus_Customer_No   = wc.Plexus_Customer_No
            LEFT JOIN Common_v_Department AS d
                ON wc.Department_No = d.Department_No
            LEFT JOIN (
                SELECT el.Work_Request_Key, SUM(el.Duration) AS Duration
                FROM Maintenance_v_Equipment_Log el
                GROUP BY el.Work_Request_Key
            ) AS el
                ON wr.Work_Request_Key = el.Work_Request_Key
            WHERE wr.Plexus_Customer_No = {PCN}
              AND {date_col} >= '{req.start_date}'
              AND {date_col} <= '{end_inclusive}'
              {type_clause}
        """)
        rows = query_to_list(cursor)
        conn.close()

        # Colapsa el fan-out de Work_Request_Failure: una entrada por WR,
        # con las fallas distintas acumuladas en una lista. Los campos planos
        # failure/failure_type/failure_action conservan la primera falla para
        # no romper el contrato del dashboard existente.
        merged: dict = {}
        for r in rows:
            no = r["Work_Request_No"]
            if no not in merged:
                merged[no] = {
                    "work_request_no":       no,
                    "description":           r["Description"] or "",
                    "request_date":          str(r["Request_Date"]   or ""),
                    "due_date":              str(r["Due_Date"]       or ""),
                    "completed_date":        str(r["Completed_Date"]) if r["Completed_Date"] else None,
                    "status":                r["Work_Request_Status"]    or "Unknown",
                    "type":                  r["Work_Request_Type"]      or "Unknown",
                    "assigned_to":           r["Assigned_To"]            or "Unassigned",
                    "equipment_id":          r["Equipment_ID"]           or "",
                    "equipment_description": r["Equipment_Description"]  or "",
                    "equipment_group":       r["Equipment_Group"]        or "Other",
                    "workcenter":            r["Workcenter"]             or "",
                    "workcenter_group":      r["Workcenter_Group"]       or "",
                    "department":            r["Department_Name"]        or "Unknown",
                    "scheduled_hours":       float(r["Scheduled_Hours"]   or 0),
                    "maintenance_hours":     float(r["Maintenance_Hours"] or 0),
                    "failures":              [],
                }
            failure = {
                "failure":        r["Failure"]        or "",
                "failure_type":   r["Failure_Type"]   or "",
                "failure_action": r["Failure_Action"] or "",
            }
            if any(failure.values()) and failure not in merged[no]["failures"]:
                merged[no]["failures"].append(failure)

        result = []
        for item in merged.values():
            first = item["failures"][0] if item["failures"] else {}
            item["failure"]        = first.get("failure",        "")
            item["failure_type"]   = first.get("failure_type",   "")
            item["failure_action"] = first.get("failure_action", "")
            result.append(item)

        return {"data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Scrap Detail (Quality Dashboard) ────────────────────────────────────────

class ScrapDetailRequest(BaseModel):
    start_date: str
    end_date:   str
    use_shift:  bool = True


# ─── Shift range helper ───────────────────────────────────────────────────────

def get_shift_ab(scrap_date) -> str:
    """
    Turno A: 6AM-6PM local = 09:00-21:00 en servidor (UTC+3).
    Turno B: 6PM-6AM local = 21:00-09:00 en servidor.
    scrap_date puede llegar como datetime o como string ISO.
    """
    from datetime import datetime as dt
    if scrap_date is None:
        return "B"
    if isinstance(scrap_date, str):
        try:
            scrap_date = dt.fromisoformat(scrap_date)
        except ValueError:
            return "B"
    hour = scrap_date.hour
    return "A" if 9 <= hour < 21 else "B"


@app.post("/scrap-detail", dependencies=[Security(verify_token)])
def scrap_detail(req: ScrapDetailRequest):
    try:
        from datetime import datetime, timedelta

        if req.use_shift:
            start_dt  = datetime.strptime(req.start_date, "%Y-%m-%d") + timedelta(hours=9)
            end_dt    = datetime.strptime(req.end_date,   "%Y-%m-%d") + timedelta(hours=33)
            date_col  = "s.Scrap_Date"
            start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            end_str   = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            prod_col  = "pe.Record_Date"
        else:
            date_col  = "CAST(s.Scrap_Date AS DATE)"
            start_str = req.start_date
            end_str   = req.end_date
            prod_col  = "CAST(pe.Report_Date AS DATE)"

        conn   = get_connection()
        cursor = conn.cursor()

        # ── Scrap detallado ───────────────────────────────────────────────
        cursor.execute(f"""
            SELECT
                wc.Name                         AS Workcenter,
                p.Part_No,
                p.Part_Type,
                s.Scrap_Reason,
                s.Scrap_Date,
                SUM(s.Quantity)                 AS Scrap_Qty,
                ROUND(SUM(s.Extended_Cost), 2)  AS Scrap_Cost
            FROM Part_v_Scrap s
            INNER JOIN Part_v_Workcenter wc
                ON s.Workcenter_Key      = wc.Workcenter_Key
                AND s.Plexus_Customer_No = wc.Plexus_Customer_No
            INNER JOIN Part_v_Part_e p
                ON s.Part_Key            = p.Part_Key
                AND s.Plexus_Customer_No = p.Plexus_Customer_No
            WHERE s.Plexus_Customer_No = {PCN}
              AND {date_col} >= '{start_str}'
              AND {date_col} <  '{end_str}'
              AND s.Quantity > 0
            GROUP BY wc.Name, p.Part_No, p.Part_Type, s.Scrap_Reason, s.Scrap_Date
            ORDER BY Scrap_Qty DESC
        """)
        scrap_rows = query_to_list(cursor)

        # ── Producción por workcenter (para yield) ────────────────────────
        cursor.execute(f"""
            SELECT
                wc.Name          AS Workcenter,
                SUM(pe.Quantity) AS Quantity
            FROM Part_v_Production_e pe
            INNER JOIN Part_v_Workcenter wc
                ON pe.Workcenter_Key      = wc.Workcenter_Key
                AND pe.Plexus_Customer_No = wc.Plexus_Customer_No
            WHERE pe.Plexus_Customer_No = {PCN}
              AND {prod_col} >= '{start_str}'
              AND {prod_col} <  '{end_str}'
            GROUP BY wc.Name
        """)
        prod_rows = query_to_list(cursor)
        conn.close()

        # ── Helpers ───────────────────────────────────────────────────────
        def get_bu(wc_name: str, part_no: str) -> str:
            if wc_name in TULC_WORKCENTERS:
                return "tulc"
            elif wc_name in VOLVO_WORKCENTERS:
                return "volvo"
            elif wc_name in CUMMINS_WORKCENTERS:
                return "cummins"
            return "cummins"  # fallback por si hay scrap en otros WC

        prod_map = {r["Workcenter"]: float(r["Quantity"] or 0) for r in prod_rows}

        # ── by_workcenter con desglose turno A/B ──────────────────────────
        wc_map: dict = {}
        for r in scrap_rows:
            wc         = r["Workcenter"]
            part_no    = r["Part_No"] or ""
            qty        = float(r["Scrap_Qty"]  or 0)
            cost       = float(r["Scrap_Cost"] or 0)
            bu         = get_bu(wc, part_no)
            scrap_date = r["Scrap_Date"]
            shift      = get_shift_ab(scrap_date) if scrap_date else "B"

            if wc not in wc_map:
                wc_map[wc] = {
                    "workcenter": wc, "bu": bu,
                    "scrap_qty": 0.0, "scrap_cost": 0.0,
                    "shift_a": {"scrap_qty": 0.0},
                    "shift_b": {"scrap_qty": 0.0},
                }
            wc_map[wc]["scrap_qty"]  += qty
            wc_map[wc]["scrap_cost"] += cost
            wc_map[wc][f"shift_{shift.lower()}"]["scrap_qty"] += qty

        by_workcenter = []
        for wc, d in wc_map.items():
            prod      = prod_map.get(wc, 0.0)
            scrap_qty = d["scrap_qty"]
            total     = prod + scrap_qty
            yield_pct = round(prod / total * 100, 2) if total > 0 else 100.0

            sa_qty  = d["shift_a"]["scrap_qty"]
            sb_qty  = d["shift_b"]["scrap_qty"]
            sa_prod = prod * 0.5
            sb_prod = prod * 0.5

            by_workcenter.append({
                "workcenter": wc,
                "bu":         d["bu"],
                "production": int(prod),
                "scrap_qty":  int(scrap_qty),
                "yield_pct":  yield_pct,
                "scrap_cost": round(d["scrap_cost"], 2),
                "shift_a": {
                    "scrap_qty": int(sa_qty),
                    "yield_pct": round((sa_prod / (sa_prod + sa_qty)) * 100, 2) if (sa_prod + sa_qty) > 0 else 100.0,
                },
                "shift_b": {
                    "scrap_qty": int(sb_qty),
                    "yield_pct": round((sb_prod / (sb_prod + sb_qty)) * 100, 2) if (sb_prod + sb_qty) > 0 else 100.0,
                },
            })
        by_workcenter.sort(key=lambda x: x["yield_pct"])

        # ── by_reason (Pareto) ────────────────────────────────────────────
        reason_map: dict = {}
        for r in scrap_rows:
            reason = r["Scrap_Reason"] or "Sin Razón"
            qty    = float(r["Scrap_Qty"]  or 0)
            cost   = float(r["Scrap_Cost"] or 0)
            if reason not in reason_map:
                reason_map[reason] = {"scrap_reason": reason, "total_qty": 0.0, "total_cost": 0.0}
            reason_map[reason]["total_qty"]  += qty
            reason_map[reason]["total_cost"] += cost

        total_scrap = sum(v["total_qty"] for v in reason_map.values())
        by_reason   = sorted(reason_map.values(), key=lambda x: x["total_qty"], reverse=True)
        cumulative  = 0.0
        for r in by_reason:
            r["total_qty"]      = int(r["total_qty"])
            r["total_cost"]     = round(r["total_cost"], 2)
            pct                 = round(r["total_qty"] / total_scrap * 100, 2) if total_scrap > 0 else 0.0
            cumulative         += pct
            r["pct_of_total"]   = pct
            r["cumulative_pct"] = round(cumulative, 2)

        # ── by_part ───────────────────────────────────────────────────────
        part_map: dict = {}
        for r in scrap_rows:
            wc        = r["Workcenter"]
            part_no   = str(r["Part_No"]   or "").strip().split(".")[0]
            part_type = str(r["Part_Type"] or "").strip()
            qty       = float(r["Scrap_Qty"]  or 0)
            cost      = float(r["Scrap_Cost"] or 0)
            key       = f"{part_no}|{wc}"
            if key not in part_map:
                part_map[key] = {
                    "part_no":    part_no,
                    "part_type":  part_type,
                    "workcenter": wc,
                    "bu":         get_bu(wc, part_no),
                    "scrap_qty":  0.0,
                    "scrap_cost": 0.0,
                }
            part_map[key]["scrap_qty"]  += qty
            part_map[key]["scrap_cost"] += cost

        by_part = sorted(part_map.values(), key=lambda x: x["scrap_qty"], reverse=True)
        for r in by_part:
            r["scrap_qty"]  = int(r["scrap_qty"])
            r["scrap_cost"] = round(r["scrap_cost"], 2)

        # ── heatmap turno × workcenter ────────────────────────────────────
        heatmap_map: dict = {}
        for r in scrap_rows:
            wc         = r["Workcenter"]
            qty        = float(r["Scrap_Qty"] or 0)
            scrap_date = r["Scrap_Date"]
            shift      = get_shift_ab(scrap_date) if scrap_date else "B"
            key        = f"{wc}|{shift}"
            heatmap_map[key] = heatmap_map.get(key, 0.0) + qty

        by_shift = [
            {"workcenter": k.split("|")[0], "shift": k.split("|")[1], "scrap_qty": int(v)}
            for k, v in heatmap_map.items()
        ]

        # ── trend diaria ──────────────────────────────────────────────────
        # ── trend diaria ──────────────────────────────────────────────────────────
        trend_map: dict = {}
        for r in scrap_rows:
            scrap_date = r["Scrap_Date"]
            if not scrap_date:
                continue
            if hasattr(scrap_date, 'date'):
                day_str = scrap_date.date().isoformat()
            else:
                day_str = str(scrap_date)[:10]

            wc      = r["Workcenter"]
            part_no = str(r["Part_No"] or "").strip().split(".")[0]
            bu      = get_bu(wc, part_no)
            qty     = float(r["Scrap_Qty"]  or 0)
            cost    = float(r["Scrap_Cost"] or 0)

            if day_str not in trend_map:
                trend_map[day_str] = {
                    "date": day_str,
                    "volvo_qty": 0.0, "cummins_qty": 0.0, "tulc_qty": 0.0,
                    "total_cost": 0.0, "scrap_qty": 0.0,
                }
            trend_map[day_str][f"{bu}_qty"] += qty
            trend_map[day_str]["total_cost"] += cost
            trend_map[day_str]["scrap_qty"]  += qty

        # Agregar producción por día para calcular yield
        for day_str, entry in trend_map.items():
            prod_day = 0.0
            for wc, prod_qty in prod_map.items():
                # prod_map es total del período — necesitamos por día
                pass

        # Query producción por día
        if req.use_shift:
            from datetime import datetime as _dt, timedelta as _td
            start_day = _dt.strptime(req.start_date, "%Y-%m-%d")
            end_day   = _dt.strptime(req.end_date,   "%Y-%m-%d")
        else:
            from datetime import datetime as _dt
            start_day = _dt.strptime(req.start_date, "%Y-%m-%d")
            end_day   = _dt.strptime(req.end_date,   "%Y-%m-%d")

        conn2   = get_connection()
        cursor2 = conn2.cursor()
        cursor2.execute(f"""
            SELECT
                CAST(pe.Record_Date AS DATE) AS Prod_Day,
                SUM(pe.Quantity)             AS Quantity
            FROM Part_v_Production_e pe
            INNER JOIN Part_v_Workcenter wc
                ON pe.Workcenter_Key      = wc.Workcenter_Key
                AND pe.Plexus_Customer_No = wc.Plexus_Customer_No
            WHERE pe.Plexus_Customer_No = {PCN}
              AND {prod_col} >= '{start_str}'
              AND {prod_col} <  '{end_str}'
            GROUP BY CAST(pe.Record_Date AS DATE)
        """)
        prod_by_day_rows = query_to_list(cursor2)
        conn2.close()

        prod_by_day: dict = {}
        for r in prod_by_day_rows:
            day = str(r["Prod_Day"])[:10]
            prod_by_day[day] = float(r["Quantity"] or 0)

        trend = []
        for d in sorted(trend_map.values(), key=lambda x: x["date"]):
            day_str       = d["date"]
            scrap_qty     = d["scrap_qty"]
            prod_qty      = prod_by_day.get(day_str, 0.0)
            total_qty_day = prod_qty + scrap_qty
            yield_pct_day = round(prod_qty / total_qty_day * 100, 2) if total_qty_day > 0 else 100.0
            volvo_qty     = d["volvo_qty"]
            cummins_qty   = d["cummins_qty"]
            tulc_qty      = d["tulc_qty"]

            trend.append({
                "date":         day_str,
                "volvo_qty":    int(volvo_qty),
                "cummins_qty":  int(cummins_qty),
                "tulc_qty":     int(tulc_qty),
                "total_qty":    int(scrap_qty),
                "total_cost":   round(d["total_cost"], 2),
                "production":   int(prod_qty),
                "yield_pct":    yield_pct_day,
            })

        # ── summary ───────────────────────────────────────────────────────
        total_prod      = sum(prod_map.values())
        total_scrap_qty = sum(r["scrap_qty"] for r in by_part)
        total_cost      = round(sum(r["scrap_cost"] for r in by_part), 2)
        grand_total     = total_prod + total_scrap_qty
        yield_pct       = round(total_prod / grand_total * 100, 2) if grand_total > 0 else 100.0

        return {
            "start_date": req.start_date,
            "end_date":   req.end_date,
            "use_shift":  req.use_shift,
            "summary": {
                "total_qty":   total_scrap_qty,
                "total_cost":  total_cost,
                "yield_pct":   yield_pct,
            },
            "by_workcenter": by_workcenter,
            "by_reason":     by_reason,
            "by_part":       by_part,
            "by_shift":      by_shift,
            "trend":         trend,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ─── Equipment catalog ────────────────────────────────────────────────────────

@app.get("/equipment", dependencies=[Security(verify_token)])
def equipment_list():
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                eq.Equipment_Key,
                eq.Equipment_ID,
                eq.Description,
                eq.Equipment_Group,
                wc.Name AS Workcenter
            FROM Maintenance_v_Equipment eq
            LEFT JOIN Part_v_Workcenter wc
                ON eq.Workcenter_Key      = wc.Workcenter_Key
                AND eq.Plexus_Customer_No = wc.Plexus_Customer_No
            WHERE eq.Plexus_Customer_No = {PCN}
              AND eq.Active = 1
            ORDER BY eq.Equipment_Group, eq.Equipment_ID
        """)
        rows = query_to_list(cursor)
        conn.close()
        return {"data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Incoming Inspection ──────────────────────────────────────────────────────

@app.post("/incoming-inspection/snapshot", dependencies=[Security(verify_token)])
def incoming_inspection_snapshot():
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        # Part_v_Container no expone Part_No/Operation_No directamente —
        # se resuelven vía Part_v_Part (Part_Key) y Part_v_Part_Operation
        # (Part_Operation_Key), confirmado empíricamente contra el schema real.
        cursor.execute(f"""
            SELECT
                c.Container_Key,
                p.Part_No,
                c.Part_Operation_Key,
                po.Operation_No,
                c.Location,
                c.Quantity,
                c.Active
            FROM Part_v_Container c
            LEFT JOIN Part_v_Part p
                ON c.Part_Key = p.Part_Key
               AND c.Plexus_Customer_No = p.Plexus_Customer_No
            LEFT JOIN Part_v_Part_Operation po
                ON c.Part_Operation_Key = po.Part_Operation_Key
               AND c.Plexus_Customer_No = po.Plexus_Customer_No
            WHERE c.Plexus_Customer_No = {PCN}
              AND c.Location LIKE '%TJ Incoming Inspection%'
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class IncomingInspectionHistoryRequest(BaseModel):
    since: str  # 'YYYY-MM-DD HH:MM:SS'


@app.post("/incoming-inspection/history", dependencies=[Security(verify_token)])
def incoming_inspection_history(req: IncomingInspectionHistoryRequest):
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        # Mismo caso que /incoming-inspection/snapshot: Part_v_Container_Change2
        # tampoco expone Part_No/Operation_No directamente.
        cursor.execute(f"""
            SELECT
                cc.Serial_No,
                p.Part_No,
                cc.Part_Key,
                po.Operation_No,
                cc.Change_Date,
                cc.Last_Action,
                cc.Location,
                cc.Container_Status,
                cc.Defect_Type,
                cc.Note,
                cc.Change_By
            FROM Part_v_Container_Change2 cc
            LEFT JOIN Part_v_Part p
                ON cc.Part_Key = p.Part_Key
               AND cc.Plexus_Customer_No = p.Plexus_Customer_No
            LEFT JOIN Part_v_Part_Operation po
                ON cc.Part_Operation_Key = po.Part_Operation_Key
               AND cc.Plexus_Customer_No = po.Plexus_Customer_No
            WHERE cc.Plexus_Customer_No = {PCN}
              AND po.Operation_No IN (10, 11, 20)
              AND cc.Change_Date >= '{req.since}'
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class UserLookupRequest(BaseModel):
    user_nos: list[int]


@app.post("/user-lookup", dependencies=[Security(verify_token)])
def user_lookup(req: UserLookupRequest):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        nos_in_clause = "(" + ", ".join(str(n) for n in req.user_nos) + ")"
        cursor.execute(f"""
            SELECT u.Plexus_User_No, u.First_Name, u.Last_Name
            FROM Plexus_Control_v_Plexus_User_e AS u
            WHERE u.Plexus_Customer_No = {PCN}
              AND u.Plexus_User_No IN {nos_in_clause}
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── COGP: Cost Model resolution ───────────────────────────────────────────────

@app.get("/cogp/cost-model", dependencies=[Security(verify_token)])
def cogp_cost_model():
    """
    Resuelve el Cost_Model_Key primario vigente. Nunca hardcodear este valor
    en el consumidor (Django) -- el modelo primario cambia cada año fiscal
    (confirmado: 5689 "2025 Standards" quedo obsoleto al activarse 5868
    "2026 Standards" como Primary_Model=1).
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT cm.Cost_Model_Key, cm.Cost_Model
            FROM Part_v_Cost_Model cm
            WHERE cm.PCN = {PCN}
              AND cm.Primary_Model = 1
              AND cm.Active = 1
        """)
        rows = cursor.fetchall()
        conn.close()

        if len(rows) != 1:
            raise HTTPException(
                status_code=409,
                detail=f"Se esperaba exactamente 1 Cost_Model con Primary_Model=1 Active=1, se encontraron {len(rows)}"
            )

        return {"cost_model_key": rows[0][0], "cost_model_name": rows[0][1]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── COGP: Scrap por Report_Date ───────────────────────────────────────────────

class CogpDateRequest(BaseModel):
    report_date: str  # YYYY-MM-DD


@app.post("/cogp/scrap-by-date", dependencies=[Security(verify_token)])
def cogp_scrap_by_date(req: CogpDateRequest):
    try:
        start = f"{req.report_date} 00:00:00"
        from datetime import datetime, timedelta
        end_dt = datetime.strptime(req.report_date, "%Y-%m-%d") + timedelta(days=1)
        end = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                s.Report_Date                 AS Report_Date,
                s.Scrap_Date                  AS Time_Scrapped,
                p.Part_No,
                p.Part_Type,
                s.Serial_No,
                s.Quantity,
                s.Net_Weight                  AS Weight,
                s.Scrap_Reason,
                wc.Name                       AS Workcenter,
                wc.Workcenter_Group,
                d.Name                        AS Department,
                s.Unit_Cost,
                s.Extended_Cost,
                s.Note
            FROM Part_v_Scrap s
            INNER JOIN Part_v_Workcenter wc
                ON s.Workcenter_Key = wc.Workcenter_Key
                AND s.Plexus_Customer_No = wc.Plexus_Customer_No
            INNER JOIN Part_v_Part_e p
                ON s.Part_Key = p.Part_Key
            LEFT JOIN Common_v_Department d
                ON wc.Department_No = d.Department_No
            WHERE s.Plexus_Customer_No = {PCN}
              AND s.Report_Date >= '{start}'
              AND s.Report_Date <  '{end}'
            ORDER BY wc.Name, s.Scrap_Date
        """)
        raw = query_to_list(cursor)
        conn.close()

        data = []
        for row in raw:
            rd = datetime.fromisoformat(row["Report_Date"])
            row["Report_Date"] = (rd + timedelta(hours=3)).date().isoformat()
            data.append(row)

        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── COGP: Producción por Report_Date (SIN filtro de workcenter) ──────────────

class CogpProductionRequest(BaseModel):
    report_date: str    # YYYY-MM-DD
    cost_model_key: int  # resuelto previamente via /cogp/cost-model


# Workcenters terminales confirmados contra el reporte nativo de Plex COGP
# (sesión 2026-07-28) -- Frontal 2/Final 3/Frontal 3/Soldadura de Siphon son
# operaciones intermedias y NO deben contarse como produccion terminada.
COGP_TERMINAL_WORKCENTERS = ('HM Ensamble Final 2', 'HM Ensamble de Servicio', 'TULC Ensamble Final')
WC_LIST_COGP = "', '".join(COGP_TERMINAL_WORKCENTERS)

@app.post("/cogp/production-by-date", dependencies=[Security(verify_token)])
def cogp_production_by_date(req: CogpProductionRequest):
    try:
        start = f"{req.report_date} 00:00:00"
        from datetime import datetime, timedelta
        end_dt = datetime.strptime(req.report_date, "%Y-%m-%d") + timedelta(days=1)
        end = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                wc.Name       AS Workcenter,
                p.Part_No,
                SUM(pe.Quantity) AS Quantity,
                ROUND(SUM(pe.Quantity * ISNULL(pc.Cost, 0)), 2) AS Extended_Cost
            FROM Part_v_Production_e pe
            LEFT JOIN Part_v_Part_e p
                ON pe.Part_Key = p.Part_Key
                AND p.Plexus_Customer_No = {PCN}
            LEFT JOIN Part_v_Workcenter wc
                ON pe.Workcenter_Key = wc.Workcenter_Key
                AND wc.Plexus_Customer_No = {PCN}
            LEFT JOIN Part_v_Part_Cost pc
                ON pe.Part_Key = pc.Part_Key
                AND pc.PCN = {PCN}
                AND pc.Cost_Model_Key = {req.cost_model_key}
            WHERE pe.Plexus_Customer_No = {PCN}
              AND pe.Report_Date >= '{start}'
              AND pe.Report_Date <  '{end}'
              AND wc.Name IN ('{WC_LIST_COGP}')
            GROUP BY wc.Name, p.Part_No
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── COGP: Customer/Part mapping (para sync de CustomerPartMapping) ───────────

@app.get("/cogp/customer-part-mapping", dependencies=[Security(verify_token)])
def cogp_customer_part_mapping():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT
                p.Part_No,
                p.Name         AS Part_Name,
                p.Part_Status,
                cp.Customer_No,
                c.Name         AS Customer_Name,
                cp.Customer_Part_No
            FROM Part_v_Part AS p
            LEFT JOIN Part_v_Customer_Part AS cp
                ON p.Part_Key = cp.Part_Key
               AND p.Plexus_Customer_No = cp.Plexus_Customer_No
            LEFT JOIN Common_v_Customer AS c
                ON cp.Customer_No = c.Customer_No
               AND cp.Plexus_Customer_No = c.Plexus_Customer_No
            WHERE p.Plexus_Customer_No = {PCN}
              AND p.Part_Status = 'Active'
            ORDER BY p.Part_No
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── COGP: Scrap por rango (agregado, sin loop) ────────────────────────────────

class CogpRangeRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date:   str  # YYYY-MM-DD (inclusive)


@app.post("/cogp/scrap-range", dependencies=[Security(verify_token)])
def cogp_scrap_range(req: CogpRangeRequest):
    try:
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(req.start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(req.end_date,   "%Y-%m-%d")
        if (end_dt - start_dt).days > 180:
            raise HTTPException(status_code=400, detail="Rango maximo 180 dias.")

        start = f"{req.start_date} 00:00:00"
        end   = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
             SELECT
                s.Report_Date                AS Report_Date,
                wc.Workcenter_Group          AS Workcenter_Group,
                wc.Name                      AS Workcenter,
                s.Scrap_Reason                AS Scrap_Reason,
                s.Quantity                   AS Quantity,
                s.Extended_Cost              AS Extended_Cost
            FROM Part_v_Scrap s
            INNER JOIN Part_v_Workcenter wc
                ON s.Workcenter_Key = wc.Workcenter_Key
                AND s.Plexus_Customer_No = wc.Plexus_Customer_No
            WHERE s.Plexus_Customer_No = {PCN}
              AND s.Report_Date >= '{start}'
              AND s.Report_Date <  '{end}'
              AND wc.Workcenter_Group IN ('Heater Module', 'TULC')
        """)
        raw = query_to_list(cursor)
        conn.close()

        data = []
        for row in raw:
            rd = datetime.fromisoformat(row["Report_Date"])
            row["Report_Date"] = (rd + timedelta(hours=3)).date().isoformat()
            data.append(row)

        return {"data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ─── COGP: Produccion por rango (agregado, sin loop) ───────────────────────────

class CogpProductionRangeRequest(BaseModel):
    start_date: str
    end_date:   str
    cost_model_key: int


@app.post("/cogp/production-range", dependencies=[Security(verify_token)])
def cogp_production_range(req: CogpProductionRangeRequest):
    try:
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(req.start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(req.end_date,   "%Y-%m-%d")
        if (end_dt - start_dt).days > 180:
            raise HTTPException(status_code=400, detail="Rango maximo 180 dias.")

        start = f"{req.start_date} 00:00:00"
        end   = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
           SELECT
                pe.Report_Date AS Report_Date,
                wc.Name        AS Workcenter,
                SUM(pe.Quantity)                      AS Quantity,
                SUM(pe.Quantity * ISNULL(pc.Cost, 0)) AS Extended_Cost
            FROM Part_v_Production_e pe
            LEFT JOIN Part_v_Workcenter wc
                ON pe.Workcenter_Key = wc.Workcenter_Key
                AND wc.Plexus_Customer_No = {PCN}
            LEFT JOIN Part_v_Part_Cost pc
                ON pe.Part_Key = pc.Part_Key
                AND pc.PCN = {PCN}
                AND pc.Cost_Model_Key = {req.cost_model_key}
            WHERE pe.Plexus_Customer_No = {PCN}
              AND pe.Report_Date >= '{start}'
              AND pe.Report_Date <  '{end}'
              AND wc.Name IN ('{WC_LIST_COGP}')
            GROUP BY pe.Report_Date, wc.Name
        """)
        raw = query_to_list(cursor)
        conn.close()

        data = []
        for row in raw:
            rd = datetime.fromisoformat(row["Report_Date"])
            row["Report_Date"] = (rd + timedelta(hours=3)).date().isoformat()
            data.append(row)

        return {"data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── Workcenters (catálogo para Downtime Settings) ────────────────────────────

@app.get("/workcenters", dependencies=[Security(verify_token)])
def workcenters():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT
                wc.Name AS Workcenter,
                wc.Workcenter_Group
            FROM Part_v_Workcenter wc
            WHERE wc.Plexus_Customer_No = {PCN}
              AND wc.Workcenter_Group IN ('Heater Module', 'TULC')
            ORDER BY wc.Name
        """)
        data = query_to_list(cursor)
        conn.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
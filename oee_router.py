# apps/plex-proxy/oee_router.py
"""
Router aislado para /oee-live.

Extraido de main.py SIN cambios de logica (misma query, mismo calculo),
solo para poder iterar en el diagnostico del "OEE semanal disparado"
sin arriesgar el resto del proxy (los otros endpoints ya corregidos
con get_shift_window siguen viviendo en main.py, intactos).

Es autocontenido: define su propia conexion/PCN/get_shift_window en vez
de importarlos de main.py, para evitar el riesgo de import circular
(main.py importaria este router, y este router tendria que importar de
vuelta de main.py mientras main.py todavia se esta cargando).
"""

import os
from datetime import date as dt, datetime, timedelta
from collections import defaultdict

import pyodbc
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

DSN      = os.getenv("PLEX_DSN")
USERNAME = os.getenv("PLEX_USERNAME")
PASSWORD = os.getenv("PLEX_PASSWORD")
PCN      = 306713

router = APIRouter()


def get_connection():
    conn_str = (
        f"DSN={DSN};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=60)


def get_shift_window(start_date: str, end_date: str, offset_hours: int = 6) -> tuple[str, str]:
    """
    Ventana de turno [start_date + offset_hours, end_date + 1 dia + offset_hours).
    Calculado en Python e inyectado como string literal -- DATEADD(...) dentro
    de un WHERE no funciona en el driver ODBC de Plex (confirmado empiricamente
    contra Part_v_Workcenter_Log).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(hours=offset_hours)
    end   = datetime.strptime(end_date,   "%Y-%m-%d") + timedelta(days=1, hours=offset_hours)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


class OEERequest(BaseModel):
    start_date: str
    end_date: str


@router.post("/oee-live")
def oee_live(req: OEERequest):
    try:
        dt.fromisoformat(req.start_date)
        dt.fromisoformat(req.end_date)
        shift_start, shift_end = get_shift_window(req.start_date, req.end_date)

        with get_connection() as conn:
            cursor = conn.cursor()

            # ── 1. Produccion detallada por operacion ──────────────────────
            # Report_Date ya viene pre-clasificado por dia de negocio en Plex
            # (no es timestamp crudo como Log_Date) -- calendario-dia es
            # correcto aqui, validado exacto contra reportes reales de Plex.
            cursor.execute(f"""
                SELECT
                    pt.Part_No, pt.Revision, wc.Name AS Workcenter,
                    pe.Part_Operation_Key, SUM(pe.Quantity) AS Good_Qty
                FROM Part_v_Production_e AS pe
                INNER JOIN Part_v_Workcenter AS wc
                    ON pe.Workcenter_Key = wc.Workcenter_Key
                    AND pe.Plexus_Customer_No = wc.Plexus_Customer_No
                INNER JOIN Part_v_Part AS pt
                    ON pe.Part_Key = pt.Part_Key
                    AND pe.Plexus_Customer_No = pt.Plexus_Customer_No
                WHERE pe.Plexus_Customer_No = {PCN}
                    AND CAST(pe.Report_Date AS DATE) >= '{req.start_date}'
                    AND CAST(pe.Report_Date AS DATE) <= '{req.end_date}'
                GROUP BY pt.Part_No, pt.Revision, wc.Name, pe.Part_Operation_Key
            """)
            production_detail = cursor.fetchall()

            # ── 2. Tasa ideal por operacion ─────────────────────────────────
            # SOSPECHA ACTIVA (diagnostico "OEE semanal disparado"): esta query
            # NO filtra por Plexus_Customer_No -- trae Ideal_Rate de todos los
            # tenants que el ODBC virtualiza, agrupado solo por
            # (Part_Operation_Key, Workcenter_Name). Si un Part_Operation_Key
            # de otro tenant coincide en numero y su workcenter comparte
            # nombre de texto con uno nuestro, el MAX() puede mezclar una tasa
            # que no es la nuestra. Se agrego "_ideal_rate_used" por fila abajo
            # para poder ver, fila por fila, que tasa se aplico realmente.
            cursor.execute("""
                SELECT aw.Part_Operation_Key, wc.Name AS Workcenter, MAX(aw.Ideal_Rate) AS Ideal_Rate
                FROM Part_v_Approved_Workcenter AS aw
                INNER JOIN Part_v_Workcenter AS wc
                    ON aw.Workcenter_Key = wc.Workcenter_Key
                GROUP BY aw.Part_Operation_Key, wc.Name
            """)
            ideal_rate_detail = {(r[0], r[1]): float(r[2]) for r in cursor.fetchall() if r[2]}

            # ── 3. Scrap ─────────────────────────────────────────────────────
            cursor.execute(f"""
                SELECT pt.Part_No, pt.Revision, wc.Name AS Workcenter, SUM(s.Quantity) AS Scrap_Qty
                FROM Part_v_Scrap AS s
                INNER JOIN Part_v_Workcenter AS wc
                    ON s.Workcenter_Key = wc.Workcenter_Key
                    AND s.Plexus_Customer_No = wc.Plexus_Customer_No
                INNER JOIN Part_v_Part AS pt
                    ON s.Part_Key = pt.Part_Key
                    AND s.Plexus_Customer_No = pt.Plexus_Customer_No
                WHERE s.Plexus_Customer_No = {PCN}
                    AND CAST(s.Scrap_Date AS DATE) >= '{req.start_date}'
                    AND CAST(s.Scrap_Date AS DATE) <= '{req.end_date}'
                GROUP BY pt.Part_No, pt.Revision, wc.Name
            """)
            scrap = {(r[0], r[1], r[2]): float(r[3] or 0) for r in cursor.fetchall()}

            # ── 4. Horas operando / planeadas ───────────────────────────────
            # Ya usa get_shift_window (offset de 6 horas), validado exacto
            # contra reportes reales de Plex.
            cursor.execute(f"""
                SELECT
                    pt.Part_No, pt.Revision, wc.Name AS Workcenter,
                    ISNULL(SUM(CASE WHEN wl.Workcenter_Status_Key = 5448 THEN wl.Log_Hours ELSE 0 END), 0) AS Operating_Hours,
                    ISNULL(SUM(CASE WHEN wl.Workcenter_Status_Key IN (5448,5445,5449) THEN wl.Log_Hours ELSE 0 END), 0) AS Plan_Hours
                FROM Part_v_Workcenter_Log AS wl
                INNER JOIN Part_v_Part AS pt ON wl.Part_Key = pt.Part_Key
                INNER JOIN Part_v_Workcenter AS wc ON wl.Workcenter_Key = wc.Workcenter_Key
                WHERE wl.Plexus_Customer_No = {PCN}
                    AND wl.Log_Date >= '{shift_start}'
                    AND wl.Log_Date <  '{shift_end}'
                GROUP BY pt.Part_No, pt.Revision, wc.Name
            """)
            operating_hours = {(r[0], r[1], r[2]): (float(r[3] or 0), float(r[4] or 0)) for r in cursor.fetchall()}

        # ── Agregacion en Python (el driver ODBC de Plex no soporta CTEs) ────
        good_qty_by_key         = defaultdict(float)
        ideal_hours_good_by_key = defaultdict(float)
        for part_no, revision, workcenter, op_key, qty in production_detail:
            key  = (part_no, revision, workcenter)
            qty  = float(qty or 0)
            good_qty_by_key[key] += qty
            rate = ideal_rate_detail.get((op_key, workcenter))
            if rate:
                ideal_hours_good_by_key[key] += qty / rate

        effective_ideal_rate = {
            key: good_qty_by_key[key] / ideal_hours_good_by_key[key]
            for key in ideal_hours_good_by_key
            if ideal_hours_good_by_key[key] > 0
        }

        all_keys = set(good_qty_by_key) | set(operating_hours)

        details = []
        totals = {"good_qty": 0.0, "scrap_qty": 0.0, "total_qty": 0.0,
                  "operating_hours": 0.0, "plan_hours": 0.0, "ideal_hours_total": 0.0}

        for key in all_keys:
            part_no, revision, workcenter = key
            good_qty  = good_qty_by_key.get(key, 0.0)
            scrap_qty = scrap.get(key, 0.0)
            total_qty = good_qty + scrap_qty
            op_hrs, plan_hrs = operating_hours.get(key, (0.0, 0.0))
            eff_rate  = effective_ideal_rate.get(key)

            ideal_hours_total = (
                ideal_hours_good_by_key.get(key, 0.0) + (scrap_qty / eff_rate)
                if eff_rate else None
            )

            availability_pct = round(op_hrs * 100.0 / plan_hrs, 2) if plan_hrs > 0 else 0.0
            performance_pct  = (
                round(ideal_hours_total * 100.0 / op_hrs, 2)
                if (ideal_hours_total is not None and op_hrs > 0) else 0.0
            )
            quality_pct = round(good_qty * 100.0 / total_qty, 2) if total_qty > 0 else 0.0
            oee_pct     = round(
                min((availability_pct / 100) * (performance_pct / 100) * (quality_pct / 100) * 100, 100.0)
            , 2)

            details.append({
                "part_workcenter":  f"{part_no} Rev:{revision} - {workcenter}",
                "good_qty":         good_qty,
                "scrap_qty":        scrap_qty,
                "total_qty":        total_qty,
                "availability_pct": availability_pct,
                "performance_pct":  performance_pct,
                "quality_pct":      quality_pct,
                "oee_pct":          oee_pct,
                # TEMPORAL para diagnostico del "OEE semanal disparado" --
                # quitar este campo una vez resuelto. Deja ver a simple vista
                # que tasa (Ideal_Rate efectiva) se aplico a cada fila.
                "_ideal_rate_used": eff_rate,
            })

            totals["good_qty"]        += good_qty
            totals["scrap_qty"]       += scrap_qty
            totals["total_qty"]       += total_qty
            totals["operating_hours"] += op_hrs
            totals["plan_hours"]      += plan_hrs
            if ideal_hours_total is not None:
                totals["ideal_hours_total"] += ideal_hours_total

        total_availability = round(totals["operating_hours"] * 100.0 / totals["plan_hours"], 2) if totals["plan_hours"] > 0 else 0.0
        total_performance  = round(totals["ideal_hours_total"] * 100.0 / totals["operating_hours"], 2) if totals["operating_hours"] > 0 else 0.0
        total_quality      = round(totals["good_qty"] * 100.0 / totals["total_qty"], 2) if totals["total_qty"] > 0 else 0.0
        total_oee          = round(
            min((total_availability / 100) * (total_performance / 100) * (total_quality / 100) * 100, 100.0)
        , 2)

        total = {
            "part_workcenter":  "TOTAL",
            "good_qty":         totals["good_qty"],
            "scrap_qty":        totals["scrap_qty"],
            "total_qty":        totals["total_qty"],
            "availability_pct": total_availability,
            "performance_pct":  total_performance,
            "quality_pct":      total_quality,
            "oee_pct":          total_oee,
        }

        # Ordenar por _ideal_rate_used ayuda a ver de un vistazo si hay tasas
        # sospechosamente bajas o altas arriba/abajo de la lista.
        details.sort(key=lambda d: d["part_workcenter"], reverse=True)
        return {"data": {"details": details, "total": total}}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
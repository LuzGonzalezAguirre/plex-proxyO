"""
settings_router.py — QWall Proxy
Endpoints de configuración: catálogos leídos desde SQL Server CCS.

Registro en main.py:
    from settings_router import router as settings_router
    app.include_router(settings_router, prefix="/settings", tags=["Settings"])

Endpoints:
    GET /settings/part-numbers-lookup  — catálogo de Part Numbers para Scan Rules
"""
import os
from typing import Optional

import pyodbc
from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# ── Auth ──────────────────────────────────────────────────────────────────────

SECRET  = os.getenv("PROXY_SECRET", "")
_bearer = HTTPBearer()


def _verify(credentials: HTTPAuthorizationCredentials = Security(_bearer)):
    if credentials.credentials != SECRET:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials


# ── Conexión SQL Server CCS ───────────────────────────────────────────────────

_CCS_CONN_STR = os.getenv(
    "QWALL_DB_CONN_STR",
    "SERVER=AAS-PAC-FTP01;DATABASE=CCS;Trusted_Connection=yes;",
)


def _conn():
    return pyodbc.connect(_CCS_CONN_STR, timeout=30)


# ── Router ────────────────────────────────────────────────────────────────────

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# GET /settings/part-numbers-lookup
#
# Retorna el catálogo de Part Numbers desde SQL Server CCS para que Django
# pueda validar pn_id y obtener ssi_pn / bu_id / bu_name al crear/editar
# una Scan Rule.
#
# IMPORTANTE: ajusta la query SQL a tu schema real de CCS.
# La tabla y columnas a continuación son un ejemplo típico — verifica contra
# el schema de producción antes de ejecutar.
#
# Formato de respuesta esperado por scan_rules_service.py:
#   {
#     "data": [
#       { "pn_id": 1, "ssiPN": "43301", "bu_id": 2, "bu_name": "Volvo" },
#       ...
#     ]
#   }
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/part-numbers-lookup", dependencies=[Security(_verify)])
def part_numbers_lookup(
    bu_id: Optional[int] = Query(default=None, description="Filtrar por Business Unit"),
):
    con = _conn()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT
                pn_id,
                ssiPN,
                bu_id,
                CASE bu_id
                    WHEN 1 THEN 'Volvo'
                    WHEN 2 THEN 'John Deere'
                    WHEN 3 THEN 'Cummins'
                    WHEN 4 THEN 'Harley-Davidson'
                    WHEN 5 THEN 'Eaton'
                    ELSE ''
                END AS bu_name
            FROM  [dbo].[ssi_PartNumbers]
            ORDER BY ssiPN ASC
        """)

        rows = cur.fetchall()

        data = []
        for row in rows:
            data.append({
                "pn_id":   row[0],
                "ssiPN":   str(row[1] or "").strip(),
                "bu_id":   row[2],
                "bu_name": str(row[3] or "").strip(),
            })

        if bu_id is not None:
            data = [d for d in data if d["bu_id"] == bu_id]

        return {"data": data}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()

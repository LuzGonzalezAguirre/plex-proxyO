"""
scan_rules_router.py — QWall Proxy
CRUD completo de Scan Rules sobre SQL Server CCS (AAS-PAC-FTP01).

Registro en main.py:
    from scan_rules_router import router as scan_rules_router
    app.include_router(scan_rules_router, prefix="/scan-rules", tags=["Scan Rules"])

Variable de entorno requerida:
    QWALL_DB_CONN_STR — connection string pyodbc hacia SQL Server CCS
    PROXY_SECRET      — Bearer token (compartido con el resto del proxy)
"""
import os
from datetime import datetime, timezone
from typing import Optional

import pyodbc
from fastapi import APIRouter, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator

# ── Autenticación (mismo esquema Bearer que el resto del proxy) ───────────────

SECRET   = os.getenv("PROXY_SECRET", "")
_bearer  = HTTPBearer()


def _verify(credentials: HTTPAuthorizationCredentials = Security(_bearer)):
    if credentials.credentials != SECRET:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials


# ── Conexión a SQL Server CCS ─────────────────────────────────────────────────

_CCS_CONN_STR = os.getenv(
    "QWALL_DB_CONN_STR",
    "SERVER=AAS-PAC-FTP01;DATABASE=CCS;Trusted_Connection=yes;",
)


def _conn():
    return pyodbc.connect(_CCS_CONN_STR, timeout=30)


# ── Valores permitidos (mirrors de los choices Django) ────────────────────────

_EXTRACTION_MODES  = {"completo", "por_separador", "pegado_longitud", "segmento"}
_SEPARATORS        = {"espacio", "apostrofe", "guion", "guion_bajo", "pipe", "ninguno", "custom"}
_VALUE_POSITIONS   = {"completo", "antes", "despues", "segmento"}
_FIELD_TARGETS     = {"frameSN", "volvoSerialNumber", "descartado"}


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class ScanFieldIn(BaseModel):
    scan_index:       int
    extraction_mode:  str = "completo"
    field_target:     str
    separator:        str = "ninguno"
    separator_custom: str = ""
    value_position:   str = "completo"
    segment_index:    Optional[int] = None
    fixed_length:     Optional[int] = None
    prefix_value:     str = ""
    display_label:    str
    sequence_order:   int = 0

    @field_validator("extraction_mode")
    @classmethod
    def _chk_mode(cls, v):
        if v not in _EXTRACTION_MODES:
            raise ValueError(f"extraction_mode inválido: {v}")
        return v

    @field_validator("field_target")
    @classmethod
    def _chk_target(cls, v):
        if v not in _FIELD_TARGETS:
            raise ValueError(f"field_target inválido: {v}")
        return v

    @field_validator("separator")
    @classmethod
    def _chk_sep(cls, v):
        if v not in _SEPARATORS:
            raise ValueError(f"separator inválido: {v}")
        return v

    @field_validator("value_position")
    @classmethod
    def _chk_vpos(cls, v):
        if v not in _VALUE_POSITIONS:
            raise ValueError(f"value_position inválido: {v}")
        return v


class ScanRuleIn(BaseModel):
    pn_id:          int
    ssi_pn:         str = Field(max_length=20)
    bu_id:          int
    bu_name:        str = Field(max_length=50)
    scan_count:     int = Field(default=1, ge=1)
    requires_match: bool = False
    notes:          str = ""
    is_active:      bool = True
    created_by_id:  Optional[int] = None
    scan_fields:    list[ScanFieldIn] = []


class ScanRuleUpdate(BaseModel):
    pn_id:          Optional[int]  = None
    ssi_pn:         Optional[str]  = None
    bu_id:          Optional[int]  = None
    bu_name:        Optional[str]  = None
    scan_count:     Optional[int]  = Field(default=None, ge=1)
    requires_match: Optional[bool] = None
    notes:          Optional[str]  = None
    is_active:      Optional[bool] = None
    updated_by_id:  Optional[int]  = None
    scan_fields:    Optional[list[ScanFieldIn]] = None


class ToggleBody(BaseModel):
    updated_by_id: Optional[int] = None


# ── Helpers internos ──────────────────────────────────────────────────────────

_RULE_COLS = (
    "id", "pn_id", "ssi_pn", "bu_id", "bu_name",
    "scan_count", "requires_match", "notes", "is_active",
    "created_by_id", "created_at", "updated_by_id", "updated_at",
)

_FIELD_COLS = (
    "id", "scan_index", "extraction_mode", "field_target",
    "separator", "separator_custom", "value_position",
    "segment_index", "fixed_length", "prefix_value",
    "display_label", "sequence_order",
)


def _iso(val):
    return val.isoformat() if isinstance(val, datetime) else val


def _rule_row(row: tuple) -> dict:
    r = dict(zip(_RULE_COLS, row))
    r["requires_match"] = bool(r["requires_match"])
    r["is_active"]      = bool(r["is_active"])
    r["created_at"]     = _iso(r["created_at"])
    r["updated_at"]     = _iso(r["updated_at"])
    return r


def _field_row(row: tuple) -> dict:
    return dict(zip(_FIELD_COLS, row))


def _fetch_rule_with_fields(cursor, rule_id: int) -> Optional[dict]:
    """Lee una regla completa con sus scan_fields desde SQL Server."""
    cursor.execute(
        """
        SELECT id, pn_id, ssi_pn, bu_id, bu_name,
               scan_count, requires_match, notes, is_active,
               created_by_id, created_at, updated_by_id, updated_at
        FROM   [dbo].[quality_pn_scan_rules]
        WHERE  id = ?
        """,
        rule_id,
    )
    row = cursor.fetchone()
    if row is None:
        return None

    rule = _rule_row(row)

    cursor.execute(
        """
        SELECT id, scan_index, extraction_mode, field_target,
               separator, separator_custom, value_position,
               segment_index, fixed_length, prefix_value,
               display_label, sequence_order
        FROM   [dbo].[quality_scan_fields]
        WHERE  rule_id = ?
        ORDER BY scan_index ASC, sequence_order ASC
        """,
        rule_id,
    )
    scan_fields = [_field_row(r) for r in cursor.fetchall()]
    rule["scan_fields"] = scan_fields
    rule["field_count"] = len(scan_fields)
    return rule


def _insert_fields(cursor, rule_id: int, fields: list[ScanFieldIn]):
    """Inserta scan_fields de una regla. Sin commit propio."""
    for f in fields:
        cursor.execute(
            """
            INSERT INTO [dbo].[quality_scan_fields]
                (rule_id, scan_index, extraction_mode, field_target,
                 separator, separator_custom, value_position,
                 segment_index, fixed_length, prefix_value,
                 display_label, sequence_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rule_id,
            f.scan_index,
            f.extraction_mode,
            f.field_target,
            f.separator,
            f.separator_custom or "",
            f.value_position,
            f.segment_index,
            f.fixed_length,
            f.prefix_value or "",
            f.display_label,
            f.sequence_order,
        )


# ── Router ────────────────────────────────────────────────────────────────────

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# GET /scan-rules/
# Query params: bu_id, is_active, pn_id
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/", dependencies=[Security(_verify)])
def list_scan_rules(
    bu_id:     Optional[int]  = Query(default=None, description="Filtrar por Business Unit"),
    is_active: Optional[bool] = Query(default=None, description="Filtrar por estado activo"),
    pn_id:     Optional[int]  = Query(default=None, description="Filtrar por Part Number ID"),
):
    con = _conn()
    cur = con.cursor()
    try:
        clauses: list[str] = []
        params:  list      = []

        if bu_id is not None:
            clauses.append("r.bu_id = ?")
            params.append(bu_id)
        if is_active is not None:
            clauses.append("r.is_active = ?")
            params.append(1 if is_active else 0)
        if pn_id is not None:
            clauses.append("r.pn_id = ?")
            params.append(pn_id)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        cur.execute(
            f"""
            SELECT id, pn_id, ssi_pn, bu_id, bu_name,
                   scan_count, requires_match, notes, is_active,
                   created_by_id, created_at, updated_by_id, updated_at
            FROM   [dbo].[quality_pn_scan_rules] r
            {where}
            ORDER BY r.ssi_pn ASC
            """,
            *params,
        )
        rule_rows = cur.fetchall()

        if not rule_rows:
            return {"data": []}

        # Una sola query para todos los campos → evita N+1
        rule_ids     = [row[0] for row in rule_rows]
        placeholders = ",".join("?" * len(rule_ids))

        cur.execute(
            f"""
            SELECT rule_id, id, scan_index, extraction_mode, field_target,
                   separator, separator_custom, value_position,
                   segment_index, fixed_length, prefix_value,
                   display_label, sequence_order
            FROM   [dbo].[quality_scan_fields]
            WHERE  rule_id IN ({placeholders})
            ORDER BY rule_id ASC, scan_index ASC, sequence_order ASC
            """,
            *rule_ids,
        )
        # Agrupar campos por rule_id
        fields_by_rule: dict[int, list] = {rid: [] for rid in rule_ids}
        for row in cur.fetchall():
            rule_id_key = row[0]        # primera columna es rule_id
            fd = dict(zip(("id", "scan_index", "extraction_mode", "field_target",
                           "separator", "separator_custom", "value_position",
                           "segment_index", "fixed_length", "prefix_value",
                           "display_label", "sequence_order"), row[1:]))
            fields_by_rule[rule_id_key].append(fd)

        result = []
        for row in rule_rows:
            rule = _rule_row(row)
            flds = fields_by_rule.get(rule["id"], [])
            rule["scan_fields"] = flds
            rule["field_count"] = len(flds)
            result.append(rule)

        return {"data": result}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# POST /scan-rules/
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/", status_code=201, dependencies=[Security(_verify)])
def create_scan_rule(body: ScanRuleIn):
    con = _conn()
    cur = con.cursor()
    try:
        # Unicidad de pn_id
        cur.execute(
            "SELECT id FROM [dbo].[quality_pn_scan_rules] WHERE pn_id = ?",
            body.pn_id,
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail=f"Ya existe una regla para pn_id={body.pn_id}.",
            )

        now = datetime.now(timezone.utc)

        cur.execute(
            """
            INSERT INTO [dbo].[quality_pn_scan_rules]
                (pn_id, ssi_pn, bu_id, bu_name,
                 scan_count, requires_match, notes, is_active,
                 created_by_id, created_at, updated_by_id, updated_at)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            body.pn_id,
            body.ssi_pn,
            body.bu_id,
            body.bu_name,
            body.scan_count,
            1 if body.requires_match else 0,
            body.notes,
            1 if body.is_active else 0,
            body.created_by_id,
            now,
            body.created_by_id,
            now,
        )
        new_id = cur.fetchone()[0]

        _insert_fields(cur, new_id, body.scan_fields)
        con.commit()

        return _fetch_rule_with_fields(cur, new_id)

    except HTTPException:
        con.rollback()
        raise
    except Exception as exc:
        con.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# GET /scan-rules/{rule_id}
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/{rule_id}", dependencies=[Security(_verify)])
def get_scan_rule(rule_id: int):
    con = _conn()
    cur = con.cursor()
    try:
        rule = _fetch_rule_with_fields(cur, rule_id)
        if rule is None:
            raise HTTPException(status_code=404, detail="Regla no encontrada.")
        return rule
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /scan-rules/{rule_id}
# Solo actualiza los campos que vienen en el body (partial update).
# Si scan_fields viene en el body, reemplaza todos los campos de la regla.
# ─────────────────────────────────────────────────────────────────────────────
@router.patch("/{rule_id}", dependencies=[Security(_verify)])
def update_scan_rule(rule_id: int, body: ScanRuleUpdate):
    con = _conn()
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id, pn_id FROM [dbo].[quality_pn_scan_rules] WHERE id = ?",
            rule_id,
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Regla no encontrada.")

        current_pn_id = row[1]

        # Validar unicidad si cambia el pn_id
        if body.pn_id is not None and body.pn_id != current_pn_id:
            cur.execute(
                "SELECT id FROM [dbo].[quality_pn_scan_rules] WHERE pn_id = ? AND id <> ?",
                body.pn_id,
                rule_id,
            )
            if cur.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail=f"Ya existe una regla para pn_id={body.pn_id}.",
                )

        # Construir SET dinámico — solo campos que vienen en el body
        # requires_match / is_active: bool → BIT (1/0)
        candidates = {
            "pn_id":          body.pn_id,
            "ssi_pn":         body.ssi_pn,
            "bu_id":          body.bu_id,
            "bu_name":        body.bu_name,
            "scan_count":     body.scan_count,
            "requires_match": None if body.requires_match is None else (1 if body.requires_match else 0),
            "notes":          body.notes,
            "is_active":      None if body.is_active is None else (1 if body.is_active else 0),
            "updated_by_id":  body.updated_by_id,
            "updated_at":     datetime.now(timezone.utc),  # siempre se actualiza
        }

        set_clauses: list[str] = []
        set_params:  list      = []
        for col, val in candidates.items():
            if val is not None:
                set_clauses.append(f"{col} = ?")
                set_params.append(val)

        if set_clauses:
            set_params.append(rule_id)
            cur.execute(
                f"UPDATE [dbo].[quality_pn_scan_rules] SET {', '.join(set_clauses)} WHERE id = ?",
                *set_params,
            )

        # Reemplazar scan_fields si se enviaron
        if body.scan_fields is not None:
            cur.execute(
                "DELETE FROM [dbo].[quality_scan_fields] WHERE rule_id = ?",
                rule_id,
            )
            _insert_fields(cur, rule_id, body.scan_fields)

        con.commit()
        return _fetch_rule_with_fields(cur, rule_id)

    except HTTPException:
        con.rollback()
        raise
    except Exception as exc:
        con.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /scan-rules/{rule_id}
# ON DELETE CASCADE en quality_scan_fields elimina los campos automáticamente.
# ─────────────────────────────────────────────────────────────────────────────
@router.delete("/{rule_id}", status_code=204, dependencies=[Security(_verify)])
def delete_scan_rule(rule_id: int):
    con = _conn()
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id FROM [dbo].[quality_pn_scan_rules] WHERE id = ?",
            rule_id,
        )
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Regla no encontrada.")

        cur.execute(
            "DELETE FROM [dbo].[quality_pn_scan_rules] WHERE id = ?",
            rule_id,
        )
        con.commit()

    except HTTPException:
        con.rollback()
        raise
    except Exception as exc:
        con.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /scan-rules/{rule_id}/toggle
# Invierte is_active. Acepta JSON body con updated_by_id opcional.
# ─────────────────────────────────────────────────────────────────────────────
@router.patch("/{rule_id}/toggle", dependencies=[Security(_verify)])
def toggle_scan_rule(rule_id: int, body: ToggleBody = None):
    con = _conn()
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id, is_active FROM [dbo].[quality_pn_scan_rules] WHERE id = ?",
            rule_id,
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Regla no encontrada.")

        new_active    = 0 if row[1] else 1
        updated_by_id = (body.updated_by_id if body else None)

        cur.execute(
            """
            UPDATE [dbo].[quality_pn_scan_rules]
            SET    is_active = ?, updated_by_id = ?, updated_at = ?
            WHERE  id = ?
            """,
            new_active,
            updated_by_id,
            datetime.now(timezone.utc),
            rule_id,
        )
        con.commit()
        return {"id": rule_id, "is_active": bool(new_active)}

    except HTTPException:
        con.rollback()
        raise
    except Exception as exc:
        con.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        con.close()

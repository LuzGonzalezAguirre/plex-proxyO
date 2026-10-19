"""Contract tests for To Promise extraction (do not require ODBC)."""
from datetime import date

from fastapi import HTTPException
import pytest

from delivery_performance_router import (
    DeliveryRequest,
    build_query,
    make_delivery_performance_router,
)


def test_sql_uses_promise_date_and_release_aggregation():
    sql = build_query("2026-09-01", "2026-10-01", "2026-07-01", "2027-01-01")
    assert "b.Promise_Ship_Date >= '2026-09-01'" in sql
    assert "b.Promise_Ship_Date < '2026-10-01'" in sql
    assert "r.Quantity_Shipped > 0" in sql
    assert "src.PO_Line_Key = dst.PO_Line_Key" in sql
    assert "pair.Source_Matches = 1 AND pair.Target_Matches = 1" in sql
    assert "Delivery_Performance_Raw" in sql
    assert "DATEADD(hour, -7" in sql
    assert "DATEADD(hour, -8" in sql


def test_router_returns_release_rows_and_closes_connection():
    class Cursor:
        def execute(self, sql):
            self.sql = sql

    class Connection:
        def __init__(self):
            self._cursor = Cursor()
            self.closed = False

        def cursor(self):
            return self._cursor

        def close(self):
            self.closed = True

    conn = Connection()
    router = make_delivery_performance_router(lambda: conn, lambda cursor: [{"Release_Key": 1}], lambda: None)
    route = next(route for route in router.routes if route.path == "/delivery-performance")
    result = route.endpoint(DeliveryRequest(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30)))
    assert result == {"data": [{"Release_Key": 1}]}
    assert conn.closed
    assert "2026-09-01" in conn._cursor.sql


def test_router_rejects_invalid_window():
    router = make_delivery_performance_router(lambda: None, lambda _: [], lambda: None)
    route = next(route for route in router.routes if route.path == "/delivery-performance")
    with pytest.raises(HTTPException) as exc:
        route.endpoint(DeliveryRequest(start_date=date(2026, 9, 30), end_date=date(2026, 9, 1)))
    assert exc.value.status_code == 400

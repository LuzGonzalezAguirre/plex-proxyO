from shelf_life_router import SHELF_LIFE_SQL, make_shelf_life_router


def test_sql_is_read_only_and_filters_current_warehouse():
    assert SHELF_LIFE_SQL.lstrip().startswith("SELECT")
    assert "Part_v_Container AS c" in SHELF_LIFE_SQL
    assert "Part_v_Part_Shelf_Life AS psl" in SHELF_LIFE_SQL
    assert "c.Active = 1" in SHELF_LIFE_SQL
    assert "c.Location LIKE 'TJR%'" in SHELF_LIFE_SQL
    assert "DATEADD(month" in SHELF_LIFE_SQL


def test_endpoint_serializes_rows_and_closes_connection():
    class Cursor:
        def execute(self, sql):
            assert sql == SHELF_LIFE_SQL
    class Conn:
        def __init__(self):
            self.closed = False
        def cursor(self):
            return Cursor()
        def close(self):
            self.closed = True
    conn = Conn()
    router = make_shelf_life_router(lambda: conn, lambda cursor: [
        {"Serial_No": "P959523", "Expired_Date": "2026-10-20"}
    ], lambda: True)
    route = next(r for r in router.routes if r.path == "/shelf-life")
    result = route.endpoint()
    assert result["data"][0]["Serial_No"] == "P959523"
    assert conn.closed

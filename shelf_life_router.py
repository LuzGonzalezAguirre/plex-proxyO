"""Warehouse inventory shelf life, read-only Plex ODBC query."""
from fastapi import APIRouter, HTTPException, Security


SHELF_LIFE_SQL = """
SELECT
    c.Serial_No,
    c.Part_Key,
    p.Part_No,
    p.Revision,
    p.Name AS Part_Name,
    c.Quantity,
    c.Location,
    c.Container_Status,
    c.Shelf_Date,
    CASE
        WHEN psl.Unit = 'Month(s)' THEN DATEADD(month, psl.Shelf_Life, c.Shelf_Date)
        WHEN psl.Unit = 'Day(s)' THEN DATEADD(day, psl.Shelf_Life, c.Shelf_Date)
        WHEN psl.Unit = 'Year(s)' THEN DATEADD(year, psl.Shelf_Life, c.Shelf_Date)
        ELSE NULL
    END AS Expired_Date
FROM Part_v_Container AS c
INNER JOIN Part_v_Part AS p
    ON c.Part_Key = p.Part_Key
    AND c.Plexus_Customer_No = p.Plexus_Customer_No
INNER JOIN Part_v_Part_Shelf_Life AS psl
    ON c.Part_Key = psl.Part_Key
    AND c.Plexus_Customer_No = psl.PCN
WHERE
    c.Plexus_Customer_No = 306713
    AND c.Active = 1
    AND c.Quantity > 0
    AND c.Location LIKE 'TJR%'
    AND c.Shelf_Date IS NOT NULL
"""


def make_shelf_life_router(get_connection, query_to_list, verify_token):
    router = APIRouter()

    @router.post("/shelf-life", dependencies=[Security(verify_token)])
    def shelf_life():
        try:
            conn = get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(SHELF_LIFE_SQL)
                rows = query_to_list(cursor)
                return {"data": rows}
            finally:
                conn.close()
        except Exception:
            # Never leak ODBC connection strings or credentials in API errors.
            raise HTTPException(status_code=502, detail="Plex shelf-life query failed")

    return router

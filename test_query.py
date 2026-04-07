import pyodbc, os
from dotenv import load_dotenv
load_dotenv()

conn = pyodbc.connect(
    f"DSN={os.getenv('PLEX_DSN')};"
    f"UID={os.getenv('PLEX_USERNAME')};"
    f"PWD={os.getenv('PLEX_PASSWORD')};",
    timeout=60
)
cursor = conn.cursor()
cursor.execute("""
    SELECT
        YEAR(wl.Log_Date)                   AS Year,
        MONTH(wl.Log_Date)                  AS Month,
        ISNULL(we.Description, 'Sin Razon') AS Reason,
        COUNT(*)                            AS Total_Events,
        ROUND(SUM(wl.Log_Hours), 2)         AS Total_Hours
    FROM Part_v_Workcenter_Log wl
    LEFT JOIN Part_v_Workcenter_Event we
        ON wl.Workcenter_Event_Key = we.Workcenter_Event_Key
        AND wl.Plexus_Customer_No  = we.Plexus_Customer_No
    WHERE wl.Plexus_Customer_No = 306713
      AND wl.Log_Date >= '2026-01-01'
      AND wl.Log_Date <  '2026-05-01'
      AND wl.Workcenter_Status_Key IN (5445, 5449)
      AND wl.Log_Hours > 0
    GROUP BY
        YEAR(wl.Log_Date),
        MONTH(wl.Log_Date),
        we.Description
    ORDER BY Year, Month, Total_Hours DESC
""")
rows = cursor.fetchall()
print(f"Rows: {len(rows)}")
for r in rows[:10]:
    print(r)
conn.close()
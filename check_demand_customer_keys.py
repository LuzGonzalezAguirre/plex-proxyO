"""Read-only checks for customer master uniqueness and Demand join behavior."""
from main import get_connection, PCN

def run(cursor, title, sql):
    print("\n" + title)
    cursor.execute(sql)
    columns = [col[0] for col in cursor.description]
    print(" | ".join(columns))
    for record in cursor.fetchall():
        print(" | ".join(str(value) for value in record))

def main():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            run(cursor, "Duplicate customer master keys (must return no rows)", f"""
                SELECT Customer_No, COUNT(*) AS Matching_Rows
                FROM Common_v_Customer
                WHERE Plexus_Customer_No = {PCN}
                GROUP BY Customer_No
                HAVING COUNT(*) > 1
            """)
            run(cursor, "Selected customers and names", f"""
                SELECT Customer_No, Name
                FROM Common_v_Customer
                WHERE Plexus_Customer_No = {PCN}
                  AND Customer_No IN (772808, 332342, 332165, 332211)
            """)
        finally:
            cursor.close()
    finally:
        conn.close()

if __name__ == "__main__":
    main()

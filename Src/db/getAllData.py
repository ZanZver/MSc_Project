from fastapi import HTTPException
from psycopg2.extras import RealDictCursor


def get_all_data_logic(get_db_connection):
    """Retrieve data from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = "SELECT * FROM vehicles;"
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {e}")

from fastapi import HTTPException
from typing import Optional, List
from psycopg2.extras import RealDictCursor

def get_data_logic(get_db_connection, query: str, params: Optional[List[str]] = None):
    """Retrieve data from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or ())
                result = cur.fetchall()
                return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {e}")

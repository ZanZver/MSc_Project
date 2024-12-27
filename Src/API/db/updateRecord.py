from fastapi import HTTPException
from typing import Optional, List
from psycopg2.extras import RealDictCursor

def update_record_logic(get_db_connection, query: str, params: Optional[List[str]] = None):
    """Update a record in the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                conn.commit()
                return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

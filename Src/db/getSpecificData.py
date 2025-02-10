from fastapi import HTTPException
from typing import Optional, List
from psycopg2.extras import RealDictCursor
import psycopg2


def get_specific_data_logic(
    get_db_connection: psycopg2._psycopg.connection,
    key: str,
    key_field: str = "vin",
    params: Optional[List[str]] = None,
) -> dict:
    """Retrieve data from the database."""
    if key_field is None:  # pragma: no cover
        key_field = "vin"
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = f"SELECT * FROM vehicles WHERE {key_field} = '{key}'"
                cur.execute(query, params or ())
                result = cur.fetchall()
                return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {e}")

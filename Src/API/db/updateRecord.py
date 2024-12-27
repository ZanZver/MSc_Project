from fastapi import HTTPException
from typing import Optional, List
from psycopg2.extras import RealDictCursor
import json

def update_record_logic(get_db_connection, update_values: dict, condition: str, params: Optional[List[str]] = None):
    """Update a record in the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                update_values = json.loads(update_values)
                set_clause = ", ".join([f"{col} = %s" for col in update_values.keys()])
                update_query = f"UPDATE vehicles SET {set_clause} WHERE {condition}"
                query_params = tuple(update_values.values()) + tuple(params)

                # Execute the update query
                cur.execute(update_query, query_params)
                conn.commit()
                
                return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

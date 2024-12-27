from fastapi import HTTPException
from typing import Optional, List

def delete_record_logic(get_db_connection, condition: str, params: Optional[List[str]] = None):
    """Delete a record from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                delete_query = f"DELETE FROM vehicles WHERE {condition}"
            
                # Execute the DELETE query
                cur.execute(delete_query, params)
                conn.commit()
            
                return {"message": "Record deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting record: {e}")


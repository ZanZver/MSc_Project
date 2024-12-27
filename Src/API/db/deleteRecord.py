from fastapi import HTTPException
from typing import Optional, List

def delete_record_logic(get_db_connection, table_name: str, query: str, params: Optional[List[str]] = None):
    """Delete a record from the database."""
    try:
        # Validate the table name (simple alphanumeric check to avoid SQL injection)
        if not table_name.isidentifier():
            raise HTTPException(status_code=400, detail="Invalid table name")

        # Construct the query with the validated table name
        query = query.replace("{table_name}", table_name)
        
        print(query)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                conn.commit()
                return {"message": "Record deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting record: {e}")


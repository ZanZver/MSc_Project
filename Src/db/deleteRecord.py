from fastapi import HTTPException


def delete_record_db_logic(get_db_connection, key: str, key_field: str = "vin"):
    if key_field is None:  # pragma: no cover
        key_field = "vin"
    """Delete a record from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                delete_query = f"DELETE FROM vehicles WHERE {key_field} = '{key}'"

                # Execute the DELETE query
                cur.execute(delete_query)
                conn.commit()

                return {"message": "Record deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting record: {e}")

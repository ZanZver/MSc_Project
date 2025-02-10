from fastapi import HTTPException
import psycopg2


def delete_record_db_logic(
    get_db_connection: psycopg2._psycopg.connection, key: str, key_field: str = "vin"
) -> dict:
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

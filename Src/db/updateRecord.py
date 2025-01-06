from fastapi import HTTPException
import json


def update_record_logic(
    get_db_connection, update_values: dict, key: str, key_field: str = "vin"
):
    """Update a record in the database."""
    if key_field is None:  # pragma: no cover
        key_field = "vin"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                update_values = json.loads(update_values)
                set_clause = ", ".join([f"{col} = %s" for col in update_values.keys()])
                query_params = tuple(update_values.values())

                update_query = (
                    f"UPDATE vehicles SET {set_clause} WHERE {key_field} = '{key}'"
                )

                # Execute the update query
                cur.execute(update_query, query_params)
                conn.commit()

                return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

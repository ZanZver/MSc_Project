from fastapi import HTTPException
import json
import psycopg2


def update_record_logic(
    get_db_connection,
    update_values,
    key,
    key_field,
):
    """Update a record in the database."""
    if key_field is None:  # pragma: no cover
        key_field = "vin"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                set_clause = ", ".join([f"{col} = %s" for col in update_values.keys()])
                query_params = tuple(update_values.values())
                update_query = (
                    f"UPDATE vehicles SET {set_clause} WHERE {key_field} = '{key}'"
                )
                print(update_query)
                # Execute the update query
                cur.execute(update_query, query_params)
                conn.commit()

                return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

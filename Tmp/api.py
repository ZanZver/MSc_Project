# File: api.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import polars as pl

app = FastAPI()

DB_NAME = "testdb"
DB_USER = "admin"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "6432"

def get_db_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

@app.get("/retrieve/")
async def retrieve_data(query: str, params: Optional[List[str]] = Query(None)):
    """Retrieve data from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or ())
                result = cur.fetchall()
                return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {e}")

class UpdateRequest(BaseModel):
    update_values: dict
    condition: str
    condition_params: List[str]

@app.put("/update/")
async def update_record(request: UpdateRequest, table_name: str):
    """Update a record in the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                set_clause = ", ".join([f"{col} = %s" for col in request.update_values.keys()])
                update_query = f"UPDATE {table_name} SET {set_clause} WHERE {request.condition}"
                query_params = tuple(request.update_values.values()) + tuple(request.condition_params)
                cur.execute(update_query, query_params)
                conn.commit()
                return {"rows_updated": cur.rowcount}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating record: {e}")

class DeleteRequest(BaseModel):
    condition: str
    condition_params: List[str]

@app.delete("/delete/")
async def delete_record(request: DeleteRequest, table_name: str):
    """Delete a record from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                delete_query = f"DELETE FROM {table_name} WHERE {request.condition}"
                cur.execute(delete_query, tuple(request.condition_params))
                conn.commit()
                return {"rows_deleted": cur.rowcount}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting record: {e}")

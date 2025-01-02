from fastapi import FastAPI, HTTPException, Query
from web3 import Web3
from blockchain import (
    get_latest_record_logic,
    get_all_records_logic,
    get_connection_logic,
    get_account_logic,
    append_data_logic,
    get_record_history_logic,
    delete_record_bc_logic,
)
from db import (
    update_record_logic,
    get_all_data_logic,
    get_specific_data_logic,
    delete_record_db_logic,
)
from models.models import BlockchainRecord
from API.tags.tags import tags_metadata
from typing import Optional, List
import psycopg2

app = FastAPI(
    title="My Custom API",
    description="This is a custom API for database and blockchain operations.",
    version="1.0.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"},
)

# Blockchain connection setup
account, w3 = None, None  # Global variables for simplicity

DB_NAME = "testdb"
DB_USER = "admin"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "6432"

# =================================================================================================================================
# Connections
# =================================================================================================================================


def create_connection(node_ip="http://127.0.0.1:8545"):  # pragma: no cover
    global account, w3
    w3 = Web3(Web3.HTTPProvider(node_ip))
    if not w3.is_connected():
        raise Exception("Failed to connect to the Ethereum node.")
    account = w3.eth.accounts[0]


def get_db_connection():  # pragma: no cover
    """Create and return a new database connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


# =================================================================================================================================
# General
# =================================================================================================================================


@app.on_event("startup")
def setup():  # pragma: no cover
    try:
        create_connection()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Blockchain connection error: {str(e)}"
        )


@app.get("/", tags=["General"])
def root():  # pragma: no cover
    return {
        "message": "Welcome to the Blockchain API. Use /docs for API documentation."
    }


@app.get("/startup", include_in_schema=False, tags=["General"])
def startup_check():  # pragma: no cover
    return {"status": "API is ready"}


@app.get("/favicon.ico", include_in_schema=False, tags=["General"])
def favicon():  # pragma: no cover
    return {"message": "No favicon set"}


# =================================================================================================================================
# Blockchain stuff
# =================================================================================================================================


@app.get("/blockchain/test-connection", tags=["Blockchain Operations"])
def get_connection():  # pragma: no cover
    return get_connection_logic(w3)


@app.get("/blockchain/test-account", tags=["Blockchain Operations"])
def get_account():  # pragma: no cover
    return get_account_logic(account)


@app.get("/blockchain/all-records", tags=["Blockchain Operations"])
def get_all_records():  # pragma: no cover
    return get_all_records_logic(w3)


@app.get("/blockchain/latest-record", tags=["Blockchain Operations"])
def get_latest_record(key: str, key_field: str = "vin"):  # pragma: no cover
    return get_latest_record_logic(w3, key, key_field)


@app.post("/blockchain/append-data", tags=["Blockchain Operations"])
def append_data(record: BlockchainRecord):  # pragma: no cover
    return append_data_logic(w3, account, record)


@app.get("/blockchain/record-history", tags=["Blockchain Operations"])
def get_record_history(key: str, key_field: str = "vin"):  # pragma: no cover
    return get_record_history_logic(w3, key, key_field)


@app.delete("/blockchain/delete-record", tags=["Blockchain Operations"])
def delete_bc_record(key: str, key_field: str = "vin"):  # pragma: no cover
    return delete_record_bc_logic(w3, account, key, key_field)


# =================================================================================================================================
# DB stuff
# =================================================================================================================================


@app.get("/db/retrieve/all/", tags=["Database Operations"])
async def get_all_data():  # pragma: no cover
    """Retrieve data from the database."""
    return {"data": get_all_data_logic(get_db_connection)}


@app.get("/db/retrieve/specific/", tags=["Database Operations"])
async def get_specific_data(
    key: str,
    key_field: Optional[str] = Query(None),
    params: Optional[List[str]] = Query(None),
):  # pragma: no cover
    """Retrieve data from the database."""
    return {"data": get_specific_data_logic(get_db_connection, key, key_field, params)}


@app.put("/db/update/", tags=["Database Operations"])
async def update_record(
    update_values, key: str, key_field: Optional[str] = Query(None)
):  # pragma: no cover
    """Update a record in the database."""
    return update_record_logic(get_db_connection, update_values, key, key_field)


@app.delete("/db/delete/", tags=["Database Operations"])
async def delete_record(
    key: str, key_field: Optional[str] = Query(None)
):  # pragma: no cover
    """Delete a record from the database."""
    return delete_record_db_logic(get_db_connection, key, key_field)

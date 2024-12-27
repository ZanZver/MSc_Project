from fastapi import FastAPI, HTTPException, Query
from web3 import Web3
from blockchain import get_latest_record_logic, test_connection_logic, test_account_logic, append_data_logic, get_record_history_logic, delete_record_bc_logic
from db import update_record_logic, get_all_data_logic, get_specific_data_logic, delete_record_db_logic
from models.models import BlockchainRecord, DeleteRequest, UpdateRequest
from tags.tags import tags_metadata
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor


app = FastAPI(
    title="My Custom API",
    description="This is a custom API for database and blockchain operations.",
    version="1.0.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"}
)

# Blockchain connection setup
account, w3 = None, None  # Global variables for simplicity

DB_NAME = "testdb"
DB_USER = "admin"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "6432"

#=================================================================================================================================
# Connections
#=================================================================================================================================

def create_connection(node_ip="http://127.0.0.1:8545"):
    global account, w3
    w3 = Web3(Web3.HTTPProvider(node_ip))
    if not w3.is_connected():
        raise Exception("Failed to connect to the Ethereum node.")
    account = w3.eth.accounts[0]
    
def get_db_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    
#=================================================================================================================================
# General
#=================================================================================================================================

@app.on_event("startup")
def setup():
    try:
        create_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Blockchain connection error: {str(e)}")

@app.get("/", tags=["General"])
def root():
    return {"message": "Welcome to the Blockchain API. Use /docs for API documentation."}

@app.get("/startup", include_in_schema=False, tags=["General"])
def startup_check():
    return {"status": "API is ready"}

@app.get("/favicon.ico", include_in_schema=False, tags=["General"])
def favicon():
    return {"message": "No favicon set"}

#=================================================================================================================================
# Blockchain stuff
#=================================================================================================================================

@app.get("/blockchain/test-connection", tags=["Blockchain Operations"])
def test_connection():
    return test_connection_logic(w3)

@app.get("/blockchain/test-account", tags=["Blockchain Operations"])
def test_account():
    return test_account_logic(account)

@app.get("/blockchain/latest-record", tags=["Blockchain Operations"])
def get_latest_record(key: str, key_field: str = "vin"):
    return get_latest_record_logic(w3, key, key_field)

@app.post("/blockchain/append-data", tags=["Blockchain Operations"])
def append_data(record: BlockchainRecord):
    return append_data_logic(w3, account, record)

@app.get("/blockchain/record-history", tags=["Blockchain Operations"])
def get_record_history(key: str, key_field: str = "vin"):
    return get_record_history_logic(w3, key, key_field)

@app.delete("/blockchain/delete-record", tags=["Blockchain Operations"])
def delete_bc_record(key: str, key_field: str = "vin"):
    return delete_record_bc_logic(w3, account, key, key_field)

#=================================================================================================================================
# DB stuff
#=================================================================================================================================

@app.get("/db/retrieve/all/", tags=["Database Operations"])
async def get_all_data():
    """Retrieve data from the database."""
    return {"data": get_all_data_logic(get_db_connection)}

@app.get("/db/retrieve/specific/", tags=["Database Operations"])
async def get_specific_data(key: str, key_field: Optional[str] = Query(None), params: Optional[List[str]] = Query(None)):
    """Retrieve data from the database."""
    return {"data": get_specific_data_logic(get_db_connection, key, key_field, params)}
    
@app.put("/db/update/", tags=["Database Operations"])
async def update_record(update_values, key: str, key_field: Optional[str] = Query(None)):
    """Update a record in the database."""
    return update_record_logic(get_db_connection, update_values, key, key_field)
    
@app.delete("/db/delete/", tags=["Database Operations"])
async def delete_record(key: str, key_field: Optional[str] = Query(None)):
    """Delete a record from the database."""
    return delete_record_db_logic(get_db_connection, key, key_field)

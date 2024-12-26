from fastapi import FastAPI, HTTPException
from web3 import Web3
from blockchain import get_latest_record_logic, test_connection_logic, test_account_logic, append_data_logic, get_record_history_logic, delete_record_logic
from models.blockchain import BlockchainRecord

app = FastAPI()

# Blockchain connection setup
account, w3 = None, None  # Global variables for simplicity

def create_connection(node_ip="http://127.0.0.1:8545"):
    global account, w3
    w3 = Web3(Web3.HTTPProvider(node_ip))
    if not w3.is_connected():
        raise Exception("Failed to connect to the Ethereum node.")
    account = w3.eth.accounts[0]

@app.on_event("startup")
def setup():
    try:
        create_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Blockchain connection error: {str(e)}")

@app.get("/")
def root():
    return {"message": "Welcome to the Blockchain API. Use /docs for API documentation."}

@app.get("/startup")
def startup_check():
    return {"status": "API is ready"}

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return {"message": "No favicon set"}

@app.get("/blockchain/latest-record")
def get_latest_record(key: str, key_field: str = "vin"):
    return get_latest_record_logic(w3, key, key_field)

@app.get("/blockchain/test-connection")
def test_connection():
    return test_connection_logic(w3)

@app.get("/blockchain/test-account")
def test_account():
    return test_account_logic(account)

@app.post("/blockchain/append-data")
def append_data(record: BlockchainRecord):
    return append_data_logic(w3, account, record)

@app.get("/blockchain/record-history")
def get_record_history(key: str, key_field: str = "vin"):
    return get_record_history_logic(w3, key, key_field)

@app.delete("/blockchain/delete-record")
def delete_record(key: str, key_field: str = "vin"):
    return delete_record_logic(w3, account, key, key_field)

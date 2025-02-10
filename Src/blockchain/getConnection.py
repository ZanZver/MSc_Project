from fastapi import HTTPException
from web3 import Web3


def get_connection_logic(w3: Web3) -> dict:
    if not w3 or not w3.is_connected():
        raise HTTPException(
            status_code=500, detail="Blockchain connection is not active."
        )
    return {"message": "Connected to blockchain"}

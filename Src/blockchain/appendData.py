from pydantic import BaseModel
from typing import Optional, Dict
from fastapi import HTTPException
import json


# Tmp: Remove after fixing git test action
class BlockchainRecord(BaseModel):
    key: str
    key_field: Optional[str] = "vin"
    data: Optional[Dict] = None


def append_data_logic(w3, account, record: BlockchainRecord):
    try:
        if not record.data:
            raise HTTPException(
                status_code=400, detail="Data to append cannot be empty"
            )

        data_hex = w3.to_hex(text=json.dumps(record.data))  # Use `w3` instance here

        # Construct the transaction
        tx = {
            "from": account,
            "to": None,
            "value": 0,
            "gas": 3000000,
            "gasPrice": w3.to_wei("20", "gwei"),  # Use `w3` instance here
            "data": data_hex,
        }

        # Send the transaction
        tx_hash = w3.eth.send_transaction(tx)  # Use `w3` instance here

        return {"transaction_hash": tx_hash.hex()}
    except HTTPException:  # Allow HTTPExceptions to propagate as-is
        raise
    except ValueError as ve:  # pragma: no cover
        print(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=f"Invalid data format: {ve}")
    except Exception as e:  # pragma: no cover
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=f"Error appending data: {str(e)}")

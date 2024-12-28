import json
from fastapi import HTTPException

def delete_record_bc_logic(w3, account, key: str, key_field: str = "vin"):
    try:
        if not key:
            raise HTTPException(status_code=400, detail="Key cannot be empty")
        
        # Create the deletion record
        deletion_record = {
            key_field: key,
            "deleted": True
        }

        # Serialize the data
        data_hex = w3.to_hex(text=json.dumps(deletion_record))

        # Construct the transaction
        tx = {
            "from": account,
            "to": None,
            "value": 0,
            "gas": 3000000,
            "gasPrice": w3.to_wei("20", "gwei"),  # Use `w3` instance here
            "data": data_hex
        }

        # Send the transaction
        tx_hash = w3.eth.send_transaction(tx) 

        return {"transaction_hash": tx_hash.hex()}
    except HTTPException:  # Allow HTTPExceptions to propagate as-is
        raise
    except ValueError as ve:
        print(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=f"Invalid key or key_field: {ve}")
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting record: {str(e)}")
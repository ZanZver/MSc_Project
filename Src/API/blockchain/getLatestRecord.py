from web3 import Web3
import json

def get_latest_record_logic(w3, key: str, key_field: str = "vin"):
    try:
        latest_record = None
        for block_number in range(w3.eth.block_number + 1):
            block = w3.eth.get_block(block_number, full_transactions=True)
            for tx in block.transactions:
                if tx.to is None:
                    data_hex = tx.input if isinstance(tx.input, str) else tx.input.hex()
                    record = json.loads(w3.to_text(hexstr=data_hex))
                    if record.get(key_field) == key:
                        latest_record = record
        if not latest_record:
            raise HTTPException(status_code=404, detail="Record not found")
        return latest_record
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving record: {str(e)}")
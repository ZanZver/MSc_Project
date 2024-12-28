from fastapi import HTTPException
import json


def get_record_history_logic(w3, key: str, key_field: str = "vin"):
    try:
        history = []
        for block_number in range(w3.eth.block_number + 1):
            block = w3.eth.get_block(block_number, full_transactions=True)
            for tx in block.transactions:
                if tx.to is None:
                    data_hex = tx.input if isinstance(tx.input, str) else tx.input.hex()
                    record = json.loads(w3.to_text(hexstr=data_hex))
                    if record.get(key_field) == key:
                        history.append(record)
        if not history:
            raise HTTPException(
                status_code=404, detail="No history found for the record"
            )
        return history
    except HTTPException:  # Allow HTTPExceptions to propagate as-is
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving history: {str(e)}"
        )

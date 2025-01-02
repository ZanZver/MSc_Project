from fastapi import HTTPException
import json


# Function to fetch and decode the latest transaction for a specific key
def get_all_records_logic(w3):
    latest_record = []
    try:
        # Iterate through the blockchain's transactions
        for block_number in range(w3.eth.block_number + 1):
            block = w3.eth.get_block(block_number, full_transactions=True)
            for tx in block.transactions:
                if tx.to is None:  # Data-only transactions
                    # Decode the transaction input (data field)
                    data_hex = tx.input.hex()  # Convert HexBytes to a hex string
                    try:
                        record = json.loads(w3.to_text(hexstr=data_hex))
                        latest_record.append(record)
                    except Exception as e:
                        print(f"Error decoding transaction: {e}")
    except HTTPException:  # pragma: no cover
        raise  # Keep HTTPExceptions as-is
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving record: {str(e)}"
        )
    return latest_record

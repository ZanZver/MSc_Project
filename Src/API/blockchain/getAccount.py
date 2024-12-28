from fastapi import HTTPException

def get_account_logic(account):
    if not account:
        raise HTTPException(status_code=500, detail="Account not initialized.")
    return {"account": account}
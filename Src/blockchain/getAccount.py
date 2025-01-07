from fastapi import HTTPException
from web3.types import ChecksumAddress


def get_account_logic(account: ChecksumAddress) -> dict:
    if not account:
        raise HTTPException(status_code=500, detail="Account not initialized.")
    return {"account": account}

from pydantic import BaseModel
from typing import Optional, Dict, List


class BlockchainRecord(BaseModel):
    key: str
    key_field: Optional[str] = "vin"
    data: Optional[Dict] = None


class DeleteRequest(BaseModel):
    condition: str
    condition_params: List[str]


class UpdateRequest(BaseModel):
    update_values: dict
    condition: str
    condition_params: List[str]

from pydantic import BaseModel
from typing import Optional, Dict, List


class BlockchainRecord(BaseModel):  # pragma: no cover
    key: str
    key_field: Optional[str] = "vin"
    data: Optional[Dict] = None


class DeleteRequest(BaseModel):  # pragma: no cover
    condition: str
    condition_params: List[str]


class UpdateRequest(BaseModel):  # pragma: no cover
    update_values: dict
    condition: str
    condition_params: List[str]

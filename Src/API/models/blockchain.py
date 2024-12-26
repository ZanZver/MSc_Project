from pydantic import BaseModel
from typing import Optional, Dict

class BlockchainRecord(BaseModel):
    key: str
    key_field: Optional[str] = "vin"
    data: Optional[Dict] = None
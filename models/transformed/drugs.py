from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Drugs(BaseModel):
    ndc: str
    med_name: Optional[str]
    med_alt_name: Optional[str]
    med_strength: Optional[str]
    med_form: Optional[str]
    inserted_at: datetime

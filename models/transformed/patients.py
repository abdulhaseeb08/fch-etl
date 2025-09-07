from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional

class Patients(BaseModel):
    mrn: str
    first_name: Optional[str]
    last_name: Optional[str]
    dob: Optional[date]
    gender: Optional[str]
    inserted_at: datetime

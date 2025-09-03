from pydantic import BaseModel
from datetime import datetime

class Facility(BaseModel):
    facility_id: str
    facility: str
    inserted_at: datetime

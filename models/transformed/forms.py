from pydantic import BaseModel
from datetime import datetime

class Forms(BaseModel):
    form_uuid: str
    form_name: str
    inserted_at: datetime

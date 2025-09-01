from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveProbs(BaseModel):
    """Model for Active Problems data from incoming_*_ActiveProbs.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    code_set: Optional[str]
    code: Optional[str]
    long_description: Optional[str]
    date_created: Optional[str]
    file_name: Optional[str]
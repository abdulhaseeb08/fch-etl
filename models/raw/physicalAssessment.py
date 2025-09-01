from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class PhysicalAssessmentWithClass(BaseModel):
    """Model for Physical Assessment data from incoming_*_PhysicalAssessmentWithClass.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    last_booked_date: Optional[str]
    last_pa: Optional[str]
    last_pchp: Optional[str]
    last_pahp: Optional[str]
    file_name: Optional[str]
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class VitalResults(BaseModel):
    """Model for Vital Results data from incoming_*_VitalResultsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    vital_type: Optional[str]
    vital_value1: Optional[str]
    vital_value2: Optional[str]
    vital_um: Optional[str]
    recorded_by_full_name: Optional[str]
    result_date: Optional[str]
    file_name: Optional[str]
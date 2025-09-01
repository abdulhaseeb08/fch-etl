from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class LabResults(BaseModel):
    """Model for Lab Results data from incoming_*_LabResultsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    service_text: Optional[str]
    obs_text: Optional[str]
    obs_result: Optional[str]
    spec_rcd_on: Optional[str]
    file_name: Optional[str]
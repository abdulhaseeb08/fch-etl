from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class LabResults(BaseModel):
    """Model for Lab Results data from incoming_*_LabResultsPrevious24Hours.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    service_text: str = Field(alias="SERVICE_TEXT")
    obs_text: str = Field(alias="OBS_TEXT")
    obs_result: str = Field(alias="OBS_RESULT")
    spec_rcd_on: datetime = Field(alias="SPEC_RCD_ON")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
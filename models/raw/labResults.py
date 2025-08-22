from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class LabResults(BaseModel):
    """Model for Lab Results data from incoming_*_LabResultsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    service_text: Optional[str] = Field(alias="SERVICE_TEXT", default=None)
    obs_text: Optional[str] = Field(alias="OBS_TEXT", default=None)
    obs_result: Optional[str] = Field(alias="OBS_RESULT", default=None)
    spec_rcd_on: Optional[str] = Field(alias="SPEC_RCD_ON", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
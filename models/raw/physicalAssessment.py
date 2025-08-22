from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class PhysicalAssessmentWithClass(BaseModel):
    """Model for Physical Assessment data from incoming_*_PhysicalAssessmentWithClass.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    last_booked_date: Optional[str] = Field(alias="LAST_BOOKED_DATE", default=None)
    last_pa: Optional[str] = Field(alias="LAST_PA", default=None)
    last_pchp: Optional[str] = Field(alias="LAST_PCHP", default=None)
    last_pahp: Optional[str] = Field(alias="LAST_PAHP", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
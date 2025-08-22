from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class PhysicalAssessmentWithClass(BaseModel):
    """Model for Physical Assessment data from incoming_*_PhysicalAssessmentWithClass.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    last_booked_date: datetime = Field(alias="LAST_BOOKED_DATE")
    last_pa: Optional[datetime] = Field(alias="LAST_PA", default=None)
    last_pchp: Optional[datetime] = Field(alias="LAST_PCHP", default=None)
    last_pahp: Optional[datetime] = Field(alias="LAST_PAHP", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
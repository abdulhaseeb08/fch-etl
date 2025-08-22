from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class VitalResults(BaseModel):
    """Model for Vital Results data from incoming_*_VitalResultsPrevious24Hours.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    vital_type: str = Field(alias="VITAL_TYPE")
    vital_value1: str = Field(alias="VITAL_VALUE1")
    vital_value2: Optional[str] = Field(alias="VITAL_VALUE2", default=None)
    vital_um: str = Field(alias="VITAL_UM")
    recorded_by_full_name: str = Field(alias="RECORDED_BY_FULL_NAME")
    result_date: datetime = Field(alias="RESULT_DATE")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class VitalResults(BaseModel):
    """Model for Vital Results data from incoming_*_VitalResultsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    vital_type: Optional[str] = Field(alias="VITAL_TYPE", default=None)
    vital_value1: Optional[str] = Field(alias="VITAL_VALUE1", default=None)
    vital_value2: Optional[str] = Field(alias="VITAL_VALUE2", default=None)
    vital_um: Optional[str] = Field(alias="VITAL_UM", default=None)
    recorded_by_full_name: Optional[str] = Field(alias="RECORDED_BY_FULL_NAME", default=None)
    result_date: Optional[str] = Field(alias="RESULT_DATE", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
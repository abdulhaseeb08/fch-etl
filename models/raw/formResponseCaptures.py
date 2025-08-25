from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormResponseCaptures(BaseModel):
    """Model for Form Response Captures data from incoming_*_FormResponseCapturesPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    form_name: Optional[str] = Field(alias="FormName", default=None)
    cap_desc: Optional[str] = Field(alias="CAP_DESC", default=None)
    ans_text: Optional[str] = Field(alias="ANS_TEXT", default=None)
    date_created: Optional[str] = Field(alias="DATE_CREATED", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
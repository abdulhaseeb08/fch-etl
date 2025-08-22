from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormResponseCaptures(BaseModel):
    """Model for Form Response Captures data from incoming_*_FormResponseCapturesPrevious24Hours.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    form_name: str = Field(alias="FormName")
    cap_desc: str = Field(alias="CAP_DESC")
    ans_text: str = Field(alias="ANS_TEXT")
    date_created: datetime = Field(alias="DATE_CREATED")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
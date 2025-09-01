from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormResponseCaptures(BaseModel):
    """Model for Form Response Captures data from incoming_*_FormResponseCapturesPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    form_name: Optional[str]
    cap_desc: Optional[str]
    ans_text: Optional[str]
    date_created: Optional[str]
    file_name: Optional[str]
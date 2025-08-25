from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormSubmissions(BaseModel):
    """Model for Form Submissions data from incoming_*_FormSubmissionsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    date_form_submitted: Optional[str] = Field(alias="DATE_FORM_SUBMITTED", default=None)
    form_name: Optional[str] = Field(alias="FORM_NAME", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
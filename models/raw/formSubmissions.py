from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormSubmissions(BaseModel):
    """Model for Form Submissions data from incoming_*_FormSubmissionsPrevious24Hours.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    date_form_submitted: datetime = Field(alias="DATE_FORM_SUBMITTED")
    form_name: str = Field(alias="FORM_NAME")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormSubmissions(BaseModel):
    """Model for Form Submissions data from incoming_*_FormSubmissionsPrevious24Hours.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    date_form_submitted: Optional[str]
    form_name: Optional[str]
    file_name: Optional[str]
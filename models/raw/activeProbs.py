from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveProbs(BaseModel):
    """Model for Active Problems data from incoming_*_ActiveProbs.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    code_set: Optional[str] = Field(alias="CODE_SET", default=None)
    code: Optional[str] = Field(alias="CODE", default=None)
    long_description: Optional[str] = Field(alias="LONG_DESCRIPTION", default=None)
    date_created: Optional[str] = Field(alias="DATE_CREATED", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
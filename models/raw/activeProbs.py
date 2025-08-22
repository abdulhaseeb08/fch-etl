from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveProbs(BaseModel):
    """Model for Active Problems data from incoming_*_ActiveProbs.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    code_set: str = Field(alias="CODE_SET")
    code: str = Field(alias="CODE")
    long_description: str = Field(alias="LONG_DESCRIPTION")
    date_created: date = Field(alias="DATE_CREATED")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class TBTestResults(BaseModel):
    """Model for TB Test Results data from incoming_*_TBTestResultsPrevious7Days.csv"""
    SAPPHIRE_PAT_ID: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    FACILITY_ID: Optional[str] = Field(alias="FACILITY_ID", default=None)
    TESTED_BY: Optional[str] = Field(alias="TESTED_BY", default=None)
    PPD_TST_DT: Optional[str] = Field(alias="PPD_TST_DT", default=None)
    PPD_TST_READ_DT: Optional[str] = Field(alias="PPD_TST_READ_DT", default=None)
    REACTION_SIZE: Optional[str] = Field(alias="REACTION_SIZE", default=None)
    READ_BY: Optional[str] = Field(alias="READ_BY", default=None)
    READER_COMMENTS: Optional[str] = Field(alias="READER_COMMENTS", default=None)
    NEXT_TST_DT: Optional[str] = Field(alias="NEXT_TST_DT", default=None)
    MOST_RECENT_TB_XRAY_RESULT: Optional[str] = Field(alias="MOST_RECENT_TB_XRAY_RESULT", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
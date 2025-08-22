from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class TBTestResults(BaseModel):
    """Model for TB Test Results data from incoming_*_TBTestResultsPrevious7Days.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    tested_by: Optional[str] = Field(alias="TESTED_BY", default=None)
    ppd_tst_dt: Optional[str] = Field(alias="PPD_TST_DT", default=None)
    ppd_tst_read_dt: Optional[str] = Field(alias="PPD_TST_READ_DT", default=None)
    reaction_size: Optional[str] = Field(alias="REACTION_SIZE", default=None)
    read_by: Optional[str] = Field(alias="READ_BY", default=None)
    reader_comments: Optional[str] = Field(alias="READER_COMMENTS", default=None)
    next_tst_dt: Optional[str] = Field(alias="NEXT_TST_DT", default=None)
    most_recent_tb_xray_result: Optional[str] = Field(alias="MOST_RECENT_TB_XRAY_RESULT", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
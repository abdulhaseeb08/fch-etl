from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class TBTestResults(BaseModel):
    """Model for TB Test Results data from incoming_*_TBTestResultsPrevious7Days.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    tested_by: Optional[str]
    ppd_tst_dt: Optional[str]
    ppd_tst_read_dt: Optional[str]
    reaction_size: Optional[str]
    read_by: Optional[str]
    reader_comments: Optional[str]
    next_tst_dt: Optional[str]
    most_recent_tb_xray_result: Optional[str]
    file_name: Optional[str]
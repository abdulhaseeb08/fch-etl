from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class MedPassResults(BaseModel):
    """Model for MedPass Results data from incoming_*_MedPassResultsPrevious12Hours.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    order_id: Optional[str]
    prn_flag: Optional[str]
    kop_flag: Optional[str]
    label_name: Optional[str]
    alt_name: Optional[str]
    strength: Optional[str]
    dos_id: Optional[str]
    formatted_ndc: Optional[str]
    medpass_date: Optional[str]
    result_date: Optional[str]
    medpass_result: Optional[str]
    qty_dose: Optional[str]
    recorded_by_full_name: Optional[str]
    file_name: Optional[str]

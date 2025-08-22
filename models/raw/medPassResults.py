from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class MedPassResults(BaseModel):
    """Model for MedPass Results data from incoming_*_MedPassResultsPrevious12Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    order_id: Optional[str] = Field(alias="ORDER_ID", default=None)
    prn_flag: Optional[str] = Field(alias="PRN_FLAG", default=None)
    kop_flag: Optional[str] = Field(alias="KOP_FLAG", default=None)
    label_name: Optional[str] = Field(alias="LABEL_NAME", default=None)
    alt_name: Optional[str] = Field(alias="ALT_NAME", default=None)
    strength: Optional[str] = Field(alias="STRENGTH", default=None)
    dos_id: Optional[str] = Field(alias="DOS_ID", default=None)
    formatted_ndc: Optional[str] = Field(alias="FORMATTED_NDC", default=None)
    medpass_date: Optional[str] = Field(alias="MEDPASS_DATE", default=None)
    result_date: Optional[str] = Field(alias="RESULT_DATE", default=None)
    medpass_result: Optional[str] = Field(alias="MEDPASS_RESULT", default=None)
    qty_dose: Optional[str] = Field(alias="QTY_DOSE", default=None)
    recorded_by_full_name: Optional[str] = Field(alias="RECORDED_BY_FULL_NAME", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

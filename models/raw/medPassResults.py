from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class MedPassResults(BaseModel):
    """Model for MedPass Results data from incoming_*_MedPassResultsPrevious12Hours.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    order_id: int = Field(alias="ORDER_ID")
    prn_flag: str = Field(alias="PRN_FLAG")
    kop_flag: str = Field(alias="KOP_FLAG")
    label_name: str = Field(alias="LABEL_NAME")
    alt_name: str = Field(alias="ALT_NAME")
    strength: str = Field(alias="STRENGTH")
    dos_id: str = Field(alias="DOS_ID")
    formatted_ndc: str = Field(alias="FORMATTED_NDC")
    medpass_date: Optional[datetime] = Field(alias="MEDPASS_DATE", default=None)
    result_date: Optional[datetime] = Field(alias="RESULT_DATE", default=None)
    medpass_result: str = Field(alias="MEDPASS_RESULT")
    qty_dose: Optional[str] = Field(alias="QTY_DOSE", default=None)
    recorded_by_full_name: Optional[str] = Field(alias="RECORDED_BY_FULL_NAME", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

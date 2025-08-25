from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveVitalTxOrders(BaseModel):
    """Model for Active Vital and TX Orders data from incoming_*_ActiveVitalandTXOrders.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    vital_order_type: Optional[str] = Field(alias="VITAL_ORDER_TYPE", default=None)
    directions: Optional[str] = Field(alias="Directions", default=None)
    frequency: Optional[str] = Field(alias="FREQUENCY", default=None)
    time_slots: Optional[str] = Field(alias="TIME_SLOTS", default=None)
    start_date: Optional[str] = Field(alias="START_DATE", default=None)
    stop_date: Optional[str] = Field(alias="STOP_DATE", default=None)
    prescriber: Optional[str] = Field(alias="PRESCRIBER", default=None)
    npi: Optional[str] = Field(alias="NPI", default=None)
    vital_order_id: Optional[str] = Field(alias="VITAL_ORDER_ID", default=None)
    ordered_by: Optional[str] = Field(alias="ORDERED_BY", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
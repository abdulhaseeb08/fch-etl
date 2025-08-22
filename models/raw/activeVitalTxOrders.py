from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveVitalTxOrders(BaseModel):
    """Model for Active Vital and TX Orders data from incoming_*_ActiveVitalandTXOrders.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    vital_order_type: str = Field(alias="VITAL_ORDER_TYPE")
    directions: str = Field(alias="Directions")
    frequency: str = Field(alias="FREQUENCY")
    time_slots: str = Field(alias="TIME_SLOTS")
    start_date: date = Field(alias="START_DATE")
    stop_date: date = Field(alias="STOP_DATE")
    prescriber: str = Field(alias="PRESCRIBER")
    npi: str = Field(alias="NPI")
    vital_order_id: int = Field(alias="VITAL_ORDER_ID")
    ordered_by: str = Field(alias="ORDERED_BY")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
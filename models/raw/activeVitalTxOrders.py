from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveVitalTxOrders(BaseModel):
    """Model for Active Vital and TX Orders data from incoming_*_ActiveVitalandTXOrders.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    vital_order_type: Optional[str]
    directions: Optional[str]
    frequency: Optional[str]
    time_slots: Optional[str]
    start_date: Optional[str]
    stop_date: Optional[str]
    prescriber: Optional[str]
    npi: Optional[str]
    vital_order_id: Optional[str]
    ordered_by: Optional[str]
    file_name: Optional[str]
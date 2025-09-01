from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveRoster(BaseModel):
    """Model for Active Roster data from incoming_*_ActiveRoster.csv"""
    sapphire_pat_id: Optional[str]
    facility: Optional[str]
    facility_id: Optional[str]
    jms_id: Optional[str]
    patient_name: Optional[str]
    unit: Optional[str]
    wing: Optional[str]
    room: Optional[str]
    bed: Optional[str]
    dob: Optional[str]
    gender: Optional[str]
    last_booked_date: Optional[str]
    last_release_date: Optional[str]
    ata_status: Optional[str]
    file_name: Optional[str]
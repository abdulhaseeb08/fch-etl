from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveRoster(BaseModel):
    """Model for Active Roster data from incoming_*_ActiveRoster.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility: str = Field(alias="FACILITY")
    facility_id: str = Field(alias="FACILITY_ID")
    jms_id: str = Field(alias="JMS_ID")
    patient_name: str = Field(alias="PATIENT_NAME")
    unit: str = Field(alias="UNIT")
    wing: Optional[str] = Field(alias="WING", default=None)
    room: Optional[str] = Field(alias="ROOM", default=None)
    bed: Optional[str] = Field(alias="BED", default=None)
    dob: date = Field(alias="DOB")
    gender: str = Field(alias="GENDER")
    last_booked_date: datetime = Field(alias="LAST_BOOKED_DATE")
    last_release_date: Optional[datetime] = Field(alias="LAST_RELEASE_DATE", default=None)
    ata_status: str = Field(alias="ATA_STATUS")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
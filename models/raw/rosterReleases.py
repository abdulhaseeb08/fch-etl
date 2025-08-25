from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class RosterReleases(BaseModel):
    """Model for Roster Releases data from incoming_*_RosterReleasesPrev24Hours.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility: Optional[str] = Field(alias="FACILITY", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    jms_id: Optional[str] = Field(alias="JMS_ID", default=None)
    patient_name: Optional[str] = Field(alias="PATIENT_NAME", default=None)
    unit: Optional[str] = Field(alias="UNIT", default=None)
    wing: Optional[str] = Field(alias="WING", default=None)
    room: Optional[str] = Field(alias="ROOM", default=None)
    bed: Optional[str] = Field(alias="BED", default=None)
    dob: Optional[str] = Field(alias="DOB", default=None)
    gender: Optional[str] = Field(alias="GENDER", default=None)
    last_booked_date: Optional[str] = Field(alias="LAST_BOOKED_DATE", default=None)
    last_release_date: Optional[str] = Field(alias="LAST_RELEASE_DATE", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
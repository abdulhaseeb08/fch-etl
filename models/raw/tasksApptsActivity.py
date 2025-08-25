from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TasksApptsActivity(BaseModel):
    """Model for Tasks Appointments Activity data from incoming_*_TasksApptsActivityPrevious1Day.csv"""
    sapphire_pat_id: Optional[str] = Field(alias="SAPPHIRE_PAT_ID", default=None)
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    item_type: Optional[str] = Field(alias="ITEM_TYPE", default=None)
    status: Optional[str] = Field(alias="STATUS", default=None)
    item_created_on_date: Optional[str] = Field(alias="ITEM_CREATED_ON_DATE", default=None)
    item_due_date: Optional[str] = Field(alias="ITEM_DUE_DATE", default=None)
    item_completed_on: Optional[str] = Field(alias="ITEM_COMPLETED_ON", default=None)
    item_note: Optional[str] = Field(alias="ITEM_NOTE", default=None)
    item_tags: Optional[str] = Field(alias="ITEM_TAGS", default=None)

    class Config:
        validate_by_name = True
        populate_by_name = True
        extra = "ignore" 
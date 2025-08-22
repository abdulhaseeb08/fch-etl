from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TasksApptsActivity(BaseModel):
    """Model for Tasks Appointments Activity data from incoming_*_TasksApptsActivityPrevious1Day.csv"""
    sapphire_pat_id: str = Field(alias="SAPPHIRE_PAT_ID")
    facility_id: str = Field(alias="FACILITY_ID")
    item_type: str = Field(alias="ITEM_TYPE")
    status: str = Field(alias="STATUS")
    item_created_on_date: datetime = Field(alias="ITEM_CREATED_ON_DATE")
    item_due_date: datetime = Field(alias="ITEM_DUE_DATE")
    item_completed_on: Optional[datetime] = Field(alias="ITEM_COMPLETED_ON", default=None)
    item_note: Optional[str] = Field(alias="ITEM_NOTE", default=None)
    item_tags: Optional[str] = Field(alias="ITEM_TAGS", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 
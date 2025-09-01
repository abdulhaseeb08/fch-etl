from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TasksApptsActivity(BaseModel):
    """Model for Tasks Appointments Activity data from incoming_*_TasksApptsActivityPrevious1Day.csv"""
    sapphire_pat_id: Optional[str]
    facility_id: Optional[str]
    item_type: Optional[str]
    status: Optional[str]
    item_created_on_date: Optional[str]
    item_due_date: Optional[str]
    item_completed_on: Optional[str]
    item_note: Optional[str]
    item_tags: Optional[str]
    file_name: Optional[str]
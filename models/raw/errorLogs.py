from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class ErrorLogs(BaseModel):
    pipeline_name: str
    datetime: datetime
    error_message: str
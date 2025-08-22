from pydantic import BaseModel, Field
from typing import Optional

class Files(BaseModel):
    file_name: str = Field(alias="FILE_NAME")
    status: str = Field(alias="STATUS")
    error_message: Optional[str] = Field(alias="ERROR_MESSAGE", default=None)
    load_time: str = Field(alias="LOAD_TIME")
    
    class Config:
        allow_population_by_field_name = True
        populate_by_name = True
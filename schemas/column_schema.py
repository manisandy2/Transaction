from pydantic import BaseModel, Field
from typing import Optional
from fastapi import Query

class AddColumnRequest(BaseModel):
    column_name: str = Field(..., description="New column name")
    column_type: str = Field(..., description="Iceberg column type (required)")
    doc: Optional[str] = Field(None, description="Column description (optional)")
    namespace: str = Field(..., description="Iceberg namespace")
    table_name: str = Field(..., description="Iceberg table name")
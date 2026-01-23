
from fastapi import APIRouter,Query,HTTPException
from ..core.catalog_client import get_catalog_client
from pydantic import BaseModel
from datetime import datetime
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from pyiceberg.types import (
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from typing import Optional

router = APIRouter(prefix="/columns", tags=["columns"])

import logging

logger = logging.getLogger(__name__)

def get_iceberg_type(type_str: str):
    type_str = type_str.lower()
    if type_str in ["int", "integer"]:
        return IntegerType()
    elif type_str in ["string", "str"]:
        return StringType()
    elif type_str in ["double", "float"]:
        return DoubleType()
    elif type_str in ["bool", "boolean"]:
        return BooleanType()
    elif type_str in ["timestamp", "datetime"]:
        return TimestampType()
    else:
        raise ValueError(f"Unsupported Iceberg type: {type_str}")


@router.put("/update_column_with_data_copy")
def update_column_with_data_copy(
    column_name: str = Query(..., description="Old column name"),
    new_column_name: str = Query(..., description="New column name"),
    column_type: Optional[str] = Query(None, description="New column type (optional)"),
    doc: Optional[str] = Query(None, description="New column description (optional)"),
    namespace: str = Query(...),
    table_name: str = Query(...)
):
    """
    Add a new column and copy old column data into it.
    Optionally update its type or doc.
    """

    try:
        catalog = get_catalog_client()
        identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(identifier)

        # --- Step 1: Schema update ---
        old_field = next((f for f in table.schema().fields if f.name == column_name), None)
        if not old_field:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")

        new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
        new_doc = doc if doc else old_field.doc

        with table.update_schema() as update:
            update.add_column(new_column_name, new_type, doc=new_doc)
            logger.info(f"Added new column '{new_column_name}' of type '{new_type}'")

        # --- Step 2: Load existing data ---
        import pyarrow.dataset as ds
        import pyarrow as pa

        dataset = ds.dataset(table.location(), format="iceberg")
        table_data = dataset.to_table()
        df = table_data.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in data files.")

        # --- Step 3: Copy data ---
        df[new_column_name] = df[column_name]
        logger.info(f"Copied data from '{column_name}' to '{new_column_name}'")

        # --- Step 4: Write data back to Iceberg ---
        import pyiceberg.io.pyarrow as paio
        writer = paio.PyArrowFileIO()
        table.overwrite(pa.Table.from_pandas(df), overwrite=True)
        logger.info(f"Data rewrite completed for table '{identifier}'")

        return {
            "status": "success",
            "message": f"New column '{new_column_name}' added and populated from '{column_name}'",
            "rows_updated": len(df)
        }

    except Exception as e:
        logger.exception(f"Failed to update and copy data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.delete("/")
def delete_column(
        column_name: str = Query(..., description="Column name to delete"),
        namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
        table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        with table.update_schema() as update:
            update.delete_column(column_name)

        return {"status": "success", "message": f"Column '{column_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete column failed: {e}")

@router.delete("/delete")
def delete_data_or_column(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'TableName')"),
    column_name: str = Query(None, description="Column name to delete (optional)"),
    date_column: str = Query(None, description="Date column for filtering (optional)"),
    start_date: str = Query(None, description="Start date (ISO format, e.g., 2025-10-01T00:00:00)"),
    end_date: str = Query(None, description="End date (ISO format, e.g., 2025-10-10T23:59:59)")
):
    """
    Deletes either a column (schema delete) or rows in a date range (data delete).
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # 🧩 If column name is given, delete the column
        if column_name:
            with table.update_schema() as update:
                update.delete_column(column_name)
            return {"status": "success", "message": f"Column '{column_name}' deleted successfully"}

        # 🕓 If date range filters are given, delete rows
        if date_column and (start_date or end_date):
            expressions = []

            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date)
                    expressions.append(GreaterThanOrEqual(date_column, start_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid start_date format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS).")

            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date)
                    expressions.append(LessThanOrEqual(date_column, end_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid end_date format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS).")

            # Combine expressions with AND
            delete_filter = expressions[0]
            for expr in expressions[1:]:
                delete_filter = And(delete_filter, expr)

            # Execute Iceberg delete
            table.delete_where(delete_filter)
            return {
                "status": "success",
                "message": f"Rows deleted successfully for date range in '{date_column}'"
            }

        raise HTTPException(status_code=400, detail="Provide either 'column_name' for schema delete or 'date_column' with date range for data delete.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete operation failed: {e}")
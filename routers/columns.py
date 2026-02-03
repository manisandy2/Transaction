
from fastapi import APIRouter,Query,HTTPException
from core.catalog_client import get_catalog_client
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


# @router.put("/update_column_with_data_copy")
# def update_column_with_data_copy(
#     column_name: str = Query(..., description="Old column name"),
#     new_column_name: str = Query(..., description="New column name"),
#     column_type: Optional[str] = Query(None, description="New column type (optional)"),
#     doc: Optional[str] = Query(None, description="New column description (optional)"),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     """
#     Add a new column and copy old column data into it.
#     Optionally update its type or doc.
#     """
#
#     try:
#         catalog = get_catalog_client()
#         identifier = f"{namespace}.{table_name}"
#         table = catalog.load_table(identifier)
#
#         # --- Step 1: Schema update ---
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         if not old_field:
#             raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")
#
#         new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#         new_doc = doc if doc else old_field.doc
#
#         with table.update_schema() as update:
#             update.add_column(new_column_name, new_type, doc=new_doc)
#             logger.info(f"Added new column '{new_column_name}' of type '{new_type}'")
#
#         # --- Step 2: Load existing data ---
#         import pyarrow.dataset as ds
#         import pyarrow as pa
#
#         dataset = ds.dataset(table.location(), format="iceberg")
#         table_data = dataset.to_table()
#         df = table_data.to_pandas()
#
#         if column_name not in df.columns:
#             raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in data files.")
#
#         # --- Step 3: Copy data ---
#         df[new_column_name] = df[column_name]
#         logger.info(f"Copied data from '{column_name}' to '{new_column_name}'")
#
#         # --- Step 4: Write data back to Iceberg ---
#         import pyiceberg.io.pyarrow as paio
#         writer = paio.PyArrowFileIO()
#         table.overwrite(pa.Table.from_pandas(df), overwrite=True)
#         logger.info(f"Data rewrite completed for table '{identifier}'")
#
#         return {
#             "status": "success",
#             "message": f"New column '{new_column_name}' added and populated from '{column_name}'",
#             "rows_updated": len(df)
#         }
#
#     except Exception as e:
#         logger.exception(f"Failed to update and copy data: {e}")
#         raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
# @router.put("/update_column_with_data_copy")
# def update_column_with_data_copy(
#     column_name: str = Query(...),
#     new_column_name: str = Query(...),
#     column_type: Optional[str] = Query(None),
#     doc: Optional[str] = Query(None),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     """
#     Add a new column and backfill data from an existing column (Iceberg-safe).
#     """
#
#     try:
#         from datetime import datetime
#         import pyarrow as pa
#
#         catalog = get_catalog_client()
#         identifier = f"{namespace}.{table_name}"
#         table = catalog.load_table(identifier)
#
#         # ---------- STEP 1: Validate column ----------
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#         new_doc = doc or old_field.doc
#
#         # ---------- STEP 2: Add new column ----------
#         with table.update_schema() as update:
#             update.add_column(
#                 new_column_name,
#                 new_type,
#                 doc=new_doc
#             )
#
#         # ---------- STEP 3: Read Iceberg data ----------
#         arrow_tbl = table.scan().to_arrow()
#         df = arrow_tbl.to_pandas()
#
#         if column_name not in df.columns:
#             raise HTTPException(400, f"Column '{column_name}' not present in data")
#
#         # ---------- STEP 4: Copy + convert ----------
#         if column_type == "timestamp":
#             def parse_ts(v):
#                 if not v:
#                     return None
#                 try:
#                     return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
#                 except Exception:
#                     return None
#
#             df[new_column_name] = df[column_name].apply(parse_ts)
#         else:
#             df[new_column_name] = df[column_name]
#
#         # ---------- STEP 5: Iceberg-safe rewrite ----------
#         arrow_out = pa.Table.from_pandas(df, preserve_index=False)
#
#         table.overwrite_by_filter(
#             filter=True,   # full-table overwrite but Iceberg-managed
#             data=arrow_out
#         )
#
#         return {
#             "status": "success",
#             "message": f"Column '{new_column_name}' added and backfilled from '{column_name}'",
#             "rows_updated": len(df)
#         }
#
#     except Exception as e:
#         logger.exception("Schema copy failed")
#         raise HTTPException(500, str(e))


@router.put("/update_column_with_data_copy02")
def update_column_with_data_copy(
    column_name: str = Query(..., description="Source column"),
    new_column_name: str = Query(..., description="Target column"),
    namespace: str = Query(...),
    table_name: str = Query(...)
):
    """
    Add a new column and copy data from an existing column (Iceberg-safe).
    No type conversion.
    """

    try:
        import pyarrow as pa

        catalog = get_catalog_client()
        identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(identifier)

        # ---------- STEP 1: Validate source column ----------
        old_field = next(
            (f for f in table.schema().fields if f.name == column_name),
            None
        )
        if not old_field:
            raise HTTPException(404, f"Column '{column_name}' not found")

        # ---------- STEP 2: Add new column (same type) ----------
        with table.update_schema() as update:
            update.add_column(
                new_column_name,
                old_field.field_type,
                doc=f"Copied from {column_name}"
            )

        # ---------- STEP 3: Read Iceberg data ----------
        arrow_tbl = table.scan().to_arrow()
        df = arrow_tbl.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(
                400, f"Column '{column_name}' not found in data"
            )

        # ---------- STEP 4: Copy data ----------
        df[new_column_name] = df[column_name]

        # ---------- STEP 5: Iceberg-safe rewrite ----------
        arrow_out = pa.Table.from_pandas(df, preserve_index=False)

        table.overwrite_by_filter(
            filter=True,   # full-table rewrite, Iceberg-managed
            data=arrow_out
        )

        return {
            "status": "success",
            "message": f"Copied data from '{column_name}' to '{new_column_name}'",
            "rows_updated": len(df)
        }

    except Exception as e:
        logger.exception("Column copy failed")
        raise HTTPException(500, str(e))

TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
]

def safe_parse_timestamp(value):
    if value in (None, "", 0):
        return None

    if isinstance(value, datetime):
        return value

    value = str(value).strip()

    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None

from pyiceberg.expressions import AlwaysTrue
import pyarrow as pa
from datetime import datetime

# @router.put("/update_column_with_data_copy")
# def update_column_with_data_copy(
#     column_name: str = Query(...),
#     new_column_name: str = Query(...),
#     column_type: Optional[str] = Query(None),
#     doc: Optional[str] = Query(None),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # ---------- STEP 1: Validate ----------
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#
#         # ---------- STEP 2: Add column ----------
#         with table.update_schema() as update:
#             update.add_column(
#                 new_column_name,
#                 new_type,
#                 doc=doc or old_field.doc
#             )
#
#         # ---------- STEP 3: Read existing data ----------
#         arrow_tbl = table.scan().to_arrow()
#         data = {}
#
#         for field in table.schema().fields:
#             name = field.name
#
#             if name == new_column_name:
#                 src = arrow_tbl[column_name].to_pylist()
#
#                 # ---------- TYPE SAFE COPY ----------
#                 if column_type == "timestamp":
#                     values = [
#                         datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
#                         if isinstance(v, str) else None
#                         for v in src
#                     ]
#                     data[name] = pa.array(values, type=pa.timestamp("us"))
#                 else:
#                     data[name] = pa.array(src)
#
#             else:
#                 data[name] = arrow_tbl[name]
#
#         # ---------- STEP 4: Build Arrow table ----------
#         final_arrow = pa.Table.from_arrays(
#             list(data.values()),
#             names=list(data.keys())
#         )
#
#         # ---------- STEP 5: Iceberg-safe overwrite ----------
#         table.overwrite_by_filter(
#             AlwaysTrue(),
#             final_arrow
#         )
#
#         return {
#             "status": "success",
#             "message": f"Column '{new_column_name}' copied from '{column_name}'",
#             "rows_updated": final_arrow.num_rows
#         }
#
#     except Exception as e:
#         logger.exception("Column copy failed")
#         raise HTTPException(500, str(e))

@router.put("/update_column_with_data_copy")
def update_column_with_data_copy(
    column_name: str = Query(..., description="Source column"),
    new_column_name: str = Query(..., description="New column"),
    column_type: Optional[str] = Query(None, description="Target Iceberg type"),
    doc: Optional[str] = Query(None, description="Column description"),
    namespace: str = Query(...),
    table_name: str = Query(...)
):
    """
    Iceberg-safe column copy:
    - Adds new column
    - Copies data from existing column
    - Supports multiple timestamp formats
    - No pandas, no NULL corruption
    """

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # ------------------------------------------------------------
        # STEP 1: Validate source column
        # ------------------------------------------------------------
        old_field = next((f for f in table.schema().fields if f.name == column_name), None)
        if not old_field:
            raise HTTPException(404, f"Column '{column_name}' not found")

        new_type = get_iceberg_type(column_type) if column_type else old_field.field_type

        # ------------------------------------------------------------
        # STEP 2: Add new column to schema
        # ------------------------------------------------------------
        with table.update_schema() as update:
            update.add_column(
                new_column_name,
                new_type,
                doc=doc or old_field.doc
            )

        # ------------------------------------------------------------
        # STEP 3: Read Iceberg data
        # ------------------------------------------------------------
        arrow_tbl = table.scan().to_arrow()

        if column_name not in arrow_tbl.column_names:
            raise HTTPException(400, f"Column '{column_name}' not present in data")

        # ------------------------------------------------------------
        # STEP 4: Build Arrow table (schema-aligned)
        # ------------------------------------------------------------
        arrays = []
        names = []

        for field in table.schema().fields:
            name = field.name

            # ---- New column: copy + convert ----
            if name == new_column_name:
                src_values = arrow_tbl[column_name].to_pylist()

                if column_type == "timestamp":
                    parsed = [safe_parse_timestamp(v) for v in src_values]
                    arr = pa.array(parsed, type=pa.timestamp("us"))
                else:
                    arr = pa.array(src_values)

                arrays.append(arr)
                names.append(name)

            # ---- Existing columns ----
            else:
                arrays.append(arrow_tbl[name])
                names.append(name)

        final_arrow = pa.Table.from_arrays(arrays, names=names)

        # ------------------------------------------------------------
        # STEP 5: Iceberg-safe overwrite
        # ------------------------------------------------------------
        table.overwrite_by_filter(
            AlwaysTrue(),
            final_arrow
        )

        return {
            "status": "success",
            "message": f"Column '{new_column_name}' copied from '{column_name}'",
            "rows_updated": final_arrow.num_rows
        }

    except Exception as e:
        logger.exception("Iceberg column copy failed")
        raise HTTPException(500, str(e))

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
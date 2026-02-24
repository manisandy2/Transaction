from fastapi import APIRouter,Query,HTTPException
from core.catalog_client import get_catalog_client
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from typing import Optional
from pyiceberg.expressions import AlwaysTrue
import pyarrow as pa
from .time_zone_utility import safe_parse_timestamp
from datetime import datetime
from .iceberg_utility import get_iceberg_type

router = APIRouter(prefix="/columns", tags=["columns"])

import logging

logger = logging.getLogger(__name__)

@router.put("/add-column")
def add_column(
        # request: AddColumnRequest):
    column_name: str = Query(..., description="New column name"),
    column_type: str = Query(..., description="Iceberg column type (required)"),
    doc: Optional[str] = Query(None, description="Column description (optional)"),
    namespace: str = Query(...),
    table_name: str = Query(...)
):

    try:
        catalog = get_catalog_client()
        identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(identifier)

        # ---- Check if column already exists ----
        existing_field = next(
            (f for f in table.schema().fields if f.name == column_name),
            None
        )

        if existing_field:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column_name}' already exists."
            )

        # ---- Get Iceberg Type ----
        iceberg_type = get_iceberg_type(column_type)

        # ---- Add Column ----
        with table.update_schema() as update:
            update.add_column(column_name, iceberg_type, doc=doc)

        logger.info(
            f"Column added | table={identifier} "
            f"column={column_name} type={column_type}"
        )

        return {
            "status": "success",
            "message": f"Column '{column_name}' added successfully.",
            "table": identifier
        }

    except Exception as e:
        logger.exception(f"Add column failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}")


# # model error
# @router.put("/update_column_with_data_copy02")
# def update_column_with_data_copy(
#     column_name: str = Query(..., description="Source column"),
#     new_column_name: str = Query(..., description="Target column"),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     """
#     Add a new column and copy data from an existing column (Iceberg-safe).
#     No type conversion.
#     """
#
#     try:
#         import pyarrow as pa
#
#         catalog = get_catalog_client()
#         identifier = f"{namespace}.{table_name}"
#         table = catalog.load_table(identifier)
#
#         # ---------- STEP 1: Validate source column ----------
#         old_field = next(
#             (f for f in table.schema().fields if f.name == column_name),
#             None
#         )
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         # ---------- STEP 2: Add new column (same type) ----------
#         with table.update_schema() as update:
#             update.add_column(
#                 new_column_name,
#                 old_field.field_type,
#                 doc=f"Copied from {column_name}"
#             )
#
#         # ---------- STEP 3: Read Iceberg data ----------
#         arrow_tbl = table.scan().to_arrow()
#         df = arrow_tbl.to_pandas()
#
#         if column_name not in df.columns:
#             raise HTTPException(
#                 400, f"Column '{column_name}' not found in data"
#             )
#
#         # ---------- STEP 4: Copy data ----------
#         df[new_column_name] = df[column_name]
#
#         # ---------- STEP 5: Iceberg-safe rewrite ----------
#         arrow_out = pa.Table.from_pandas(df, preserve_index=False)
#
#         table.overwrite_by_filter(
#             filter=True,   # full-table rewrite, Iceberg-managed
#             data=arrow_out
#         )
#
#         return {
#             "status": "success",
#             "message": f"Copied data from '{column_name}' to '{new_column_name}'",
#             "rows_updated": len(df)
#         }
#
#     except Exception as e:
#         logger.exception("Column copy failed")
#         raise HTTPException(500, str(e))

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
#         old_field = next((f for f in table.schema-data().fields if f.name == column_name), None)
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
#         for field in table.schema-data().fields:
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

# # model error
# @router.put("/update_column_with_data_copy")
# def update_column_with_data_copy(
#     column_name: str = Query(..., description="Source column"),
#     new_column_name: str = Query(..., description="New column"),
#     column_type: Optional[str] = Query(None, description="Target Iceberg type"),
#     doc: Optional[str] = Query(None, description="Column description"),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     """
#     Iceberg-safe column copy:
#     - Adds new column
#     - Copies data from existing column
#     - Supports multiple timestamp formats
#     - No pandas, no NULL corruption
#     """
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # ------------------------------------------------------------
#         # STEP 1: Validate source column
#         # ------------------------------------------------------------
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#
#         # ------------------------------------------------------------
#         # STEP 2: Add new column to schema-data
#         # ------------------------------------------------------------
#         with table.update_schema() as update:
#             update.add_column(
#                 new_column_name,
#                 new_type,
#                 doc=doc or old_field.doc
#             )
#
#         # ------------------------------------------------------------
#         # STEP 3: Read Iceberg data
#         # ------------------------------------------------------------
#         arrow_tbl = table.scan().to_arrow()
#
#         if column_name not in arrow_tbl.column_names:
#             raise HTTPException(400, f"Column '{column_name}' not present in data")
#
#         # ------------------------------------------------------------
#         # STEP 4: Build Arrow table (schema-data-aligned)
#         # ------------------------------------------------------------
#         arrays = []
#         names = []
#
#         for field in table.schema().fields:
#             name = field.name
#
#             # ---- New column: copy + convert ----
#             if name == new_column_name:
#                 src_values = arrow_tbl[column_name].to_pylist()
#
#                 if column_type == "timestamp":
#                     parsed = [safe_parse_timestamp(v) for v in src_values]
#                     arr = pa.array(parsed, type=pa.timestamp("us"))
#                 else:
#                     arr = pa.array(src_values)
#
#                 arrays.append(arr)
#                 names.append(name)
#
#             # ---- Existing columns ----
#             else:
#                 arrays.append(arrow_tbl[name])
#                 names.append(name)
#
#         final_arrow = pa.Table.from_arrays(arrays, names=names)
#
#         # ------------------------------------------------------------
#         # STEP 5: Iceberg-safe overwrite
#         # ------------------------------------------------------------
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
#         logger.exception("Iceberg column copy failed")
#         raise HTTPException(500, str(e))
@router.put("/make-column-nullable")
def make_column_nullable(
    namespace: str = Query(...),
    table_name: str = Query(...),
    column_name: str = Query(...),
):
    """
    Convert NOT NULL column to NULL allowed in Iceberg.
    Metadata-only operation.
    """

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")
        schema = table.schema()

        # 1️⃣ Validate column exists
        field = next((f for f in schema.fields if f.name == column_name), None)
        print("field",field)
        if not field:
            raise HTTPException(404, f"Column '{column_name}' not found")

        if not field.required:
            return {"message": "Column already nullable"}

        # 2️⃣ Update nullability
        with table.update_schema() as update:
            update.update_column(
                column_name,
                required=False
            )

        return {
            "status": "success",
            "message": f"Column '{column_name}' is now NULL allowed"
        }

    except Exception as e:
        raise HTTPException(500, str(e))

@router.put("/rename_column")
def rename_column(
    namespace: str = Query(...),
    table_name: str = Query(...),
    old_column_name: str = Query(..., description="Existing column name"),
    new_column_name: str = Query(..., description="New column name"),

):
    """
    Rename column in Iceberg safely.
    Metadata-only operation (no data rewrite).
    """

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Validate column exists
        field = next((f for f in table.schema().fields if f.name == old_column_name), None)
        if not field:
            raise HTTPException(404, f"Column '{old_column_name}' not found")

        # Rename column
        with table.update_schema() as update:
            update.rename_column(old_column_name, new_column_name)

        return {
            "status": "success",
            "message": f"Column renamed from '{old_column_name}' to '{new_column_name}'"
        }

    except Exception as e:
        logger.exception("Column rename failed")
        raise HTTPException(500, str(e))

@router.put("/update-identifier-column")
def update_identifier_column(
    namespace: str = Query(...),
    table_name: str = Query(...),
    new_identifier_column: str = Query(...),
):
    """
    Change Iceberg identifier-field-ids safely.
    Compatible with PyIceberg stable versions.
    """

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")
        schema = table.schema()

        # --------------------------------------------------
        # 1. Validate column exists
        # --------------------------------------------------
        field = next(
            (f for f in schema.fields if f.name == new_identifier_column),
            None
        )

        if not field:
            raise HTTPException(404, f"Column '{new_identifier_column}' not found")

        # --------------------------------------------------
        # 2. Validate NULL values
        # --------------------------------------------------
        arrow_tbl = table.scan(selected_fields=[new_identifier_column]).to_arrow()

        if arrow_tbl[new_identifier_column].null_count > 0:
            raise HTTPException(
                400,
                "Column contains NULL values. Cannot use as identifier."
            )

        # --------------------------------------------------
        # 3. Validate uniqueness
        # --------------------------------------------------
        values = arrow_tbl[new_identifier_column].to_pylist()
        if len(values) != len(set(values)):
            raise HTTPException(
                400,
                "Column contains duplicate values. Cannot use as identifier."
            )

        # --------------------------------------------------
        # 4. Update identifier-field-ids
        # --------------------------------------------------

        # Remove old identifier
        current_props = dict(table.properties)
        current_props.pop("identifier-field-ids", None)

        # Set new identifier
        current_props["identifier-field-ids"] = str(field.field_id)

        # Apply update
        table.update_properties(current_props)

        return {
            "status": "success",
            "message": f"Identifier column changed to '{new_identifier_column}'",
            "field_id": field.field_id
        }

    except Exception as e:
        logger.exception("Identifier update failed")
        raise HTTPException(500, str(e))

def parse_datetime(value: str):
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise HTTPException(400, "Invalid date format")

@router.delete("/delete-between-dates")
def delete_between_dates(
    namespace: str = Query(...),
    table_name: str = Query(...),
    date_column: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # Convert input to ISO-8601 format manually
        start_dt = datetime.fromisoformat(start_date.replace(" ", "T"))
        end_dt = datetime.fromisoformat(end_date.replace(" ", "T"))

        if start_dt > end_dt:
            raise HTTPException(400, "start_date must be <= end_date")

        # Force ISO format with 'T'
        start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

        delete_expression = (
            f"{date_column} >= '{start_iso}' "
            f"AND {date_column} <= '{end_iso}'"
        )

        print("Delete expression:", delete_expression)  # Debug

        table.delete(delete_expression)

        return {
            "status": "success",
            "deleted_between": f"{start_iso} → {end_iso}"
        }

    except Exception as e:
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
    Deletes either a column (schema-data delete) or rows in a date range (data delete).
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

        raise HTTPException(status_code=400, detail="Provide either 'column_name' for schema-data delete or 'date_column' with date range for data delete.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete operation failed: {e}")
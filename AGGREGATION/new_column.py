from fastapi import Query,HTTPException
from typing import Optional,List, Any
from core.catalog_client import get_catalog_client
from pyiceberg.expressions import AlwaysTrue
import pyarrow as pa
from datetime import datetime
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
)

import logging

logger = logging.getLogger(__name__)

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

def iceberg_to_arrow_schema(iceberg_schema):
    fields = []

    for f in iceberg_schema.fields:
        if isinstance(f.field_type, IntegerType):
            pa_type = pa.int32()
        elif isinstance(f.field_type, LongType):
            pa_type = pa.int64()
        elif isinstance(f.field_type, StringType):
            pa_type = pa.string()
        elif isinstance(f.field_type, DoubleType):
            pa_type = pa.float64()
        elif isinstance(f.field_type, BooleanType):
            pa_type = pa.bool_()
        elif isinstance(f.field_type, TimestampType):
            pa_type = pa.timestamp("us")
        elif isinstance(f.field_type, DateType):
            pa_type = pa.date32()
        else:
            raise ValueError(f"Unsupported Iceberg type: {f.field_type}")

        fields.append(
            pa.field(
                f.name,
                pa_type,
                nullable=not f.required
            )
        )

    return pa.schema(fields)

def safe_cast_column(values: List[Any], arrow_type):
    cleaned = []

    for v in values:

        if v in (None, "", " "):
            cleaned.append(None)
            continue

        try:
            if pa.types.is_string(arrow_type):
                cleaned.append(str(v))

            elif pa.types.is_integer(arrow_type):
                cleaned.append(int(v))

            elif pa.types.is_floating(arrow_type):
                cleaned.append(float(v))

            elif pa.types.is_boolean(arrow_type):
                if str(v).lower() in ("true", "1"):
                    cleaned.append(True)
                elif str(v).lower() in ("false", "0"):
                    cleaned.append(False)
                else:
                    cleaned.append(None)

            elif pa.types.is_timestamp(arrow_type):
                cleaned.append(safe_parse_timestamp(v))

            elif pa.types.is_date(arrow_type):
                dt = safe_parse_timestamp(v)
                cleaned.append(dt.date() if dt else None)

            else:
                cleaned.append(v)

        except Exception:
            cleaned.append(None)

    return cleaned

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

def normalize_for_arrow(values, pa_type):
    normalized = []

    for v in values:
        if v in (None, "", " "):
            normalized.append(None)
            continue

        if pa.types.is_string(pa_type):
            normalized.append(str(v))

        elif pa.types.is_integer(pa_type):
            normalized.append(int(v))

        elif pa.types.is_floating(pa_type):
            normalized.append(float(v))

        elif pa.types.is_boolean(pa_type):
            if str(v).lower() in ("true", "1"):
                normalized.append(True)
            elif str(v).lower() in ("false", "0"):
                normalized.append(False)
            else:
                normalized.append(None)

        else:
            normalized.append(v)

    return normalized


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
#         print("table:",table)
#         # ------------------------------------------------------------
#         # STEP 1: Validate source column
#         # ------------------------------------------------------------
#         old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#         print("new_types:",new_type)
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
#         print("arrow_tbl",arrow_tbl)
#         if column_name not in arrow_tbl.column_names:
#             raise HTTPException(400, f"Column '{column_name}' not present in data")
#
#         # ------------------------------------------------------------
#         # STEP 4: Build Arrow table (schema-data-aligned)
#         # ------------------------------------------------------------
#         print("Iceberg schema-data:", table.schema())
#         iceberg_schema = table.schema()
#         arrow_schema = iceberg_to_arrow_schema(iceberg_schema)
#
#         arrays = []
#
#         for field in iceberg_schema.fields:
#             name = field.name
#
#             if name == new_column_name:
#                 src = arrow_tbl[column_name].to_pylist()
#
#                 if isinstance(field.field_type, TimestampType):
#                     parsed = [safe_parse_timestamp(v) for v in src]
#                     arr = pa.array(parsed, type=pa.timestamp("us"))
#                 else:
#                     arr = pa.array(src, type=arrow_schema.field(name).type)
#             else:
#                 arr = arrow_tbl[name].cast(arrow_schema.field(name).type)
#
#             arrays.append(arr)
#
#         final_arrow = pa.Table.from_arrays(arrays, schema=arrow_schema)
#         print("Arrow schema-data:", final_arrow.schema)
#
#         # ------------------------------------------------------------
#         # STEP 5: Iceberg-safe overwrite
#         # ------------------------------------------------------------
#         # table.overwrite_by_filter(
#         #     AlwaysTrue(),
#         #     final_arrow
#         # )
#         table.overwrite(final_arrow)
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

# -------------------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------------------
# def update_column_with_data_copy(
#     column_name: str = Query(...),
#     new_column_name: str = Query(...),
#     column_type: Optional[str] = Query(None),
#     doc: Optional[str] = Query(None),
#     namespace: str = Query(...),
#     table_name: str = Query(...)
# ):
#     """
#     Production-safe column copy for Iceberg tables
#     Supports:
#     - Large tables
#     - Mixed data types
#     - Timestamp parsing
#     - Safe overwrite
#     """
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         # --------------------------------------------------
#         # STEP 1: Validate source column
#         # --------------------------------------------------
#         old_field = next(
#             (f for f in table.schema().fields if f.name == column_name),
#             None
#         )
#
#         if not old_field:
#             raise HTTPException(404, f"Column '{column_name}' not found")
#
#         new_type = (
#             get_iceberg_type(column_type)
#             if column_type
#             else old_field.field_type
#         )
#
#         # --------------------------------------------------
#         # STEP 2: Add new column
#         # --------------------------------------------------
#         with table.update_schema() as update:
#             update.add_column(
#                 new_column_name,
#                 new_type,
#                 doc=doc or old_field.doc
#             )
#
#         logger.info("New column added successfully")
#
#         # --------------------------------------------------
#         # STEP 3: Prepare Arrow schema
#         # --------------------------------------------------
#         iceberg_schema = table.schema()
#         arrow_schema = iceberg_to_arrow_schema(iceberg_schema)
#
#         total_rows = 0
#         batch_size = 50000
#
#         # --------------------------------------------------
#         # STEP 4: Batch Processing (Memory Safe)
#         # --------------------------------------------------
#         for batch in table.scan().to_arrow().to_batches(max_chunksize=batch_size):
#
#             src_values = batch[column_name].to_pylist()
#
#             target_type = arrow_schema.field(new_column_name).type
#
#             cleaned_values = safe_cast_column(src_values, target_type)
#
#             new_array = pa.array(cleaned_values, type=target_type)
#
#             batch = batch.append_column(new_column_name, new_array)
#
#             final_table = pa.Table.from_batches([batch], schema=arrow_schema)
#
#             # table.overwrite_by_filter(
#             #     AlwaysTrue(),
#             #     final_table
#             # )
#             table.overwrite(final_table)
#
#             total_rows += final_table.num_rows
#
#         return {
#             "status": "success",
#             "message": f"Column '{new_column_name}' copied from '{column_name}'",
#             "rows_updated": total_rows
#         }
#
#     except Exception as e:
#         logger.exception("Iceberg column copy failed")
#         raise HTTPException(500, str(e))
# column copy data
def update_column_with_data_copy(
    column_name: str,
    new_column_name: str,
    column_type: Optional[str],
    doc: Optional[str],
    namespace: str,
    table_name: str
):

    catalog = get_catalog_client()
    table = catalog.load_table(f"{namespace}.{table_name}")

    # ---------------------------
    # Validate source column
    # ---------------------------
    old_field = next(
        (f for f in table.schema().fields if f.name == column_name),
        None
    )

    if not old_field:
        raise Exception(f"Column {column_name} not found")

    new_type = (
        get_iceberg_type(column_type)
        if column_type
        else old_field.field_type
    )

    # ---------------------------
    # Add new column
    # ---------------------------
    with table.update_schema() as update:
        update.add_column(new_column_name, new_type)

    print("Column added")

    # ---------------------------
    # Read FULL table once
    # ---------------------------
    arrow_tbl = table.scan().to_arrow()

    iceberg_schema = table.schema()
    arrow_schema = iceberg_to_arrow_schema(iceberg_schema)

    arrays = []

    for field in iceberg_schema.fields:

        name = field.name

        if name == new_column_name:

            src_values = arrow_tbl[column_name].to_pylist()

            target_type = arrow_schema.field(name).type

            cleaned = [str(v) if v is not None else None for v in src_values]

            arr = pa.array(cleaned, type=target_type)

        else:
            arr = arrow_tbl[name].cast(
                arrow_schema.field(name).type
            )

        arrays.append(arr)

    final_arrow = pa.Table.from_arrays(arrays, schema=arrow_schema)

    # ---------------------------
    # SINGLE overwrite
    # ---------------------------
    table.overwrite(final_arrow)

    return {
        "status": "success",
        "rows_updated": final_arrow.num_rows
    }

dd = update_column_with_data_copy(
    column_name="pri_id",
    new_column_name="pri_id_copy",
    column_type="string",
    doc="",
    namespace="POS_Transactions",
    table_name="Transaction_vars"
)

print(dd)
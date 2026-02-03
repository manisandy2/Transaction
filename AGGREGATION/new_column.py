from fastapi import APIRouter,Query,HTTPException
from typing import Optional
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

        print("table:",table)
        # ------------------------------------------------------------
        # STEP 1: Validate source column
        # ------------------------------------------------------------
        old_field = next((f for f in table.schema().fields if f.name == column_name), None)
        if not old_field:
            raise HTTPException(404, f"Column '{column_name}' not found")

        new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
        print("new_types:",new_type)
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
        print("arrow_tbl",arrow_tbl)
        if column_name not in arrow_tbl.column_names:
            raise HTTPException(400, f"Column '{column_name}' not present in data")

        # ------------------------------------------------------------
        # STEP 4: Build Arrow table (schema-aligned)
        # ------------------------------------------------------------
        print("Iceberg schema:", table.schema())
        iceberg_schema = table.schema()
        arrow_schema = iceberg_to_arrow_schema(iceberg_schema)

        arrays = []

        for field in iceberg_schema.fields:
            name = field.name

            if name == new_column_name:
                src = arrow_tbl[column_name].to_pylist()

                if isinstance(field.field_type, TimestampType):
                    parsed = [safe_parse_timestamp(v) for v in src]
                    arr = pa.array(parsed, type=pa.timestamp("us"))
                else:
                    arr = pa.array(src, type=arrow_schema.field(name).type)
            else:
                arr = arrow_tbl[name].cast(arrow_schema.field(name).type)

            arrays.append(arr)

        final_arrow = pa.Table.from_arrays(arrays, schema=arrow_schema)
        print("Arrow schema:", final_arrow.schema)

        # ------------------------------------------------------------
        # STEP 5: Iceberg-safe overwrite
        # ------------------------------------------------------------
        # table.overwrite_by_filter(
        #     AlwaysTrue(),
        #     final_arrow
        # )
        table.overwrite(final_arrow)

        return {
            "status": "success",
            "message": f"Column '{new_column_name}' copied from '{column_name}'",
            "rows_updated": final_arrow.num_rows
        }

    except Exception as e:
        logger.exception("Iceberg column copy failed")
        raise HTTPException(500, str(e))

dd = update_column_with_data_copy(
    column_name="bill_time__c",
    new_column_name="bill_time__c_c_c",
    column_type="timestamp",
    doc="Billing time in C",
    namespace="POS_Transactions",
    table_name="Transaction"
)

print(dd)
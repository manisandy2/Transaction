from pyiceberg.types import *
import pyarrow as pa
import decimal
import json
import datetime
from botocore.exceptions import ClientError, BotoCoreError
from fastapi import HTTPException
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.exceptions import NoSuchTableError

type_mapping = {
    "int": LongType(),
    'bigint': LongType(),
    'varchar': StringType(),
    'char': StringType(),
    'text': StringType(),
    'longtext': StringType(),
    'date': DateType(),
    'datetime': TimestampType(),
    'timestamp': TimestampType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'tinyint': BooleanType()
}

arrow_mapping = {
    # 'int': pa.int32(),
    "int": pa.int64(),
    'bigint': pa.int64(),
    'varchar': pa.string(),
    'char': pa.string(),
    'text': pa.string(),
    'longtext': pa.string(),
    'date': pa.date32(),
    'datetime': pa.timestamp('ms'),
    'timestamp': pa.timestamp('ms'),
    'float': pa.float32(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'tinyint': pa.bool_(),
    'bit': pa.bool_(),
    # 'decimal': lambda p=18, s=6: pa.decimal128(p, s)
    'decimal' : pa.decimal128(18, 6)
}

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)

def upload_file(r2_client, bucket, r2_key, body):
    try:
        r2_client.put_object(Bucket=bucket, Key=r2_key, Body=body)
        return r2_key
    except ClientError as e:
        raise HTTPException(status_code=400, detail=f"R2 Client error for {r2_key}: {e.response['Error']['Message']}")

    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail=f"R2 BotoCore error for {r2_key}: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error for {r2_key}: {str(e)}")

def get_or_create_table(catalog, table_identifier,iceberg_schema):
    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        tbl = catalog.create_table(table_identifier, schema=iceberg_schema)
    return tbl

def fetch_mysql_data(mysql_creds, dbname: str, start_range: int, end_range: int):

    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname, start_range, end_range)
        return description, rows
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"MySQL fetch error for db={dbname}, range=({start_range}, {end_range}): {str(e)}"
        )

def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
    converted = {}

    for field in arrow_schema:
        name = field.name
        dtype = field.type
        val = row.get(name)
        # print("name", name, "dtype", dtype, "val", val)
        # Handle None / Empty
        if val in (None, "", "NULL"):
            converted[name] = None
            continue

        # ---- Type-based Conversion ----
        try:
            # Integer
            if pa.types.is_integer(dtype):
                converted[name] = int(val)

            # Floating point
            elif pa.types.is_floating(dtype):
                converted[name] = float(val)

            # Boolean
            elif pa.types.is_boolean(dtype):
                converted[name] = str(val).lower() in ("true", "1", "yes")

            # Timestamp / Date
            elif pa.types.is_timestamp(dtype) or "date" in name.lower():
                if isinstance(val, str):
                    val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
                if isinstance(val, datetime):
                    converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
                    converted[f"{name}_year"] = val.year
                    converted[f"{name}_month"] = val.month
                    converted[f"{name}_day"] = val.day
                else:
                    converted[name] = None

            # String / Bytes
            elif pa.types.is_string(dtype):
                converted[name] = str(val)

            else:
                converted[name] = str(val)

        except Exception as e:
            # print("Failed to convert column", row)
            #
            print("name", name, "dtype", dtype, "val", val ,{e})
            converted[name] = None

    return converted

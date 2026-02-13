from fastapi import FastAPI, Query, HTTPException
# from routers import namespace.router
import json
import decimal
import datetime
from mysql.connector import Error
import logging
from routers import namespace as transaction_namespace
from routers import bucket_data_store01 as transaction_bds
from routers import table as transaction_table
from routers import meta_data as transaction_meta_data
from routers import transaction as data_insert
from routers import date_transaction as date_insert
from routers import filters as filters
from routers import multipart as multipart_data
from routers import parquet as parquet_table
from routers import columns as columns
# from routers import avro as avro_files

from routers import schema as schema_schema
from routers import Inspecting_tables as Inspecting_tables
from routers import partition as partition_schema
from routers import error_records
from core.mysql_client import MysqlCatalog

logger = logging.getLogger(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)

app = FastAPI()
# app.include_router(bucket.router)
# app.include_router(transaction_bucket.router)
app.include_router(transaction_namespace.router)
# app.include_router(transaction_database.router)
app.include_router(transaction_bds.router)
app.include_router(transaction_table.router)
app.include_router(transaction_meta_data.router)
app.include_router(data_insert.router)
app.include_router(date_insert.router)
app.include_router(filters.router)
app.include_router(columns.router)
app.include_router(multipart_data.router)
app.include_router(parquet_table.router)
# app.include_router(avro_files.router)
app.include_router(schema_schema.router)
# app.include_router(duckdb_ph.router)
app.include_router(Inspecting_tables.router)
app.include_router(partition_schema.router)
app.include_router(error_records.router)
# app.include_router(r2_catalog_create_table.router)


ALLOWED_TABLES = ["Transaction",]

@app.get("/")
def root():
    tables_name = ["Transaction", ]

    return {"message": "API is running",
            "version": "1.0",
            "Tables": tables_name
            }



@app.get("/table/schema")
def table_schema(table_name: str = Query(..., description="Table name")):
    catalog = MysqlCatalog()
    try:
        description = catalog.get_describe(table_name)
        if not description:
            raise HTTPException(
                status_code=404,
                detail={
                    "error_code": "TABLE_NOT_FOUND",
                    "message": f"Table '{table_name}' not found"
                }
            )
        return {"schema": description}

    except Error as e:
        # Database-related error
        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "DB_ERROR",
                "message": str(e)
            }
        )
    except Exception as e:
        # Unexpected error
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "BAD_REQUEST",
                "message": str(e)
            }
        )
    finally:
        catalog.close()


from fastapi import FastAPI, APIRouter, Query, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, DateType,TimestampType
import logging
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, ValidationError
from sqlalchemy.sql.sqltypes import NullType

from core.catalog_client import get_catalog_client
from fastapi import APIRouter,Query,HTTPException

app = FastAPI()
router = APIRouter(prefix="", tags=["Schema"])
logger = logging.getLogger(__name__)


@router.get("/Schema/list")
def list_schema(
        namespace: str = Query(default="POS_transactions",description="Namespace"),
        table_name: str = Query(default="Transaction",description="Table name"),

):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        return {"status": "success", "schema": table.schema().fields}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/Schema/create")
def create_table(
    namespace: str = Query(default="POS_transactions",description="Namespace"),
    table_name: str = Query(default="Transaction",description="Table name"),
    columns: list = Query(..., description="List of columns as [{'name':'col1','type':'string','required':True}, ...]")
):
    try:
        catalog = get_catalog_client()
        fields = []
        for idx, col in enumerate(columns):
            type_map = {"string": StringType(), "long": LongType(), "date": DateType()}
            col_type = type_map.get(col["type"].lower(), StringType())
            fields.append(NestedField(idx + 1, col["name"], col_type, col.get("required", False)))

        schema = Schema(*fields)
        catalog.create_table(f"{namespace}.{table_name}", schema=schema)
        return {"status": "success", "message": f"Table '{table_name}' created successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/Schema/update")
def update_schema(
    namespace: str = Query(default="POS_transactions",description="Namespace"),
    table_name: str = Query(default="Transaction",description="Table name"),
    column_name: str = Query(...),
    new_type: str = Query(..., description="New type: string, long, date,datetime")
):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(f"{namespace}.{table_name}")


        old_field = next((f for f in table.schema().fields if f.name.lower() == column_name.lower()), None)

        print("#"*100)
        if not old_field:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

        # Map string type to Iceberg type
        type_map = {
            "string": StringType(),
            "long": LongType(),
            "date": DateType(),
            "datetime": TimestampType()
        }
        iceberg_type = type_map.get(new_type.lower())
        # iceberg_type = type_map.get(new_type)
        if not iceberg_type:
            raise HTTPException(status_code=400, detail=f"Unsupported type '{new_type}'")

        try:
            (
                table.update_schema(allow_incompatible_changes=True)
                .update_column(column_name, iceberg_type)
                .commit()
            )
        except NoSuchTableError:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=f"Schema validation failed: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Schema update failed: {str(e)}")

            # Reload table to get new schema
        updated_table = catalog.load_table(table_identifier)
        new_field = next((f for f in updated_table.schema().fields if f.name.lower() == column_name.lower()), None)

        return {
            "status": "success",
            "column": column_name,
            "old_type": str(old_field.field_type),
            "new_type": str(new_field.field_type),
            "message": f"Column '{column_name}' successfully updated from '{old_field.field_type}' to '{new_field.field_type}'",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.delete("/Schema/delete")
def delete_table(namespace: str = Query(...), table_name: str = Query(...)):
    try:
        catalog = get_catalog_client()
        catalog.drop_table(f"{namespace}.{table_name}")
        return {"status": "success", "message": f"Table '{table_name}' deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

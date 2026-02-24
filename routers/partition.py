from fastapi import APIRouter, Query, HTTPException
from pyiceberg.exceptions import NoSuchTableError, ValidationError
from core.catalog_client import get_catalog_client
from .iceberg_utility import get_transform

router = APIRouter(prefix="/Partition", tags=["Partition"])

@router.get("/list")
def list_partitions(
    namespace: str = Query("POS_transactions", description="Namespace name"),
    table_name: str = Query("Transaction", description="Table name")
):
    """
    List all partition fields in the Iceberg table's current partition spec.
    Returns each field's id, name, source column, and transform type.
    """
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"

        # Load the table
        table = catalog.load_table(table_identifier)
        spec = table.spec()  # Current partition spec

        if not spec.fields:
            return {
                "status": "success",
                "table": table_identifier,
                "partitions": [],
                "message": "No partitions defined in this table."
            }

        # Collect partition details
        partitions = []
        for field in spec.fields:
            partitions.append({
                "partition_field_id": field.field_id,
                "source_column_name": field.name,
                "source_column_id": field.source_id,
                "transform": str(field.transform),
            })

        return {
            "status": "success",
            "table": table_identifier,
            "partition_count": len(partitions),
            "partitions": partitions
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in namespace '{namespace}'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing partitions: {str(e)}")


# ✅ CREATE / ADD PARTITION SPEC
@router.post("/create")
def create_partition(
    namespace: str = Query("POS_transactions", description="Namespace"),
    table_name: str = Query("Transaction", description="Table name"),
    column_name: str = Query(..., description="Column to partition on"),
    transform: str = Query("identity", description="Transform type (identity, year, month, day, bucket, truncate)"),
    arg: int | None = Query(None, description="Optional transform argument (e.g., bucket size or truncate length)")
):
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(table_identifier)

        tr = get_transform(transform, arg)
        (
            table.update_spec()
            .add_field(column_name, tr)
            .commit()
        )

        return {
            "status": "success",
            "table": table_identifier,
            "action": "create",
            "partition": f"{transform}({column_name})",
            "message": f"Partition '{transform}({column_name})' successfully created"
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=f"Partition validation failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Partition create failed: {str(e)}")



@router.put("/update")
def update_partition(
    namespace: str = Query("POS_transactions", description="Namespace"),
    table_name: str = Query("Transaction", description="Table name"),
    column_name: str = Query(...),
    new_transform: str = Query(...),
    arg: int | None = Query(None)
):
    """
    Replace existing partition spec by redefining how the column is partitioned.
    """
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(table_identifier)

        tr = get_transform(new_transform, arg)
        (
            table.update_spec()
            .remove_field(column_name)
            .add_field(column_name, tr)
            .commit()
        )

        return {
            "status": "success",
            "table": table_identifier,
            "action": "update",
            "partition": f"{new_transform}({column_name})",
            "message": f"Partition for column '{column_name}' updated to '{new_transform}'"
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Partition update failed: {str(e)}")


# ❌ DELETE PARTITION SPEC
@router.delete("/delete")
def delete_partition(
    namespace: str = Query("POS_transactions", description="Namespace"),
    table_name: str = Query("Transaction", description="Table name"),
    column_name: str = Query(...)
):
    """
    Remove partition field from Iceberg spec (deletes logical partitioning rule).
    """
    try:
        catalog = get_catalog_client()
        table_identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(table_identifier)

        (
            table.update_spec()
            .remove_field(column_name)
            .commit()
        )

        return {
            "status": "success",
            "table": table_identifier,
            "action": "delete",
            "message": f"Partition on column '{column_name}' successfully removed"
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Partition delete failed: {str(e)}")
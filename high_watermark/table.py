from fastapi import APIRouter,Query,HTTPException
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType
from pyiceberg.partitioning import PartitionSpec
from datetime import datetime
from core.catalog_client import get_catalog_client
router = APIRouter(prefix="/high-watermark", tags=["high-watermark"])

def create_ingestion_tracking_table():

    namespace = "POS_Transactions"
    table_name = "ingestion_tracking"

    catalog = get_catalog_client()

    identifier = f"{namespace}.{table_name}"

    # Check if table already exists
    try:
        catalog.load_table(identifier)
        print("Table already exists.")
        return
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "namespace", StringType(), required=True),
        NestedField(2, "table_name", StringType(), required=True),
        NestedField(3, "high_watermark", StringType(), required=True),
        NestedField(4, "updated_at", TimestampType(), required=True)
    )

    spec = PartitionSpec()  # no partition needed

    catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec
    )

    print("ingestion_tracking table created successfully.")


if __name__ == "__main__":
    create_ingestion_tracking_table()
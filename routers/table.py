from fastapi import APIRouter,Query,HTTPException
from .transaction_utility import transaction_schema
from core.catalog_client import get_catalog_client
from pyiceberg.partitioning import PartitionSpec,PartitionField
from pyiceberg.transforms import YearTransform
from .dump_utility import *
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError,NoSuchTableError

router = APIRouter(prefix="/table", tags=["Tables"])

@router.get("/list")
def get_tables(
        namespace: str = Query(..., description="Namespace to list tables from"),
):
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")

@router.post("/create")
def create_transaction(
        namespace: str = Query("POS_transactions"),
        table_name: str = Query("Transaction", description="Table name"),
):
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema-data

    transaction_data_schema = Schema(*transaction_schema)

    # print(transaction_schema.find_field("Bill_Date__c").field_id+1)
    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        # PartitionField(
        #     source_id=transaction_data_schema.find_field("store_code__c").field_id,
        #     field_id=1001,
        #     transform=IdentityTransform(),
        #     name="store_code",
        # ),
        PartitionField(
            source_id=transaction_data_schema.find_field("Bill_Date__c").field_id,
            field_id=1001,
            transform=YearTransform(),
            name="year",
        ),
    )
    # transaction_partition_spec = PartitionSpec(
    #     # Bucket on mobile number (FAST SEARCH)
    #     PartitionField(
    #         source_id=transaction_data_schema.find_field("customer_mobile__c").field_id,
    #         field_id=1001,
    #         transform=BucketTransform(512),  # 👈 KEY POINT
    #         name="mobile_bucket",
    #     ),

    # Time pruning (optional but recommended)
    # PartitionField(
    #     source_id=transaction_data_schema.find_field("Bill_Date__c").field_id,
    #     field_id=1002,
    #     transform=MonthTransform(),
    #     name="bill_month",
    # ),
    # )

    # Step 3: Connect to catalog
    catalog = get_catalog_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_data_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1",
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                "write.sort.order": "Bill_Date__c ASC, customer_mobile__c, IMEINumber__c, Invoice_Amount__c",
                # "write.metadata.sort-order": "customer_mobile__c, IMEINumber__c",
                # "write.sort.order": "customerId,customer_mobile__c",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_data_schema.fields],
            # "partitions": [f.name for f in transaction_partition_spec.fields],
        }

    except TableAlreadyExistsError:
        # return {"status": "exists", "table": table_identifier}
        table = catalog.load_table(table_identifier)
        snapshot = table.current_snapshot()
        total_records = snapshot.summary.get("total-records", 0) if snapshot else 0

        return {
            "status": "exists",
            "table": table_identifier,
            "total_records": total_records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


@router.post("/create-bill-header")
def create_transaction(
        namespace: str = Query("POS_transactions"),
        table_name: str = Query("bill_header", description="Table name"),
):
    table_identifier = f"{namespace}.{table_name}"
    transaction_data_schema = Schema(*bill_header_schema)

    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=transaction_data_schema.find_field("bill_date").field_id,
            field_id=1001,
            transform=YearTransform(),
            name="year",
        ),
    )

    catalog = get_catalog_client()

    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass


    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_data_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1",
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                "write.sort.order": "batch_id ASC, store_code, bill_no, bill_date",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_data_schema.fields],

        }

    except TableAlreadyExistsError:

        table = catalog.load_table(table_identifier)
        snapshot = table.current_snapshot()
        total_records = snapshot.summary.get("total-records", 0) if snapshot else 0

        return {
            "status": "exists",
            "table": table_identifier,
            "total_records": total_records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")

@router.post("/create-bill-items")
def create_transaction(
        namespace: str = Query("POS_transactions"),
        table_name: str = Query("bill_items", description="Table name"),
):
    table_identifier = f"{namespace}.{table_name}"
    transaction_data_schema = Schema(*bill_items_schema)

    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=transaction_data_schema.find_field("bill_date").field_id,
            field_id=1001,
            transform=YearTransform(),
            name="year",
        ),
    )

    catalog = get_catalog_client()

    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass


    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_data_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "identifier-field-ids": "1",
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "hierarchical",   # hierarchical & directory
                "write.sort.order": "batch_id ASC, store_code, bill_no, bill_date",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_data_schema.fields],

        }

    except TableAlreadyExistsError:

        table = catalog.load_table(table_identifier)
        snapshot = table.current_snapshot()
        total_records = snapshot.summary.get("total-records", 0) if snapshot else 0

        return {
            "status": "exists",
            "table": table_identifier,
            "total_records": total_records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


@router.post("/rename")
def rename_table(
    namespace: str = Query(..., description="Namespace containing the table"),
    old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
    new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),

):

    catalog = get_catalog_client()
    try:

        old_identifier = f"{namespace}.{old_table_name}"
        new_identifier = f"{namespace}.{new_table_name}"

        catalog.rename_table(old_identifier, new_identifier)

        return {
            "status": "success",
            "message": f"Table renamed from '{old_table_name}' to '{new_table_name}' in namespace '{namespace}' successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rename table '{old_table_name}' in namespace '{namespace}': {str(e)}")

    finally:
        try:
            catalog.close()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to close catalog: {str(e)}")



@router.delete("/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop"),

):

    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name,purge_requested=True)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")

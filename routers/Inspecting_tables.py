from fastapi import APIRouter, HTTPException, Query
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from datetime import datetime
from core.catalog_client import get_catalog_client
# from pyiceberg.io import PyArrowFileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
# from pyiceberg.manifest import ManifestList, DataFile
from pyiceberg.manifest import read_manifest_list


router = APIRouter(prefix="", tags=["Inspect"])

@router.get("/inspect-snapshots")
def inspect_iceberg_snapshots(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect all snapshots of an Iceberg table.
    Shows snapshot IDs, timestamps, operations, and summary info.
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        # Load Iceberg catalog (e.g., REST, Hadoop, or local)
        catalog = get_catalog_client()  # or use your get_catalog_client()
        table = catalog.load_table(table_identifier)

        snapshots = []
        for snapshot in table.snapshots():
            summary = snapshot.summary or {}
            snapshots.append({
                "snapshot_id": str(snapshot.snapshot_id),
                "parent_snapshot_id": str(
                    getattr(snapshot, "parent_snapshot_id", None)
                ) if getattr(snapshot, "parent_snapshot_id", None) else None,
                "sequence_number": snapshot.sequence_number,
                # "is_current": str(snapshot.snapshot_id) == current_snapshot_id,
                # "timestamp": datetime.fromtimestamp(
                #     snapshot.timestamp_millis / 1000
                # ).isoformat(),
                "operation": summary.get("operation"),
                "manifest_list": str(snapshot.manifest_list)
                if snapshot.manifest_list else None,
                "added_files": summary.get("added-data-files"),
                "deleted_files": summary.get("deleted-data-files"),
                "total_files": summary.get("total-data-files"),
                "added_records": summary.get("added-records"),
                "deleted_records": summary.get("deleted-records"),
                "total_records": summary.get("total-records"),
            })

        return {
            "status": "success",
            "table": table_identifier,
            "snapshot_count": len(snapshots),
            "snapshots": snapshots
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg snapshots: {str(e)}")


@router.get("/inspect-partitions")
def inspect_iceberg_partitions(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect the partition specification of an Iceberg table.
    Shows field IDs, source columns, transforms, and partition names.
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)

        partition_spec = table.spec()
        partitions = []

        for field in partition_spec.fields:
            partitions.append({
                "field_id": field.field_id,
                "source_column_id": field.source_id,
                "partition_name": field.name,
                "transform": str(field.transform),
            })

        return {
            "status": "success",
            "table": table_identifier,
            "partition_count": len(partitions),
            "partitions": partitions
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg partitions: {str(e)}")


# @router.get("/iceberg/inspect-entries")
# def inspect_iceberg_entries(
#     namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     table_name: str = Query("transaction01", description="Iceberg table name")
# ):
#     """
#     Inspect all manifest entries (data files) of the current Iceberg table snapshot.
#     Compatible with all recent PyIceberg versions.
#     """
#     from datetime import datetime
#     from pyiceberg.exceptions import NoSuchTableError
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(table_identifier)
#         snapshot = table.current_snapshot()
#
#         if not snapshot:
#             return {
#                 "status": "success",
#                 "message": f"No active snapshot found for {table_identifier}"
#             }
#
#         entries = []
#         # --- Read the manifest list using the table's IO layer ---
#         manifest_list_input = table.io.new_input_file(snapshot.manifest_list)
#         manifest_list = table.io.read_manifest_list(manifest_list_input)
#
#         # --- Loop through manifest files ---
#         for manifest in manifest_list:
#             manifest_input = table.io.new_input_file(manifest.path)
#             manifest_file = table.io.read_manifest(manifest_input)
#
#             # --- Iterate through manifest entries (data files) ---
#             for entry in manifest_file:
#                 data_file = entry.data_file
#                 entries.append({
#                     "data_file": data_file.file_path,
#                     "partition": data_file.partition,
#                     "record_count": data_file.record_count,
#                     "file_size_bytes": data_file.file_size_in_bytes,
#                     "content": str(data_file.content),
#                     "status": str(entry.status),
#                 })
#
#         return {
#             "status": "success",
#             "table": table_identifier,
#             "snapshot_id": str(snapshot.snapshot_id),
#             "entry_count": len(entries),
#             "entries": entries
#         }
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg entries: {str(e)}")

# error
# @router.get("/iceberg/inspect-entries")
# def inspect_iceberg_entries(
#     namespace: str = Query("pos_transactions01", description="Iceberg namespace name"),
#     table_name: str = Query("transaction01", description="Iceberg table name")
# ):
#     """
#     Inspect all manifest entries (data files) of the current Iceberg table snapshot.
#     Compatible with PyIceberg 0.4.x and newer versions.
#     """
#     from pyiceberg.io.pyarrow import PyArrowFileIO
#     from pyiceberg.exceptions import NoSuchTableError
#     from datetime import datetime
#     import os
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(table_identifier)
#         snapshot = table.current_snapshot()
#
#         if not snapshot:
#             return {
#                 "status": "success",
#                 "message": f"No active snapshot found for {table_identifier}"
#             }
#
#         entries = []
#         io = PyArrowFileIO()  # ✅ Create IO handler manually
#
#         # --- Step 1: Load manifest list file ---
#         manifest_list = io.read_manifest_list(snapshot.manifest_list)
#
#         # --- Step 2: Iterate over each manifest file ---
#         for manifest in manifest_list:
#             manifest_file = io.read_manifest(manifest.path)
#
#             # --- Step 3: Loop through entries ---
#             for entry in manifest_file:
#                 data_file = entry.data_file
#                 entries.append({
#                     "data_file": data_file.file_path,
#                     "partition": data_file.partition,
#                     "record_count": data_file.record_count,
#                     "file_size_bytes": data_file.file_size_in_bytes,
#                     "content": str(data_file.content),
#                     "status": str(entry.status),
#                 })
#
#         return {
#             "status": "success",
#             "table": table_identifier,
#             "snapshot_id": str(snapshot.snapshot_id),
#             "entry_count": len(entries),
#             "entries": entries
#         }
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg entries: {str(e)}")

@router.get("/inspect-refs")
def inspect_iceberg_refs(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect all snapshot references (branches/tags) in an Iceberg table.
    Shows reference name, type (branch/tag), snapshot_id, and retention policies.
    """
    from pyiceberg.exceptions import NoSuchTableError
    from datetime import datetime

    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)

        refs = table.refs() if callable(table.refs) else table.refs
        refs_info = []


        for ref_name, ref in (refs or {}).items():
            refs_info.append({
                "name": ref_name,
                # "type":ref.type,
                # "type": (
                #     ref.type.value
                #     if hasattr(ref.type, "value")
                #     else str(ref.type)
                # ),
                # "snapshot_id": str(ref.snapshot_id),
                "max_ref_age_ms": ref.max_ref_age_ms,
                "min_snapshots_to_keep": ref.min_snapshots_to_keep,
                "max_snapshot_age_ms": ref.max_snapshot_age_ms,
            })

        return {
            "status": "success",
            "table": table_identifier,
            "ref_count": len(refs_info),
            "refs": refs_info
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg refs: {str(e)}")

@router.get("/inspect-manifests")
def inspect_iceberg_manifests(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect manifest files of the current snapshot in an Iceberg table.
    Lists each manifest file with its path, partition spec, and file count.
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)

        snapshot = table.current_snapshot()
        if not snapshot:
            raise HTTPException(status_code=404, detail="No snapshot found for this table")

        manifest_list_path = snapshot.manifest_list
        io = PyArrowFileIO()  # used to read manifest list
        manifest_list = ManifestList.read(io, manifest_list_path)

        manifest_info = []
        for m in manifest_list.manifests:
            manifest_info.append({
                "manifest_path": m.manifest_path,
                "partition_spec_id": m.partition_spec_id,
                "added_files_count": m.added_files_count,
                "existing_files_count": m.existing_files_count,
                "deleted_files_count": m.deleted_files_count,
                "added_rows_count": m.added_rows_count,
                "existing_rows_count": m.existing_rows_count,
                "deleted_rows_count": m.deleted_rows_count,
                "sequence_number": m.sequence_number,
                "min_sequence_number": m.min_sequence_number,
            })

        return {
            "status": "success",
            "table": table_identifier,
            "snapshot_id": snapshot.snapshot_id,
            "manifest_list": manifest_list_path,
            "manifest_count": len(manifest_info),
            "manifests": manifest_info
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg manifests: {str(e)}")

@router.get("/inspect-history")
def inspect_iceberg_history(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect the snapshot history of an Iceberg table.
    Lists all snapshots with parent linkage and timestamps.
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)

        print("Data",table.history())
        history_info = []
        for entry in table.history():
            history_info.append({
                "snapshot_id": str(entry.snapshot_id),
                "parent_id": str(entry.parent_id) if entry.parent_id else None,
                "timestamp": datetime.fromtimestamp(entry.timestamp_millis / 1000).isoformat()
            })

        return {
            "status": "success",
            "table": table_identifier,
            "history_count": len(history_info),
            "history": history_info
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg history: {str(e)}")

@router.get("/inspect-files")
def inspect_iceberg_files(
    namespace: str = Query("POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query("Transaction", description="Iceberg table name")
):
    """
    Inspect all data files in the current snapshot of an Iceberg table.
    Returns the list of Parquet (or other) files and their metadata.
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)

        # snapshot = table.current_snapshot()
        snapshot = table.current_snapshot()
        print("Data",snapshot.history())
        if not snapshot:
            raise HTTPException(status_code=404, detail="No snapshot found for this table")

        io = PyArrowFileIO()
        print("Io:io",io)
        manifest_list = read_manifest_list(io, snapshot.manifest_list())
        print("Manifest list:",manifest_list)
        data_files_info = []
        for manifest in manifest_list:
            manifest_path = manifest.manifest_path

            # ✅ Step 2: Read data file entries in this manifest
            entries = read_manifest_list(io, manifest_path)

            for entry in entries:
                df = entry.data_file
                data_files_info.append({
                    "manifest_path": manifest_path,
                    "status": entry.status.name if entry.status else None,  # ADDED / EXISTING / DELETED
                    "file_path": df.file_path,
                    "file_format": df.file_format,
                    "record_count": df.record_count,
                    "file_size_in_bytes": df.file_size_in_bytes,
                    "partition": df.partition,
                    "content": df.content.name if df.content else None,
                })

        return {
            "status": "success",
            "table": table_identifier,
            "snapshot_id": snapshot.snapshot_id,
            "total_files": len(data_files_info),
            "data_files": data_files_info
        }

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found in catalog")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect Iceberg files: {str(e)}")

@router.get("/table-total/count")
def iceberg_table_count(
    name_space: str = Query(default="POS_transactions", description="Iceberg namespace name"),
    table_name: str = Query(default="Transaction", description="Iceberg table name")
):
    table_identifier = f"{name_space}.{table_name}"
    catalog = get_catalog_client()

    try:
        tbl = catalog.load_table(table_identifier)
        print("data",tbl)
        row_count = tbl.scan().count()
        return {"table": table_identifier, "count": row_count}

    except NoSuchTableError:
        raise HTTPException(404, f"Table not found: {table_identifier}")
    except Exception as e:
        raise HTTPException(500, f"Error counting rows: {str(e)}")
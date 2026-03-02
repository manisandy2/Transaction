import time
import json
# from .utility import schema-data,clean_rows
from .utility import schema,clean_rows
from .transaction_utility import *
from core.catalog_client import get_catalog_client
from routers.table_utility import clean_bill_header,clean_bill_items
from .table_utility import process_chunk

from bucket_to_catalog.read_bucket import list_json_files
from core.r2_client import get_r2_client



def split_bill_json(raw_data):
    """
    Split bill JSON into header row and item rows
    """

    header = raw_data.copy()
    items = header.pop("output", [])  # remove items from header

    bill_no = header.get("bill_no")
    bill_date = header.get("bill_date")
    batch_id = header.get("batch_id")

    item_rows = []

    for item in items:
        row = item.copy()
        row["bill_no"] = bill_no
        row["bill_date"] = bill_date
        row["batch_id"] = batch_id
        row["bill_transcation_no"] = header.get("bill_transcation_no")
        item_rows.append(row)

    return header, item_rows

def ingest_bill_from_r2():

    namespace = "POS_Transactions"
    header_table_name = "bill_header"
    items_table_name = "bill_items"

    bucket = "pos-transaction"
    prefix = "dump/2026/01/29/"
    batch_size = 50

    start_time = time.time()

    catalog = get_catalog_client()

    header_table = catalog.load_table(f"{namespace}.{header_table_name}")
    items_table = catalog.load_table(f"{namespace}.{items_table_name}")

    header_schema = header_table.schema().as_arrow()
    items_schema = items_table.schema().as_arrow()

    keys = list_json_files(bucket=bucket, prefix=prefix)
    if not keys:
        return {"success": True, "message": "No files found"}

    total_headers = 0
    total_items = 0

    r2 = get_r2_client()

    for i in range(0, len(keys), batch_size):

        batch_keys = keys[i:i + batch_size]

        header_rows = []
        item_rows = []

        # -------------------------------
        # Read R2 Files
        # -------------------------------
        for key in batch_keys:
            try:
                obj = r2.get_object(Bucket=bucket, Key=key)
                raw_data = json.loads(obj["Body"].read())

                header, items = split_bill_json(raw_data)
                print("header",header)
                print("items",items)
                header_rows.append(header)
                item_rows.extend(items)

            except Exception as e:
                logger.error(f"Error reading file {key}: {e}")

        # -------------------------------
        # Process Header
        # -------------------------------
        if header_rows:

            cleaned_headers = clean_bill_header(header_rows)

            header_arrow, _ = process_chunk(
                cleaned_headers,
                header_schema
            )

            if header_arrow and header_arrow.num_rows > 0:
                header_table.append(header_arrow)
                total_headers += header_arrow.num_rows

        # -------------------------------
        # Process Items
        # -------------------------------
        if item_rows:

            cleaned_items = clean_bill_items(item_rows)

            items_arrow, _ = process_chunk(
                cleaned_items,
                items_schema
            )

            if items_arrow and items_arrow.num_rows > 0:
                items_table.append(items_arrow)
                total_items += items_arrow.num_rows

    return {
        "success": True,
        "headers_inserted": total_headers,
        "items_inserted": total_items,
        "time_taken_sec": round(time.time() - start_time, 2)
    }

if __name__ == "__main__":
    ingest_bill_from_r2()

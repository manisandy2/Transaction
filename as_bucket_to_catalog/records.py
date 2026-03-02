from as_bucket_to_catalog.utility import process_files_async
import asyncio
from core.catalog_client import get_catalog_client


def run_async_ingestion():

    namespace = "POS_Transactions"
    table_name = "Transaction_vars"

    catalog = get_catalog_client()

    # 🔥 Load Iceberg table properly
    table = catalog.load_table(f"{namespace}.{table_name}")

    # 🔥 Convert Iceberg schema → Arrow schema
    arrow_schema = table.schema().as_arrow()

    asyncio.run(
        process_files_async(
            bucket="pos-transaction-imei",
            prefix="history/2026/03/01/",
            table=table,
            arrow_schema=arrow_schema
        )
    )
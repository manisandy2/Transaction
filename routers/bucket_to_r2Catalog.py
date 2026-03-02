from core.catalog_client import get_catalog_client

# def get_last_pri_id(namespace, table_name):
#     catalog = get_catalog_client()
#     table = catalog.load_table(f"{namespace}.{table_name}")
#     # print("table",table)
#     try:
#         result = (
#             table.scan(selected_fields=["high_watermark"])
#                  .to_arrow()["high_watermark"]
#                  .to_pylist()
#         )
#         # print(len(result))
#         # print(max(result))
#         print("data:",result)
#
#         # print("increment",max(result)+1)
#         return int(result)
#     except Exception :
#         return 0


# get_last_pri_id("POS_Transactions", "Transaction_vars")


# def get_last_value(namespace: str, table_name: str) -> int:
#     """
#     Get latest high_watermark for a table from ingestion_tracking.
#     Returns 0 if not found.
#     """
#
#     catalog = get_catalog_client()
#     identifier = f"{namespace}.ingestion_tracking"
#     print(f"get_last_value: {identifier}")
#
#     try:
#         tracking_table = catalog.load_table(identifier)
#         print(f"tracking_table: {tracking_table}")
#     except Exception:
#         return 0
#
#     try:
#         # Filter only this table
#         scan = tracking_table.scan(
#             row_filter=f"table_name = '{table_name}'",
#             selected_fields=["high_watermark"]
#         ).to_arrow()
#         print(f"scan: {scan['high_watermark']}")
#         if scan.num_rows == 0:
#             return 0
#
#         values = scan["high_watermark"].to_pylist()
#         print("values:",values)
#         # Convert to int and return max
#         return values
#
#     except Exception:
#         return 0

def get_last_value(namespace: str, table_name: str) -> int:
    """
    Get latest high_watermark for a table from ingestion_tracking.
    Returns 0 if not found.
    """

    try:
        catalog = get_catalog_client()
        identifier = f"{namespace}.ingestion_tracking"

        tracking_table = catalog.load_table(identifier)

        result = (
            tracking_table.scan(
                row_filter=f"table_name = '{table_name}'",
                selected_fields=["high_watermark"]
            )
            .to_arrow()
        )
        print(f"result: {result}")
        if result.num_rows == 0:
            return 0

        values = result["high_watermark"].to_pylist()

        # Filter None safely
        values = [int(v) for v in values if v is not None]

        if not values:
            return 0

        return max(values)

    except Exception as e:
        print("get_last_value error:", e)
        return 0
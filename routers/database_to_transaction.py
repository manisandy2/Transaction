#
# from fastapi import APIRouter,HTTPException,Query,Body
# import time
#
# # from get_schema import field
# from ...mysql_creds import *
# from ...mapping import *
# from ...core.catalog_client import get_catalog_client
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
# from datetime import datetime
# from pyiceberg.table import Table
# import pandas as pd
# from pyiceberg.partitioning import PartitionSpec, PartitionField
# from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform,BucketTransform
# from pyiceberg.exceptions import BadRequestError
# from pyiceberg.expressions import EqualTo,And,GreaterThanOrEqual,LessThanOrEqual,In
#
#
# router = APIRouter(prefix="", tags=["Database to Transaction"])
#
#
#
# # version 01
# # transaction data invalid literal for int() with base 10: date format error '6/24'
# # row-wise
# @router.post("/create")
# def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()  # Your MySQL wrapper
#
#     namespace, table_name = "pos_transactions01", "transaction01"
#     # dbname = "Transaction_pos"
#     dbname = "Transaction"
#
#     # ---------- Fetch data from MySQL ----------
#     print("Transaction Table name 01")
#     db_fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     # print("MySQL fetch", time.time() - db_fetch_start)
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     def safe_parse_date(value):
#         """Try multiple formats or datetime types for Bill_Date__c"""
#         if isinstance(value, datetime):
#             return value  # already a datetime object
#         if not value:
#             return None
#
#         # Try common string formats
#         for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
#             try:
#                 return datetime.strptime(value[:10], fmt)
#             except Exception as e:
#                 print(f"Failed to parse {fmt} from {value}")
#                 print(e)
#                 continue
#         return None  # fallback if nothing matches
#
#     # ---------- Convert Bill_Date__c to timestamp ----------
#     convert_start = time.time()
#     converted_rows = []
#     for row in rows:
#         bill_date = row.get("Bill_Date__c","")
#         dt = safe_parse_date(bill_date)
#         if dt:
#             row["year"] = dt.year
#             row["month"] = dt.month
#             row["day"] = dt.day
#         else:
#             row["year"] = row["month"] = row["day"] = None  # fallback for invalid date
#
#         converted_rows.append(row)
#
#     # ---------- Infer Iceberg / Arrow schema ----------
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     # print("Schema inference", schema_start)
#     # ---------- Convert records to match Arrow schema ----------
#     arrow_conv_start = time.time()
#     # print("###"*50)
#     # print("infer_schema Arrow conversion", )
#     # converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
#     converted_records = [convert_column(r, arrow_schema) for r in converted_rows]
#
#     # print("Records conversion to Arrow",  time.time() - arrow_conv_start  )
#     # ---------- Create Arrow Table ----------
#     arrow_table_start = time.time()
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#     # print("Arrow table creation", time.time() - arrow_table_start)
#     catalog_start = time.time()
#     # ---------- Iceberg catalog ----------
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#     # print("Iceberg catalog setup", time.time() -catalog_start)
#     # ---------- Create table if not exists with partitions ----------
#     table_start = time.time()
#     try:
#         tbl = catalog.load_table(table_identifier)
#         # print("Table exists. Ready to append data.")
#     except NoSuchTableError:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("Bill_Date__c").field_id,
#                     field_id=2001,
#                     transform=IdentityTransform(),
#                     name="Date"
#                 ),
#                 # PartitionField(
#                 #     source_id=iceberg_schema.find_field("month").field_id,
#                 #     field_id=2002,
#                 #     transform=IdentityTransform(),
#                 #     name="month"
#                 # ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("store_code__c").field_id,
#                     field_id=2003,
#                     transform=BucketTransform(32),
#                     name="store_Code"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
#                     field_id=2003,
#                     transform=BucketTransform(32),
#                     name="customer_mobile"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("customer_mobile__c").field_id,
#                     field_id=2003,
#                     transform=BucketTransform(32),
#                     name="customer_mobile"
#                 )
#             ],
#
#         )
#
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={"write.partition.path-style": "directory"},
#         )
#         print("✅ Iceberg table created successfully")
#
#
#     append_start = time.time()
#     # ---------- Append data ----------
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     # print("Append data to Iceberg", time.time() - append_start)
#     elapsed = time.time() - total_start
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "table_properties": getattr(tbl, "properties", {}),
#     }
# ############################################################################
#
# from pyiceberg.schema import Schema
# from pyiceberg.types import NestedField, StringType, IntegerType, DoubleType, DateType, TimestampType
# from pyiceberg.partitioning import PartitionSpec
# from pyiceberg.catalog import load_catalog
#
#
# # Connect to REST Iceberg catalog (adjust URI and bucket)
#
# # @router.post("/create-table")
# # def create():
# #     catalog = get_catalog_client()
# #
# #     namespace = "pos_transactions01"  # namespace = folder = database
# #     # base_location = "s3://your-r2-bucket/iceberg_catalog/pos_transactions"
# #     # base_location = "https://a7562e46d10beec81b77d112e5000e4d.r2.cloudflarestorage.com"
# #
# #
# #     # -------------------------
# #     # 1️⃣ Transaction Table (Fact)
# #     # -------------------------
# #     transaction_schema = Schema(
# #         NestedField(1, "Bill_No__c", StringType()),
# #         NestedField(2, "Bill_Date__c", DateType()),
# #         NestedField(3, "store_code__c", StringType()),
# #         NestedField(4, "customer_mobile__c", StringType()),
# #         NestedField(5, "Item_Code__c", StringType()),
# #         NestedField(6, "Invoice_Amount__c", DoubleType()),
# #         NestedField(7, "bill_tax__c", DoubleType()),
# #         NestedField(8, "bill_grand_total__c", DoubleType()),
# #         NestedField(9, "CreatedDate", TimestampType()),
# #     )
# #
# #     transaction_partition_spec = PartitionSpec(
# #         PartitionField(
# #             source_id=transaction_schema.find_field("Bill_Date__c").field_id,
# #             field_id=2001,
# #             transform=YearTransform(),
# #             name="year"
# #         ),
# #         PartitionField(
# #             source_id=transaction_schema.find_field("Bill_Date__c").field_id,
# #             field_id=2002,
# #             transform=MonthTransform(),
# #             name="month"
# #         ),
# #         PartitionField(
# #             source_id=transaction_schema.find_field("store_code__c").field_id,
# #             field_id=2003,
# #             transform=BucketTransform(32),
# #             name="store_bucket"
# #         ),
# #         PartitionField(
# #             source_id=transaction_schema.find_field("customer_mobile__c").field_id,
# #             field_id=2004,
# #             transform=IdentityTransform(),
# #             name="customer_mobile"
# #         )
# #     )
# #     # builder_for(transaction_schema)
# #     #     .year("Bill_Date__c")
# #     #     .month("Bill_Date__c")
# #     #     .identity("store_code__c")
# #     #     .bucket("customer_mobile__c", 32)
# #     #     .build()
# #
# #     table_identifier = f"{"pos_transactions01"}.{"transaction01"}"
# #
# #     catalog.create_table(
# #         identifier=table_identifier,
# #         schema=transaction_schema,
# #         partition_spec=transaction_partition_spec,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: transaction table")
# #
# #
# #     # -------------------------
# #     # 2️⃣ Customers Table (Dimension)
# #     # -------------------------
# #     customers_schema = Schema(
# #         NestedField(1, "customer_mobile__c", StringType(), required=True),
# #         NestedField(2, "customer_name__c", StringType()),
# #         NestedField(3, "customer_city__c", StringType()),
# #         NestedField(4, "customer_type__c", StringType()),
# #         NestedField(5, "CreatedDate", TimestampType()),
# #     )
# #
# #     customers_partition = (
# #         PartitionSpec.builder_for(customers_schema)
# #         .identity("customer_city__c")
# #         .build()
# #     )
# #
# #     catalog.create_table(
# #         identifier=(namespace, "customers"),
# #         schema=customers_schema,
# #         partition_spec=customers_partition,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: customers table")
# #
# #
# #     # -------------------------
# #     # 3️⃣ Items Table (Dimension)
# #     # -------------------------
# #     items_schema = Schema(
# #         NestedField(1, "Item_Code__c", StringType(), required=True),
# #         NestedField(2, "Item_Name__c", StringType()),
# #         NestedField(3, "Item_Group_Name__c", StringType()),
# #         NestedField(4, "Item_Brand_Name__c", StringType()),
# #         NestedField(5, "CreatedDate", TimestampType()),
# #     )
# #
# #     items_partition = (
# #         PartitionSpec.builder_for(items_schema)
# #         .identity("Item_Group_Name__c")
# #         .identity("Item_Brand_Name__c")
# #         .build()
# #     )
# #
# #     catalog.create_table(
# #         identifier=(namespace, "items"),
# #         schema=items_schema,
# #         partition_spec=items_partition,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: items table")
# #
# #
# #     # -------------------------
# #     # 4️⃣ Complaints Table
# #     # -------------------------
# #     complaints_schema = Schema(
# #         NestedField(1, "complaint_id", StringType(), required=True),
# #         NestedField(2, "customer_mobile__c", StringType()),
# #         NestedField(3, "complaint_type", StringType()),
# #         NestedField(4, "complaint_status", StringType()),
# #         NestedField(5, "CreatedDate", TimestampType()),
# #     )
# #
# #     complaints_partition = (
# #         PartitionSpec.builder_for(complaints_schema)
# #         .identity("complaint_status")
# #         .build()
# #     )
# #
# #     catalog.create_table(
# #         identifier=(namespace, "complaints"),
# #         schema=complaints_schema,
# #         partition_spec=complaints_partition,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: complaints table")
# #
# #
# #     # -------------------------
# #     # 5️⃣ Returns Table
# #     # -------------------------
# #     returns_schema = Schema(
# #         NestedField(1, "Return_No__c", StringType(), required=True),
# #         NestedField(2, "Bill_No__c", StringType()),
# #         NestedField(3, "Return_Date__c", DateType()),
# #         NestedField(4, "store_code__c", StringType()),
# #         NestedField(5, "Return_Amount__c", DoubleType()),
# #         NestedField(6, "CreatedDate", TimestampType()),
# #     )
# #
# #     returns_partition = (
# #         PartitionSpec.builder_for(returns_schema)
# #         .year("Return_Date__c")
# #         .month("Return_Date__c")
# #         .identity("store_code__c")
# #         .build()
# #     )
# #
# #     catalog.create_table(
# #         identifier=(namespace, "returns"),
# #         schema=returns_schema,
# #         partition_spec=returns_partition,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: returns table")
# #
# #
# #     # -------------------------
# #     # 6️⃣ Online Orders Table
# #     # -------------------------
# #     online_orders_schema = Schema(
# #         NestedField(1, "order_id", StringType(), required=True),
# #         NestedField(2, "Bill_No__c", StringType()),
# #         NestedField(3, "Bill_Date__c", DateType()),
# #         NestedField(4, "customer_mobile__c", StringType()),
# #         NestedField(5, "order_status", StringType()),
# #         NestedField(6, "order_amount", DoubleType()),
# #     )
# #
# #     online_orders_partition = (
# #         PartitionSpec.builder_for(online_orders_schema)
# #         .year("Bill_Date__c")
# #         .month("Bill_Date__c")
# #         .identity("order_status")
# #         .build()
# #     )
# #
# #     catalog.create_table(
# #         identifier=(namespace, "online_orders"),
# #         schema=online_orders_schema,
# #         partition_spec=online_orders_partition,
# #         properties={"write.partition.path-style": "directory"},
# #     )
# #     print("✅ Created: online_orders table")
# #
# #     print("\n🎯 All Iceberg tables created successfully under namespace 'pos_transactions'")
# #########################################################################
# # from pyiceberg.exceptions import AlreadyExistsError
#
# # @router.post("/create-business-tables")
# # def create_business_tables():
# #     """
# #     Create all core Iceberg tables for business data under the given namespace.
# #     Includes:
# #     - transaction (fact)
# #     - customers
# #     - items
# #     - complaints
# #     - returns
# #     - online_orders
# #     """
# #     try:
# #         catalog = get_catalog_client()
# #         namespace = "pos_transactions01"
# #
# #         # -----------------------------------------------------------
# #         # 1️⃣ Transaction Table (Fact)
# #         # -----------------------------------------------------------
# #         transaction_schema = Schema(
# #             NestedField(1, "Bill_No__c", StringType()),
# #             NestedField(2, "Bill_Date__c", DateType()),
# #             NestedField(3, "store_code__c", StringType()),
# #             NestedField(4, "customer_mobile__c", StringType()),
# #             NestedField(5, "Item_Code__c", StringType()),
# #             NestedField(6, "Invoice_Amount__c", DoubleType()),
# #             NestedField(7, "bill_tax__c", DoubleType()),
# #             NestedField(8, "bill_grand_total__c", DoubleType()),
# #             NestedField(9, "CreatedDate", TimestampType()),
# #         )
# #
# #         # transaction_partition = PartitionSpec(
# #         #     transaction_schema.find_field("Bill_Date__c").field_id, YearTransform(), name="year",
# #         #     transaction_schema.find_field("Bill_Date__c").field_id, MonthTransform(), name="month",
# #         #     transaction_schema.find_field("store_code__c").field_id, BucketTransform(32), name="store_bucket",
# #         #     transaction_schema.find_field("customer_mobile__c").field_id, IdentityTransform(), name="customer_mobile",
# #         # )
# #         # transaction_partition = (
# #         #     PartitionSpec.builder_for(transaction_schema)
# #         #     .year("Bill_Date__c")
# #         #     .month("Bill_Date__c")
# #         #     .bucket("store_code__c", 32)
# #         #     .identity("customer_mobile__c")
# #         #     .build()
# #         # )
# #         transaction_partition = PartitionSpec(
# #             PartitionField(
# #                 source_id=transaction_schema.find_field("Bill_Date__c").field_id,
# #                 field_id=2001,
# #                 transform=YearTransform(),
# #                 name="year",
# #             ),
# #             # PartitionField(
# #             #     source_id=transaction_schema.find_field("Bill_Date__c").field_id,
# #             #     field_id=2002,
# #             #     transform=MonthTransform(),
# #             #     name="month",
# #             # ),
# #             PartitionField(
# #                 source_id=transaction_schema.find_field("store_code__c").field_id,
# #                 field_id=2003,
# #                 transform=BucketTransform(32),
# #                 name="store_bucket",
# #             ),
# #             PartitionField(
# #                 source_id=transaction_schema.find_field("customer_mobile__c").field_id,
# #                 field_id=2004,
# #                 transform=IdentityTransform(),
# #                 name="customer_mobile",
# #             ),
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "transaction"),
# #             schema=transaction_schema,
# #             partition_spec=transaction_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: transaction table")
# #
# #         # -----------------------------------------------------------
# #         # 2️⃣ Customers Table (Dimension)
# #         # -----------------------------------------------------------
# #         customers_schema = Schema(
# #             NestedField(1, "customer_mobile__c", StringType(), required=True),
# #             NestedField(2, "customer_name__c", StringType()),
# #             NestedField(3, "customer_city__c", StringType()),
# #             NestedField(4, "customer_type__c", StringType()),
# #             NestedField(5, "CreatedDate", TimestampType()),
# #         )
# #
# #         # customers_partition = PartitionSpec.builder_for(customers_schema) \
# #         #     .identity("customer_city__c") \
# #         #     .build()
# #         customers_partition = PartitionSpec(
# #             PartitionField(
# #                 source_id=customers_schema.find_field("customer_city__c").field_id,
# #                 field_id=2101,
# #                 transform=IdentityTransform(),
# #                 name="customer_city__c"
# #             )
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "customers"),
# #             schema=customers_schema,
# #             partition_spec=customers_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: customers table")
# #
# #         # -----------------------------------------------------------
# #         # 3️⃣ Items Table (Dimension)
# #         # -----------------------------------------------------------
# #         items_schema = Schema(
# #             NestedField(1, "Item_Code__c", StringType(), required=True),
# #             NestedField(2, "Item_Name__c", StringType()),
# #             NestedField(3, "Item_Group_Name__c", StringType()),
# #             NestedField(4, "Item_Brand_Name__c", StringType()),
# #             NestedField(5, "CreatedDate", TimestampType()),
# #         )
# #
# #         # items_partition = PartitionSpec.builder_for(items_schema) \
# #         #     .identity("Item_Group_Name__c") \
# #         #     .identity("Item_Brand_Name__c") \
# #         #     .build()
# #         items_partition = PartitionSpec(
# #             PartitionField(
# #                 source_id=items_schema.find_field("Item_Group_Name__c").field_id,
# #                 field_id=2201,
# #                 transform=IdentityTransform(),
# #                 name="Item_Group_Name__c"
# #             ),
# #             PartitionField(
# #                 source_id=items_schema.find_field("Item_Brand_Name__c").field_id,
# #                 field_id=2202,
# #                 transform=IdentityTransform(),
# #                 name="Item_Brand_Name__c"
# #             )
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "items"),
# #             schema=items_schema,
# #             partition_spec=items_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: items table")
# #
# #         # -----------------------------------------------------------
# #         # 4️⃣ Complaints Table
# #         # -----------------------------------------------------------
# #         complaints_schema = Schema(
# #             NestedField(1, "complaint_id", StringType(), required=True),
# #             NestedField(2, "customer_mobile__c", StringType()),
# #             NestedField(3, "complaint_type", StringType()),
# #             NestedField(4, "complaint_status", StringType()),
# #             NestedField(5, "CreatedDate", TimestampType()),
# #         )
# #
# #         # complaints_partition = PartitionSpec.builder_for(complaints_schema) \
# #         #     .identity("complaint_status") \
# #         #     .build()
# #         complaints_partition = PartitionSpec(
# #             PartitionField(
# #                 source_id=complaints_schema.find_field("complaint_status").field_id,
# #                 field_id=3001,
# #                 transform=IdentityTransform(),
# #                 name="complaint_status"
# #             )
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "complaints"),
# #             schema=complaints_schema,
# #             partition_spec=complaints_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: complaints table")
# #
# #         # -----------------------------------------------------------
# #         # 5️⃣ Returns Table
# #         # -----------------------------------------------------------
# #         returns_schema = Schema(
# #             NestedField(1, "Return_No__c", StringType(), required=True),
# #             # NestedField(2, "Bill_No__c", StringType()),
# #             NestedField(3, "Return_Date__c", DateType()),
# #             NestedField(4, "store_code__c", StringType()),
# #             NestedField(5, "Return_Amount__c", DoubleType()),
# #             NestedField(6, "CreatedDate", TimestampType()),
# #         )
# #
# #         returns_partition = PartitionSpec(
# #             # PartitionField(
# #             #     source_id=returns_schema.find_field("Return_Date__c").field_id,
# #             #     field_id=3101,
# #             #     transform=YearTransform(),
# #             #     name="year"
# #             # ),
# #             PartitionField(
# #                 source_id=returns_schema.find_field("Return_Date__c").field_id,
# #                 field_id=3102,
# #                 transform=MonthTransform(),
# #                 name="month"
# #             ),
# #             PartitionField(
# #                 source_id=returns_schema.find_field("store_code__c").field_id,
# #                 field_id=3103,
# #                 transform=IdentityTransform(),
# #                 name="store_code"
# #             )
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "returns"),
# #             schema=returns_schema,
# #             partition_spec=returns_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: returns table")
# #
# #         # -----------------------------------------------------------
# #         # 6️⃣ Online Orders Table
# #         # -----------------------------------------------------------
# #         online_orders_schema = Schema(
# #             NestedField(1, "order_id", StringType(), required=True),
# #             NestedField(2, "Bill_No__c", StringType()),
# #             NestedField(3, "Bill_Date__c", DateType()),
# #             NestedField(4, "customer_mobile__c", StringType()),
# #             NestedField(5, "order_status", StringType()),
# #             NestedField(6, "order_amount", DoubleType()),
# #         )
# #
# #         online_orders_partition = PartitionSpec(
# #             # PartitionField(
# #             #     source_id=online_orders_schema.find_field("Bill_Date__c").field_id,
# #             #     field_id=3201,
# #             #     transform=YearTransform(),
# #             #     name="year"
# #             # ),
# #             PartitionField(
# #                 source_id=online_orders_schema.find_field("Bill_Date__c").field_id,
# #                 field_id=3202,
# #                 transform=MonthTransform(),
# #                 name="month"
# #             ),
# #             PartitionField(
# #                 source_id=online_orders_schema.find_field("order_status").field_id,
# #                 field_id=3203,
# #                 transform=IdentityTransform(),
# #                 name="order_status"
# #             )
# #         )
# #
# #         catalog.create_table(
# #             identifier=(namespace, "online_orders"),
# #             schema=online_orders_schema,
# #             partition_spec=online_orders_partition,
# #             properties={"write.partition.path-style": "directory"},
# #         )
# #         print("✅ Created: online_orders table")
# #
# #         return {
# #             "status": "success",
# #             "namespace": namespace,
# #             "message": "All business tables created successfully",
# #             "tables": [
# #                 "transaction", "customers", "items",
# #                 "complaints", "returns", "online_orders"
# #             ]
# #         }
# #
# #     except NoSuchTableError as e:
# #         raise HTTPException(status_code=409, detail=f"Table already exists: {str(e)}")
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Error creating Iceberg tables: {str(e)}")
#
#
# ##########################################################################
# @router.post("/update")
# def update_transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()  # Your MySQL wrapper
#
#     namespace, table_name = "pos_transactions", "transaction"
#     # dbname = "Transaction_pos"
#     dbname = "Transaction"
#
#     # ---------- Fetch data from MySQL ----------
#
#     db_fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     print("MySQL fetch", time.time() - db_fetch_start)
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     def safe_parse_date(value):
#         """Try multiple formats or datetime types for Bill_Date__c"""
#         if isinstance(value, datetime):
#             return value  # already a datetime object
#         if not value:
#             return None
#         for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
#             try:
#                 return datetime.strptime(value[:10], fmt)
#             except Exception:
#                 continue
#         return None  # fallback if nothing matches
#
#     # ---------- Convert Bill_Date__c to timestamp ----------
#     convert_start = time.time()
#     converted_rows = []
#     for row in rows:
#         bill_date = row.get("Bill_Date__c","")
#         dt = safe_parse_date(bill_date)
#         if dt:
#             row["year"] = dt.year
#             row["month"] = dt.month
#             row["day"] = dt.day
#         else:
#             row["year"] = row["month"] = row["day"] = None  # fallback for invalid date
#         converted_rows.append(row)
#
#
#
#     # print("rows", converted_rows)
#     print("Bill_Date__c conversion",  time.time() - convert_start)
#     schema_start = time.time()
#     # ---------- Infer Iceberg / Arrow schema ----------
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     print("Schema inference", schema_start)
#     # ---------- Convert records to match Arrow schema ----------
#     arrow_conv_start = time.time()
#     # converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
#     converted_records = [convert_column(r, arrow_schema) for r in converted_rows]
#
#     print("Records conversion to Arrow",  time.time() - arrow_conv_start  )
#     # ---------- Create Arrow Table ----------
#     arrow_table_start = time.time()
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#     print("Arrow table creation", time.time() - arrow_table_start)
#     catalog_start = time.time()
#     # ---------- Iceberg catalog ----------
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#     print("Iceberg catalog setup", time.time() -catalog_start)
#     # ---------- Create table if not exists with partitions ----------
#     table_start = time.time()
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("Table exists. Ready to append data.")
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Iceberg table '{table_identifier}' not found.")
#
#
#
#     append_start = time.time()
#     # ---------- Append data ----------
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     print("Append data to Iceberg", time.time() - append_start)
#     elapsed = time.time() - total_start
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "table_properties": getattr(tbl, "properties", {}),
#     }
#
#
# @router.get("/filter")
# def dynamic_filter(
#     namespace: str = Query(..., description="Namespace (e.g. 'crm')"),
#     table_name: str = Query(..., description="Table name (e.g. 'serial_number_requests')"),
#     columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
#     values: list[str] = Query(..., description="Values to match (in same order as columns)"),
#
# ):
#
#     start_time = time.time()
#
#     # Handle comma-separated inputs
#     if len(columns) == 1 and ',' in columns[0]:
#         columns = [c.strip() for c in columns[0].split(',')]
#     if len(values) == 1 and ',' in values[0] and len(columns) > 1:
#         values = [v.strip() for v in values[0].split(',')]
#
#     try:
#         # Check lengths
#         if len(columns) != len(values):
#             raise HTTPException(
#                 status_code=400,
#                 detail={
#                     "status": "error",
#                     "error_code": "MISMATCHED_COLUMNS_VALUES",
#                     "message": "Number of columns and values must match",
#                     "data": [],
#                     "status_code": 400
#                 }
#             )
#
#
#         try:
#             catalog = get_catalog_client()
#         except Exception as e:
#             raise HTTPException(
#                 status_code=500,
#                 detail={
#                     "status": "error",
#                     "error_code": "CATALOG_CONNECTION_FAILED",
#                     "message": f"Failed to connect to catalog: {str(e)}",
#                     "data": [],
#                     "status_code": 500
#                 }
#             )
#         try:
#             table = catalog.load_table((namespace, table_name))
#         except Exception as e:
#             raise HTTPException(
#                 status_code=404,
#                 detail={
#                     "status": "error",
#                     "error_code": "TABLE_NOT_FOUND",
#                     "message": f"Table '{namespace}.{table_name}' not found or could not be loaded: {str(e)}",
#                     "data": [],
#                     "status_code": 404
#                 }
#             )
#
#         # Validate columns
#         schema_fields = {f.name for f in table.schema().fields}
#         for col in columns:
#             if col not in schema_fields:
#                 raise HTTPException(
#                     status_code=404,
#                     detail={
#                         "status": "error",
#                         "error_code": "COLUMN_NOT_FOUND",
#                         "message": f"Column '{col}' not found in schema",
#                         "data": [],
#                         "status_code": 404
#                     }
#                 )
#         filters = None
#         for col, val in zip(columns, values):
#             if col == "Bill_Date__c" and ',' in val:
#                 start_date, end_date = [v.strip() for v in val.split(',')]
#                 condition = And(
#                     GreaterThanOrEqual(col, start_date),
#                     LessThanOrEqual(col, end_date)
#                 )
#             else:
#                 condition = EqualTo(col, val)
#             filters = condition if filters is None else And(filters, condition)
#
#         # # Apply filter
#         scan = table.scan(row_filter=filters)
#         arrow_table = scan.to_arrow()
#
#         rows = []
#         for batch in arrow_table.to_batches():
#             rows.extend(batch.to_pylist())
#
#         elapsed = round(time.time() - start_time, 2)
#
#         if len(rows) == 0:
#             raise HTTPException(
#                 status_code=404,
#                 detail={
#                     "status": "error",
#                     "error_code": "NO_DATA",
#                     "message": "No data found",
#                     "data": [],
#                     "status_code": 404
#                 }
#             )
#
#         return {
#             "status": "success",
#             "status_code": 200,
#             "count": len(scan.to_arrow()),
#             "data": scan.to_arrow().to_pylist(),
#             "execution_time_seconds": elapsed
#         }
#
#     except HTTPException as http_err:
#         # Re-raise known HTTP errors
#         raise http_err
#
#     except Exception as e:
#         # Catch any unknown errors
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "error_code": "INTERNAL_ERROR",
#                 "message": str(e),
#                 "data": [],
#                 "status_code": 500
#             }
#         )
#
#
# # @router.get("/filter-business")
# # def filter_business_logic(
# #     namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
# #     table_name: str = Query(..., description="Table name (e.g. 'pos')"),
# #     store_codes: list[str] = Query(..., description="List of store codes"),
# #     start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
# #     end_date: str = Query(..., description="End date (YYYY-MM-DD)")
# # ):
# #     start_time = time.time()
# #
# #     try:
# #         # Step 1: Load Iceberg table
# #         catalog = get_catalog_client()
# #         table = catalog.load_table((namespace, table_name))
# #
# #         # Step 2: Build filter
# #         filters = And(
# #             In("store_code__c", store_codes),
# #             GreaterThanOrEqual("Bill_Date__c", start_date),
# #             LessThanOrEqual("Bill_Date__c", end_date)
# #         )
# #         print(filters)
# #         # Step 3: Scan and load data
# #         scan = table.scan(row_filter=filters)
# #         arrow_table = scan.to_arrow()
# #         df = arrow_table.to_pandas()
# #         print(df)
# #         if df.empty:
# #             raise HTTPException(
# #                 status_code=404,
# #                 detail={
# #                     "status": "error",
# #                     "error_code": "NO_DATA",
# #                     "message": "No records found for the given filters.",
# #                     "data": [],
# #                     "status_code": 404
# #                 }
# #             )
# #         # Step 3.5: Convert numeric columns from string → float
# #         numeric_columns = ["item_gross_amount__c", "item_tax__c"]
# #         for col in numeric_columns:
# #             if col in df.columns:
# #                 df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
# #
# #         # Step 4: Business logic aggregation
# #         agg_df = df.groupby("store_code__c").agg(
# #             total_transactions=("store_code__c", "count"),
# #             Mobile_count=("customer_mobile__c", "nunique"),
# #             Invoice_count=("bill_transaction_no__c", "nunique"),
# #             Invoice_gross_amount=("item_gross_amount__c", "sum"),
# #             Invoice_tax_amount=("item_tax__c", "sum"),
# #         ).reset_index()
# #
# #         agg_df["Invoice_total_with_tax"] = (
# #             agg_df["Invoice_gross_amount"] + agg_df["Invoice_tax_amount"]
# #         )
# #
# #         elapsed = round(time.time() - start_time, 2)
# #
# #         # Step 5: Return JSON
# #         return {
# #             "status": "success",
# #             "status_code": 200,
# #             "execution_time_seconds": elapsed,
# #             "count": len(agg_df),
# #             "data": agg_df.to_dict(orient="records")
# #         }
# #
# #     except HTTPException as http_err:
# #         raise http_err
# #     except Exception as e:
# #         raise HTTPException(
# #             status_code=500,
# #             detail={
# #                 "status": "error",
# #                 "error_code": "INTERNAL_ERROR",
# #                 "message": str(e),
# #                 "data": [],
# #                 "status_code": 500
# #             }
# #         )
# #
#
# @router.get("/filter-business")
# def filter_business_logic(
#     # namespace: str = Query(...),
#     # table_name: str = Query(...),
#     store_codes: list[str] = Query(...),
#     # billed_at_branch_name: list[str] = Query(...),
#     start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
#     end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
# ):
#     import time
#     import pandas as pd
#     from fastapi import HTTPException, Query
#     from pyiceberg.expressions import And, In, GreaterThanOrEqual, LessThanOrEqual
#
#     start_time = time.time()
#
#     namespace = "pos_transactions01"
#     table_name = "transaction01"
#
#     try:
#         # Step 1: Load table
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         # Step 2: Basic filter
#         filters = And(
#             In("store_code__c", store_codes),
#             # In("billed_at_branch_name", billed_at_branch_name),
#             GreaterThanOrEqual("Bill_Date__c", start_date),
#             LessThanOrEqual("Bill_Date__c", end_date),
#         )
#
#         # Step 3: Fetch data
#         df = table.scan(row_filter=filters).to_arrow().to_pandas()
#         filter_value = df.to_dict(orient="records")
#         # print(df)
#         print(filter_value)
#
#         if df.empty:
#             raise HTTPException(status_code=404, detail="No data found")
#
#         numeric_columns = ["item_gross_amount__c", "item_tax__c"]
#         print(numeric_columns)
#         for col in numeric_columns:
#             if col in df.columns:
#                 df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
#
#
#         # Step 4: Clean & convert numeric
#         df["item_gross_amount__c"] = pd.to_numeric(df["item_gross_amount__c"], errors="coerce").fillna(0.0)
#         df["item_tax__c"] = pd.to_numeric(df["item_tax__c"], errors="coerce").fillna(0.0)
#
#         # Step 5: Apply WHERE filters (Python equivalent of NOT LIKE)
#         # exclude_transaction_types = ["%Return%", "%Cancelled%"]  # example patterns
#         # exclude_product_names = [
#         #     "%Gift%", "%Coupon%", "%Voucher%", "%Discount%", "%Promo%", "%Free%"
#         # ]
#         #
#         # # Apply string filters (case-insensitive contains)
#         # for pattern in exclude_transaction_types:
#         #     key = pattern.replace("%", "")
#         #     df = df[~df["bill_transaction_type__c"].str.contains(key, case=False, na=False)]
#         #
#         # for pattern in exclude_product_names:
#         #     key = pattern.replace("%", "")
#         #     df = df[~df["item_product_name"].str.contains(key, case=False, na=False)]
#
#         # Step 6: Aggregate
#         # agg_df = df.groupby(["store_code__c", "billed_at_branch_name"]).agg(
#         agg_df = df.groupby("store_code__c").agg(
#             total_transactions=("store_code__c", "count"),
#             mobile_count=("customer_mobile__c", "nunique"),
#             invoice_count=("bill_transaction_no__c", "nunique"),
#             invoice_gross_amount=("item_gross_amount__c", "sum"),
#             invoice_tax_amount=("item_tax__c", "sum"),
#         ).reset_index()
#
#         print("agg_df",agg_df)
#
#         agg_df["invoice_total_with_tax"] = agg_df["invoice_gross_amount"] + agg_df["invoice_tax_amount"]
#
#         elapsed = round(time.time() - start_time, 2)
#         return {
#             "status": "success",
#             "status_code": 200,
#             "execution_time_seconds": elapsed,
#             "count": len(df),
#             "data": agg_df.to_dict(orient="records"),
#             "data_list":filter_value
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
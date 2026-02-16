# import pyarrow.parquet as pq
#
# def read_parquet_file(file_path: str):
#     """
#     Read a single parquet file and return Arrow Table.
#     """
#     try:
#         table = pq.read_table(file_path)
#         print(table)
#         return table
#     except Exception as e:
#         print(f"Failed to read parquet: {e}")
#         return None
#
# tables = read_parquet_file("downloads/parquet/00000-0-1879bcbb-661c-4261-b858-374a94a71f59.parquet")
# print(tables.schema)
# print(tables.num_rows)

import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.path.join(BASE_DIR, "downloads/parquet")


file_path = os.path.join(PARQUET_DIR, "00000-0-1879bcbb-661c-4261-b858-374a94a71f59.parquet")

print("Reading from:", file_path)

df = pd.read_parquet(file_path)
for col,values in df.iterrows():
    print(col,values)

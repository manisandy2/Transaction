import pandas as pd

data = pd.read_parquet(r"routers/transaction_model02/02.parquet")
print(data.columns)


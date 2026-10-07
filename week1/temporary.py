import pandas as pd

raw = pd.read_csv("data/orders_raw.csv")
kept = raw.drop_duplicates(subset=["order_id"])

print((kept.index + 1).tolist()) 

from pathlib import Path
import duckdb

sql = Path("week1/queries.sql").read_text()
sql_positions = duckdb.sql(sql).df()["source_row"].tolist()
pandas_positions = (kept.index + 1).tolist()

print("Same rows kept:", sql_positions == pandas_positions)
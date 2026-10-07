from pathlib import Path
import duckdb

query = Path("week1/queries.sql").read_text()
duckdb.sql(query).show()

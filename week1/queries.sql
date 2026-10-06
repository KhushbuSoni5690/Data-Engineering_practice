import duckdb 


duckdb.sql("SELECT * FROM 'data/orders_clean.parquet' LIMIT 5").show()
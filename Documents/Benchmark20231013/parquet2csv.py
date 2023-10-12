import duckdb

# Initialize DuckDB
conn = duckdb.connect()

# Read the Parquet file
df = conn.from_parquet('inbox/1M_fact.parquet')

# Write the DataFrame to a CSV file
df.to_csv('outbox/Convert_1M_fact.csv')


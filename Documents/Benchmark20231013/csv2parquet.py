import duckdb

# Create a connection to DuckDB
conn = duckdb.connect()

# Read the CSV file into a DuckDB table
conn.execute("CREATE TABLE fact AS SELECT * FROM read_csv_auto('Inbox/1M_fact.csv')")

# Write the table to a Parquet file
conn.execute("COPY fact TO 'Inbox/1M_fact.parquet' (FORMAT 'PARQUET')")

print("CSV file has been successfully converted to Parquet.")

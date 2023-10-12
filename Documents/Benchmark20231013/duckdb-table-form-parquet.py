import duckdb
import sys
import time

start_time = time.time()

# Create a persistent database on disk
conn = duckdb.connect()

# Set memory limit to 10GB
conn.execute("SET memory_limit='10GB'")

# Execute SQL queries using duckdb.sql()
conn.execute("CREATE TABLE master AS SELECT * FROM 'Inbox/Master.parquet'")
conn.execute("CREATE VIEW filter_master AS SELECT * FROM master WHERE Style >= 'B' AND Style <= 'F'")

# Use Python's f-string for string formatting
conn.execute(f"CREATE TABLE fact_table AS SELECT * FROM 'Inbox/{sys.argv[1]}'")

conn.execute("""
    CREATE VIEW detail_result AS
    SELECT fact_table.*, Quantity * Unit_Price AS Amount
    FROM fact_table
    JOIN filter_master ON fact_table.Product = filter_master.Product AND fact_table.Style = filter_master.Style
    WHERE Shop >= 'S21' AND Shop <= 'S99' AND Amount > 1000
""")

conn.execute("""
    CREATE VIEW summary_result AS
    SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount
    FROM detail_result
    GROUP BY Shop, Product
""")

# Save the detail_result and summary_result to parquet files
detail_filename = f"Outbox/DuckDB_TableForm_Detail_Result_{sys.argv[1].replace('.parquet', '')}.parquet"
summary_filename = f"Outbox/DuckDB_TableForm_Summary_Result_{sys.argv[1].replace('.parquet', '')}.parquet"
conn.execute(f"COPY (SELECT * FROM detail_result) TO '{detail_filename}' (FORMAT PARQUET)")
conn.execute(f"COPY (SELECT * FROM summary_result) TO '{summary_filename}' (FORMAT PARQUET)")

detail_df = conn.execute("SELECT * FROM detail_result").fetch_df()
summary_df = conn.execute("SELECT * FROM summary_result").fetch_df()

print(detail_df.head(10))
print("\n")  
print(summary_df.head(10))
print("\n")  
end_time = time.time()
print("DuckDB Parquet Duration (In Second): {}".format(round(end_time-start_time,3)))
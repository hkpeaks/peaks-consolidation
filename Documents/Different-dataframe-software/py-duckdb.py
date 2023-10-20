import duckdb
import sys
import time

start_time = time.time()

# Create a persistent database on disk
conn = duckdb.connect()

# Set memory limit to 10GB
conn.execute("SET memory_limit='10GB'")

# Execute SQL queries using duckdb.sql()
conn.execute("CREATE TABLE master AS SELECT * FROM read_csv_auto('Inbox/Master.csv')")
conn.execute("CREATE VIEW filter_master AS SELECT * FROM master WHERE Style == 'C' OR Style == 'F'")  # Corrected here

# Use Python's f-string for string formatting
conn.execute(f"CREATE TABLE fact_table AS SELECT * FROM read_csv_auto('Inbox/{sys.argv[1]}')")

conn.execute("""
    CREATE VIEW detail_result AS
    SELECT fact_table.*, Quantity * Unit_Price AS Amount
    FROM fact_table
    JOIN filter_master ON fact_table.Product = filter_master.Product AND fact_table.Style = filter_master.Style
    WHERE Shop >= 'S77' AND Shop <= 'S78' AND Amount > 100000
""")

conn.execute("""
    CREATE VIEW summary_result AS
    SELECT Shop, Product, Style, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount
    FROM detail_result
    GROUP BY Shop, Product, Style
""")

# Save the detail_result and summary_result to parquet files
detail_filename = f"Outbox/PyDuckdb_Detail_Result_{sys.argv[1].replace('.csv', '')}.csv"
summary_filename = f"Outbox/PyDuckdb_Summary_Result_{sys.argv[1].replace('.csv', '')}.csv"
conn.execute(f"COPY (SELECT * FROM detail_result) TO '{detail_filename}' (FORMAT CSV)")
conn.execute(f"COPY (SELECT * FROM summary_result) TO '{summary_filename}' (FORMAT CSV)")

# Fetch the first 10 rows from the detail_result and summary_result views and print them to the CLI in a list of tuples format
detail_result = conn.execute("SELECT * FROM detail_result LIMIT 10").fetchall()
summary_result = conn.execute("SELECT * FROM summary_result LIMIT 10").fetchall()

print("\nDetail Result (First 10 Rows):")
for row in detail_result:
    print(row)

print("\nSummary Result (First 10 Rows):")
for row in summary_result:
    print(row)

end_time = time.time()
print("\nDuckDB CSV Duration (In Second): {}".format(round(end_time-start_time,3)))

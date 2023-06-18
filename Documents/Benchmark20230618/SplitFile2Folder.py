import duckdb

import time

s = time.time()

con = duckdb.connect()

con.execute("Create Table my_table As Select * From read_csv_auto('Input/100-MillionRows.csv');")
con.execute("Copy my_table TO 'Output/DuckDBFolder' (Format CSV, Header True, Partition_By (Ledger));")

e = time.time()

print("DuckDB SplitFile2Folder Time = {}".format(round(e-s,3)))
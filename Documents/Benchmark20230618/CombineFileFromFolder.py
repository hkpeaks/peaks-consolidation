import duckdb

import time

s = time.time()

con = duckdb.connect()

con.execute("copy (select * from read_csv_auto('Output/DuckDBFolder/*/*.csv')) to 'Output/DuckDBCombineFile.csv'  (Header True);")

e = time.time()

print("DuckDB CombineFileFromFolder Time = {}".format(round(e-s,3)))
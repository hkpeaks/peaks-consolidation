import duckdb

import time

s = time.time()

con = duckdb.connect()

con.execute("""copy (SELECT Ledger, Account, DC, Currency, SUM(Base_Amount) as Total_Base_Amount 
FROM read_parquet('input/300-MillionRows.parquet') 
WHERE Ledger>='L30' AND Ledger <='L70' 
GROUP BY Ledger, Account, DC, Currency) 
to 'output/DuckFilterGroupByParquet.csv' (format csv, header true);""")

e = time.time()

print("DuckDB FilterGroupBy Parquet Time = {}".format(round(e-s,3)))

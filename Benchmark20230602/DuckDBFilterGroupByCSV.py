import duckdb

import time

s = time.time()

con = duckdb.connect()

con.execute("""copy (SELECT Ledger, Account, DC, Currency, SUM(Base_Amount) as Total_Base_Amount 
FROM read_csv_auto('input/300-MillionRows.csv') 
WHERE Ledger>='L30' AND Ledger <='L70' 
GROUP BY Ledger, Account, DC, Currency) 
to 'output/DuckFilterGroupByCSV.csv' (format csv, header true);""")

e = time.time()

print("DuckDB FilterGroupBy CSV Time = {}".format(round(e-s,3)))
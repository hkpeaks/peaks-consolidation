import duckdb

import time

s = time.time()

con = duckdb.connect()

con.execute("""copy (SELECT Shop, Date, Product, Style, SUM(Amount) as Total_Amount 
FROM read_csv_auto('input/7-BillionRows.csv') 
WHERE Product>='200' AND Product <='220' 
GROUP BY Shop, Date, Product, Style) 
to 'output/DuckFilterGroupByCSV.csv' (format csv, header true);""")

e = time.time()

print("DuckDB FilterGroupBy CSV Time = {}".format(round(e-s,3)))

import polars as pl
import time
import pathlib
s = time.time()

table1 = pl.scan_csv("Input/300-MillionRows.csv")
table2 = table1.filter((pl.col('Ledger') >= "L30") & (pl.col('Ledger') <= "L70"))
print("Number of selected rows for table2: {}", table2.select(pl.count()).collect());
table3 = table2.collect(streaming=True)
path: pathlib.Path = "Output/PolarsFilter.csv"
table3.write_csv(path)

e = time.time()
print("Polars Filter Data Time = {}".format(round(e-s,3)))







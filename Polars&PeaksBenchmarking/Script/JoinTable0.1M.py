import polars as pl
import time
import pathlib
s = time.time()

transaction = pl.read_csv("Input/0.1MillionRows.CSV")            
master = pl.read_csv("Input/Master.CSV") 
joined_table = transaction.join(master, on=["Ledger","Account","Project"], how="inner")

joined_table.write_csv("Output/Polars-JoinTable0.1M.csv")

e = time.time()
print("Polars JoinTable 0.1M Time = {}".format(e-s))


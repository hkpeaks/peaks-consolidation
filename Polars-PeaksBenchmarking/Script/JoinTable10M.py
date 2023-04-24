import polars as pl
import time
import pathlib
s = time.time()

transaction = pl.read_csv("Input/10MillionRows.CSV")            
master = pl.read_csv("Input/Master.CSV") 
joined_table = transaction.join(master, on=["Ledger","Account","Project"], how="inner")

joined_table.write_csv("Output/Polars-JoinTable10M.csv")

e = time.time()
print("Polars JoinTable 10M Time = {}".format(e-s))


import polars as pl
import time
import pathlib
s = time.time()

q = (
     pl.scan_csv("Input/0.1MillionRows.csv")      
    .select(["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"]).unique()
    )    

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-Distinct0.1M.csv"
a.write_csv(path, separator=",")

e = time.time()
print("Polars Distinct 0.1M Time = {}".format(e-s))


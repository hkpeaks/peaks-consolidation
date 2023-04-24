import polars as pl
import time
import pathlib
s = time.time()

q = (
     pl.scan_csv("Input/1MillionRows.csv")      
    .select(["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"]).unique()
    )    

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-Distinct1M.csv"
a.write_csv(path, separator=",")

e = time.time()
print("Polars Distinct 1M Time = {}".format(e-s))


import polars as pl
import time
import pathlib
s = time.time()

q = (
     pl.scan_csv("Input/100MillionRows.csv")      
    .select(["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"]).unique()
    )    

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-Distinct100M.csv"
a.write_csv(path, separator=",")

e = time.time()
print("Polars Distinct 100M Time = {}".format(e-s))


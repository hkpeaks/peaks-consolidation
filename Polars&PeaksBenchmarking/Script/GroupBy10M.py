import polars as pl
import time
import pathlib
s = time.time()

q = (
     pl.scan_csv("Input/10MillionRows.csv")      
     .groupby(by=["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"])
    .agg([   
        pl.count('Quantity').alias('Quantity(Count)'),
        pl.max('Quantity').alias('Quantity(Max)'),
        pl.min('Quantity').alias('Quantity(Min)'),
        pl.sum('Quantity').alias('Quantity(Sum)'),        
    ])) 

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-GroupBy10M.csv"
a.write_csv(path, separator=",")

e = time.time()
print("Polars GroupBy 10M Time = {}".format(e-s))



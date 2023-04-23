import polars as pl
import time
import pathlib
s = time.time()

q = (
     pl.scan_csv("Input/0.01MillionRows.csv")      
     .groupby(by=["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"])
    .agg([   
        pl.count('Quantity').alias('Quantity(Count)'),
        pl.max('Quantity').alias('Quantity(Max)'),
        pl.min('Quantity').alias('Quantity(Min)'),
        pl.sum('Quantity').alias('Quantity(Sum)'),        
    ])) 

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-GroupBy0.01M.csv"
a.write_csv(path, separator=",")

e = time.time()
print("Polars GroupBy 0.01M Time = {}".format(e-s))



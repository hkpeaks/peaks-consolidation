import polars as pl
import time
import pathlib
s = time.time()

table1 = (
      pl.scan_csv("Input/300-MillionRows.csv")   
     .groupby(by=["Ledger", "Account", "DC","Currency"])
    .agg([    
        pl.sum('Base_Amount').alias('Base_Amount(Sum)'),        
    ])) 

table2 = table1.collect(streaming=True)

path: pathlib.Path = "Output/PolarsGroupBy.csv"
table2.write_csv(path)

e = time.time()
print("Polars Filter Data Time = {}".format(round(e-s,3)))







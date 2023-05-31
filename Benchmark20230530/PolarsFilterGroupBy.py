import polars as pl
import time
import pathlib
s = time.time()

table1 = pl.scan_csv("Input/300-MillionRows.csv")
table2 = table1.filter((pl.col('Ledger') >= "L30") & (pl.col('Ledger') <= "L60"))
print("Number of selected rows for table2: {}", table2.select(pl.count()).collect());
table3 = table2.collect(streaming=True)

table4 = (
     table3      
     .groupby(by=["Ledger", "Account", "DC","Currency"])
    .agg([    
        pl.sum('Base_Amount').alias('Base_Amount(Sum)'),        
    ])) 

path: pathlib.Path = "Output/PolarsFilterGroupBy.csv"
table4.write_csv(path)

e = time.time()
print("Polars Filter Data Time = {}".format(round(e-s,3)))









import polars as pl
import time
import pathlib
s = time.time()

table1 = (
     pl.scan_csv("Input/1-MillionRows.csv")
     .filter((pl.col('Product') >= "200") & (pl.col('Product') <= "220"))      
     .groupby(by=["Shop", "Date", "Product", "Style"])
    .agg([    
        pl.sum('Amount').alias('Total_Amount'),        
    ])) 

path = "Output/PolarsFilterGroupBy.csv"
table1 = table1.lazy().collect(streaming=True)
table1.write_csv(path)

e = time.time()
print("Polars FilterGroupBy Time = {}".format(round(e-s,3)))













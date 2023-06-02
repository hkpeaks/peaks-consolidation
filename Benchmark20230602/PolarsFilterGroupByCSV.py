import polars as pl
import time
import pathlib
s = time.time()

table1 = (
     pl.scan_csv("Input/300-MillionRows.csv")
     .filter((pl.col('Ledger') >= "L30") & (pl.col('Ledger') <= "L70"))      
     .groupby(by=["Ledger", "Account", "DC","Currency"])
    .agg([    
        pl.sum('Base_Amount').alias('Total_Base_Amount'),        
    ])) 

table2 = table1.collect(streaming=True)
path: pathlib.Path = "Output/PolarsFilterGroupByCSV.csv"
table2.write_csv(path)

e = time.time()
print("Polars FilterGroupBy CSV Time = {}".format(round(e-s,3)))











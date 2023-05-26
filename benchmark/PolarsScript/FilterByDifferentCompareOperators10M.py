import polars as pl
import time
import pathlib
s = time.time()

table1 = pl.scan_csv("Input/10MillionRows.csv")

table2 = table1.filter(
        ((pl.col('Ledger') == "L99") | (pl.col('Ledger') < "L20")) & ((pl.col('Project') > "B25") | (pl.col('Project') < "B23")))

print("Number of selected rows for table2: {}", table2.select(pl.count()).collect());

table3 = table2.filter(  
        (pl.col('Currency') != "C06"))

print("Number of selected rows for table3: {}", table3.select(pl.count()).collect());

table4 = table3.filter(  
        (pl.col('Account') <= 11000) | (pl.col('Account') >= 18000))

print("Number of selected rows for table4: {}", table4.select(pl.count()).collect());

table5 = table4.filter(
        ((pl.col('Quantity') >= 100) & (pl.col('Quantity') <= 300)) | ((pl.col('Quantity') >= 600) & (pl.col('Quantity') <= 900)))

print("Number of selected rows for table5: {}", table5.select(pl.count()).collect());

table6 = table5.filter(  
        (pl.col('Contact') >= 'C32') & (pl.col('Contact') <= 'C39'))

print("Number of selected rows for table6: {}", table6.select(pl.count()).collect());

table7 = table6.filter(  
        (pl.col('Contact') != "C33"))

print("Number of selected rows for table7: {}", table7.select(pl.count()).collect());

path: pathlib.Path = "Output/PolarsFilterByDifferentCompareOperators10M.csv"

final = table7.collect(streaming=True)

final.write_csv(path)

e = time.time()
print("Polars Filter Data Time = {}".format(round(e-s,3)))







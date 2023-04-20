import polars as pl
import pathlib
import time

s = time.time()
df = pl.read_csv("Input/10MillionRows.csv")
path: pathlib.Path = "Output/PolarResult-10MillionRows.csv"
df.write_csv(path, separator=",")

e = time.time()
print("Polars Read/Write 10 Million Rows Time = {}".format(e-s))



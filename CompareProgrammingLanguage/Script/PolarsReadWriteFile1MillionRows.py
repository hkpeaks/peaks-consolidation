import polars as pl
import pathlib
import time

s = time.time()
df = pl.read_csv("Input/1MillionRows.csv")
path: pathlib.Path = "Output/PolarResult-1MillionRows.csv"
df.write_csv(path, separator=",")

e = time.time()
print("Polars Read/Write 1 Million Rows Time = {}".format(e-s))



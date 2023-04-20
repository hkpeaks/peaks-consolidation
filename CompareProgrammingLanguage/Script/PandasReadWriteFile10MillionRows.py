import pandas as pd
import pathlib
import time

s = time.time()
df = pd.read_csv("Input/10MillionRows.csv")
df.to_csv('Output/PandasResult-10MillionRows.csv', index=False)
e = time.time()
print("Pandas Read/Write 10 Million Rows Time = {}".format(e-s))



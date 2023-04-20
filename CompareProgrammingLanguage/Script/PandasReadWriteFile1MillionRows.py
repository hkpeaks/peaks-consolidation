import pandas as pd
import pathlib
import time

s = time.time()
df = pd.read_csv("Input/1MillionRows.csv")
df.to_csv('Output/PandasResult-1MillionRows.csv', index=False)
e = time.time()
print("Pandas Read/Write 1 Million Rows Time = {}".format(e-s))



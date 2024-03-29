import sys
import os
from datetime import datetime
import peakrs as pr
from peakpy import *
import time

# get the username from command line arguments
username = sys.argv[2]

session = datetime.now().strftime(f"{username}_%y%m%d_%H%M%S")

os.makedirs(f"Outbox/{session}", exist_ok=True)

df = pr.Dataframe()
df.log_file_name = f"Outbox/{session}/PyPeakrs_Log.csv"
df.partition_size_mb = 5
df.thread = 100

pr.create_log(df)

query1 = query(
    "filter", "Style(=F)",
    "build_key_value", "Product, Style => Table(key_value)")

master_df = run_batch(df, "Inbox/Master.csv", query1)

query2 = query(    
    "filter", "Shop(S90..S99)",
    "join_key_value", "Product, Style => Inner(key_value)",    
    "add_column", "Quantity, Unit_Price => Multiply(Amount)",
    "filter", "Amount:Float(>100000)",
    "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)"
)

source_file = os.path.join("Inbox/", sys.argv[1])
result_file = [f"Outbox/{session}/Peakpy-Detail-Result-{os.path.basename(source_file)}", 
               f"Outbox/{session}/Peakpy-Summary-Result-{os.path.basename(source_file)}"]

run_stream(df, source_file, master_df, query2, result_file)

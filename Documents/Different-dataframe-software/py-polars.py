import polars as pl
import os
import sys
import time

start_time = time.time()

master = pl.scan_csv("Inbox/Master.csv")

filter_master = master.filter((pl.col('Style') == "C") | (pl.col('Style') == "F"))  # Corrected here

source_file_path = os.path.join("Inbox/", sys.argv[1])

fact_table = pl.scan_csv(source_file_path)

detail_result = fact_table.filter((pl.col('Shop') >= "S77") & (pl.col('Shop') <= "S78")
                  ).join(filter_master, on=["Product","Style"], how="inner"
                  ).with_columns((pl.col("Quantity") * pl.col("Unit_Price")).alias("Amount")
                  ).filter(pl.col('Amount') > 100000)

summary_result = detail_result.group_by(by=["Shop", "Product", "Style"]).agg(
    [pl.count().alias('Count'), 
     pl.sum('Quantity').alias('Sum_Quantity'), 
     pl.sum('Amount').alias('Sum_Amount')])

result_file_path0 = f"Outbox/PyPolars_Detail_Result_{os.path.basename(source_file_path)}"
detail_result.sink_csv(result_file_path0)
detail_result = pl.scan_csv(result_file_path0)
sample_df0 = detail_result.fetch(10)
print(sample_df0)

result_file_path1 = f"Outbox/PyPolars_Summary_Result_{os.path.basename(source_file_path)}"
summary_result.sink_csv(result_file_path1)
summary_result = pl.scan_csv(result_file_path1)
sample_df1 = summary_result.fetch(10)
print(sample_df1)

end_time = time.time()
print("Polars CSV Duration (In Second): {}".format(round(end_time-start_time,3)))

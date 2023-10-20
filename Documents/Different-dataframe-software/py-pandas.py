import pandas as pd
import os
import sys
import time

start_time = time.time()

master = pd.read_csv("Inbox/Master.csv", engine='pyarrow')

filter_master = master[(master['Style'] == "C") | (master['Style'] == "F")]

source_file_path = os.path.join("Inbox/", sys.argv[1])

fact_table = pd.read_csv(source_file_path, engine='pyarrow')

detail_result = fact_table[(fact_table['Shop'] >= "S77") & (fact_table['Shop'] <= "S78")]
detail_result = pd.merge(detail_result, filter_master, on=["Product","Style"], how="inner")
detail_result['Amount'] = detail_result["Quantity"] * detail_result["Unit_Price"]
detail_result = detail_result[detail_result['Amount'] > 100000]

summary_result = detail_result.groupby(["Shop", "Product"]).agg(
    Count=('Amount', 'count'), 
    Sum_Quantity=('Quantity', 'sum'), 
    Sum_Amount=('Amount', 'sum')).reset_index()

print()
result_file_path0 = f"Outbox/PyPandas_Detail_Result_{os.path.basename(source_file_path)}"
detail_result.to_csv(result_file_path0, index=False)
sample_df0 = pd.read_csv(result_file_path0).head(10)
print(sample_df0)

print()
result_file_path1 = f"Outbox/PyPandas_Summary_Result_{os.path.basename(source_file_path)}"
summary_result.to_csv(result_file_path1, index=False)
sample_df1 = pd.read_csv(result_file_path1).head(10)
print(sample_df1)
print()

end_time = time.time()
print("Pandas CSV Duration (In Second): {}".format(round(end_time-start_time,3)))

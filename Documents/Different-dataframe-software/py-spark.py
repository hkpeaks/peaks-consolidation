from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os
import sys
import time

start_time = time.time()

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Local PySpark Application") \
    .getOrCreate()

master = spark.read.csv("Inbox/Master.csv", header=True, inferSchema=True)

filter_master = master.filter((col('Style') == "C") | (col('Style') == "F"))

source_file_path = os.path.join("Inbox/", sys.argv[1])

fact_table = spark.read.csv(source_file_path, header=True, inferSchema=True)

detail_result = fact_table.filter((col('Shop') >= "S77") & (col('Shop') <= "S78"))
detail_result = detail_result.join(filter_master, ["Product","Style"], how="inner")
detail_result = detail_result.withColumn('Amount', col("Quantity") * col("Unit_Price"))
detail_result = detail_result.filter(col('Amount') > 100000)

summary_result = detail_result.groupBy(["Shop", "Product", "Style"]).agg(
    expr('count(Amount)').alias('Count'), 
    expr('sum(Quantity)').alias('Sum_Quantity'), 
    expr('sum(Amount)').alias('Sum_Amount'))

result_file_path0 = f"Outbox/PySpark_Detail_Result_{os.path.basename(source_file_path)}"
detail_result.coalesce(1).write.csv(result_file_path0, header=True)
sample_df0 = spark.read.csv(result_file_path0, header=True, inferSchema=True).show(10)
print(sample_df0)

result_file_path1 = f"Outbox/PySpark_Summary_Result_{os.path.basename(source_file_path)}"
summary_result.coalesce(1).write.csv(result_file_path1, header=True)
sample_df1 = spark.read.csv(result_file_path1, header=True, inferSchema=True).show(10)
print(sample_df1)

end_time = time.time()
print("PySpark CSV Duration (In Second): {}".format(round(end_time-start_time,3)))

os._exit(0)

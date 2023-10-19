import re
import sys
import math
import peakrs as pr
from typing import List


def query(*commands):
    queries = []
    for i in range(0, len(commands), 2):
        queries.append({"Command": commands[i], "Setting": commands[i+1]})
    return queries

def replace_count_by_sum_count(query):    
    re_pattern = re.compile(r'(?i)count\(\)')
    result = re_pattern.sub(lambda m: "Sum(" + m.group(0)[:-2] + ")", query)    
    return result

def run_batch(ref_df: pr.Dataframe, source_file_path: str, query: List[str]) -> pr.Dataframe:    

    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)      
    df = pr.read_csv(ref_df, source_file_path)        

    for d in query:
        command = d["Command"].lower()

        if command == "build_key_value":
            df = pr.build_key_value(df, d["Setting"])
        elif command == "filter":
            df = pr.filter(df, d["Setting"])

    return df

def run_stream(ref_df: pr.Dataframe, source_file_path: str, master_df: pr.Dataframe, query: List[str], result_file_path: List[str]):    

    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)
    total_batch_float = math.ceil(ref_df.partition_count / ref_df.thread)
    total_batch = int(total_batch_float)      

    print("Partition Count: ", ref_df.partition_count)
    print("Streaming Batch Count: ", total_batch)

    final_df_group = {}
    ref_df.processed_partition = 0
    ref_df.streaming_batch = 0  

    is_append_to_disk = False

    if len(result_file_path[0]) > 5:
        try:
            file1 = open(result_file_path[0], "w")   
        except:
            print("Fail to create file")

    final_command = ""
    final_setting = ""    

    while ref_df.processed_partition < ref_df.partition_count:        

        df = pr.read_csv(ref_df, source_file_path)   

        for d in query:
            command = d["Command"].lower()

            if command == "add_column":
                df = pr.add_column(df, d["Setting"])
                is_append_to_disk = True

            elif command == "distinct":
                if len(result_file_path[0]) > 5:
                    pr.append_csv(df, result_file_path[0])
                
                df = pr.distinct(df, d["Setting"])                
                final_df_group[ref_df.streaming_batch] = df
                final_command = "distinct"
                final_setting = d["Setting"]
                is_append_to_disk = False

            elif command == "filter":
                df = pr.filter(df, d["Setting"])
                is_append_to_disk = True

            elif command == "filter_unmatch":
                df = pr.filter_unmatch(df, d["Setting"])
                is_append_to_disk = True

            elif command == "group_by":
                if len(result_file_path[0]) > 5:
                    pr.append_csv(df, result_file_path[0])

                df = pr.group_by(df, d["Setting"])
                final_df_group[ref_df.streaming_batch] = df
                final_command = "group_by"
                final_setting = d["Setting"]
                is_append_to_disk = False

            elif command == "final_distinct":
                result_df = pr.final_distinct(final_df_group, d["Setting"])
                is_append_to_disk = False
                pr.write_csv(result_df, result_file_path[1])
            elif command == "final_group_by":
                result_df = pr.final_group_by(final_df_group, d["Setting"])
                is_append_to_disk = False
                pr.write_csv(result_df, result_file_path[1])

            elif command == "join_key_value":
                df = pr.join_key_value(df, master_df, d["Setting"])
                is_append_to_disk = True

            elif command == "select_column":
                df = pr.select_column(df, d["Setting"])
                is_append_to_disk = True
            else:
                print(f"{d['Command']} command not found")

        if is_append_to_disk:
            if len(result_file_path[0]) > 5:
                pr.append_csv(df, result_file_path[0])

        ref_df.processed_partition += df.thread
        ref_df.streaming_batch += 1
        print(f"{ref_df.streaming_batch} ", end="")
        sys.stdout.flush()         
        
        if not is_append_to_disk:
            if final_command == "group_by":             
                new_query = replace_count_by_sum_count(final_setting)
                result_df = pr.final_group_by(final_df_group, new_query)              
                pr.write_csv(result_df, result_file_path[1])
            elif final_command == "distinct":
                result_df = pr.final_distinct(final_df_group, final_setting)
                pr.write_csv(result_df, result_file_path[1])      
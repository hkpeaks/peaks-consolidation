library(data.table)

# Enable multi-threading
setDTthreads(threads = 100)

start_time <- Sys.time()

master <- fread("Inbox/Master.csv")

filter_master <- master[Style == "C" | Style == "F"]  # Corrected here

source_file_path <- paste0("Inbox/", commandArgs(trailingOnly = TRUE)[1])

fact_table <- fread(source_file_path)

detail_result <- fact_table[Shop >= "S77" & Shop <= "S78"]
setkey(detail_result, Product, Style)
setkey(filter_master, Product, Style)
detail_result <- filter_master[detail_result]
detail_result[, Amount := Quantity * Unit_Price]
detail_result <- detail_result[Amount > 100000]

summary_result <- detail_result[, .(Count = .N, 
                                    Sum_Quantity = sum(Quantity), 
                                    Sum_Amount = sum(Amount)), 
                               by = .(Shop, Product, Style)]

result_file_path0 <- paste0("Outbox/Rdata.table_Detail_Result_", basename(source_file_path))
fwrite(detail_result, result_file_path0)
sample_df0 <- fread(result_file_path0)[1:10]
cat("\n")  
print(sample_df0)

result_file_path1 <- paste0("Outbox/Rdata.table_Summary_Result_", basename(source_file_path))
fwrite(summary_result, result_file_path1)
sample_df1 <- fread(result_file_path1)[1:10]
cat("\n")  
print(sample_df1)
cat("\n")  

end_time <- Sys.time()
print(paste("data.table CSV Duration (In Second):", round(end_time-start_time, 3)))

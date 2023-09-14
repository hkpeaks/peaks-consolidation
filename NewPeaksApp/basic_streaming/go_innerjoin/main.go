package main

import (
	"fmt"	
	"gopeaks/gopeaks"	
	"path/filepath"
	"time"
	"os"
)

func main() {
	start_time := time.Now()	

	var df gopeaks.Dataframe		

	df.Log_File_Name = "Outbox/Log-" +
		start_time.Format("060102-150405") + ".csv"

	gopeaks.Create_Log(df)	

	source_file_path := gopeaks.Get_CLI_file_path("10M-Fact.csv")
	
	gopeaks.View_Sample(source_file_path)	
	df.Partition_Size_MB = 10
	df.Thread = 30
		
	result_file_path := "Outbox/ResultInnerJoin-"+filepath.Base(source_file_path)

	inner_join(df, source_file_path, result_file_path);    	
	
	fmt.Println()
	gopeaks.View_Sample(source_file_path)

	end_time := time.Now()
	elapsed := end_time.Sub(start_time)

	fmt.Println()	
	fmt.Printf("Group_By Duration: %.3f (in second)\n", elapsed.Seconds())
	
}

func inner_join(ref_df gopeaks.Dataframe, source_file_path string, result_file_path string)  {

	ref_df = *gopeaks.Get_CSV_Partition_Address(ref_df, "Inbox/Master.csv")	
	master_df := gopeaks.Read_CSV(ref_df, "Inbox/Master.csv");     
   // master_df = gopeaks.Filter(*master_df,"Product(200..220)");        
    master_df = gopeaks.Build_Key_Value(*master_df, "Product, Style => Table(KeyValue)"); 

    ref_df = *gopeaks.Get_CSV_Partition_Address(ref_df, source_file_path);   
	fmt.Println("Partition Count: ", ref_df.Partition_Count)	
	ref_df.Processed_Partition = 0;
    ref_df.Streaming_Batch = 0; 	

	file, err := os.Create(result_file_path)
	if err != nil {
		//return "Fail to create file"
	}
	
	defer file.Close()
	
	for ref_df.Processed_Partition < ref_df.Partition_Count {		

		df := gopeaks.Read_CSV(ref_df, source_file_path)
		df = gopeaks.Filter(*df,"Shop(S11..S89)Product(105..899)")	   
		df = gopeaks.Join_Key_Value(*df, *master_df, "Product, Style => Inner(KeyValue)")
		df = gopeaks.Add_Column(*df, "Quantity, Unit_Price => Multiply(Amount)");	

		gopeaks.Append_CSV(*df, file, result_file_path)

		ref_df.Processed_Partition += df.Thread;    
        ref_df.Streaming_Batch += 1;
		fmt.Print(ref_df.Processed_Partition, " ")		
	}	
}

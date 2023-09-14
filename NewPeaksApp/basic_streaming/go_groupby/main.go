package main

import (
	"fmt"	
	"gopeaks/gopeaks"	
	"path/filepath"
	"time"
)

func main() {
	start_time := time.Now()	

	var df gopeaks.Dataframe		

	df.Log_File_Name = "Outbox/Log-" +
		start_time.Format("060102-150405") + ".csv"

		gopeaks.Create_Log(df)	

	source_file_path := gopeaks.Get_CLI_file_path("10-MillionRows.csv")
	
	gopeaks.View_Sample(source_file_path)	
	df.Partition_Size_MB = 10
	df.Thread = 100
	
	df = group_by(df, source_file_path)
	result_file_path := "Outbox/ResultGroupBy-"+filepath.Base(source_file_path)
	
	gopeaks.Write_CSV(df, result_file_path)	
	fmt.Println()
	gopeaks.View_Sample(source_file_path)

	end_time := time.Now()
	elapsed := end_time.Sub(start_time)

	fmt.Println()	
	fmt.Printf("Group_By Duration: %.3f (in second)\n", elapsed.Seconds())
	
}

func group_by(df gopeaks.Dataframe, source_file_path string) gopeaks.Dataframe {

	ref_df := *gopeaks.Get_CSV_Partition_Address(df, source_file_path)	

	fmt.Println("Partition Count: ", ref_df.Partition_Count)

	final_df_group := make(map[int]gopeaks.Dataframe)
	ref_df.Processed_Partition = 0;
    ref_df.Streaming_Batch = 0; 
	
	for ref_df.Processed_Partition < ref_df.Partition_Count {		

		df := gopeaks.Read_CSV(ref_df, source_file_path)
		df = gopeaks.Filter(*df,"Shop(S11..S89)Product(105..899)")	   
		//df = gopeaks.Filter(*df,"Base_Amount:Float(100..500)")
		df = gopeaks.Group_By(*df, "Shop, Product => Count() Sum(Quantity) Sum(Base_Amount)")

		final_df_group[ref_df.Streaming_Batch] = *df		
		ref_df.Processed_Partition += ref_df.Thread;    
        ref_df.Streaming_Batch += 1;
		fmt.Print(ref_df.Processed_Partition, " ")		
	}
	
	result_df := gopeaks.Final_Group_By(final_df_group,"Shop, Product => Sum(Count) Sum(Quantity) Sum(Base_Amount)")

	return *result_df
}

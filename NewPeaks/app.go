package peaks

import (
	"fmt"
	"os"
	"path/filepath"	
	"time"
)

func Start() {
	start_time := time.Now()

	var meta_info MetaInfo
	// Optional log record
	meta_info.log_file_name = "Outbox/Log-" +
		start_time.Format("060102-150405") + ".csv"

	// Determine source file path
	file_path := "D:/Peaks/100-MillionRows.csv"
	if len(os.Args) == 2 {
		file_path = os.Args[1]
	}

	// Optional to preview file prior to real execution
	View_Sample(file_path)

	// Start streaming processing for query
	meta_info.partition_size_mb = 60
	meta_info.thread = 10		

	// Sceanrio 1: If final command has either the distinct or groupby
	//vector, meta_info := Query_Stream(file_path, meta_info)	
	//Write_CSV(*vector, meta_info, "Outbox/Result-" +
	//filepath.Base(file_path))

    // Scenario 2: If the final query command does not include either the distinct or groupBy
	Query_Stream_Append_File(file_path, meta_info)

	end_time := time.Now()
	elapsed := end_time.Sub(start_time)

	fmt.Println()
	if elapsed.Seconds() <= 1 {
		fmt.Printf("Duration: %.3f second\n", elapsed.Seconds())
	} else {
		fmt.Printf("Duration: %.3f seconds\n", elapsed.Seconds())
	}
}

func Query_Stream(file_path string, meta_info MetaInfo) (*[]byte, MetaInfo) {

	Create_Log(meta_info)

	if meta_info.partition_size_mb == 0 {
		meta_info.partition_size_mb = 20
	}

	if meta_info.thread == 0 {
		meta_info.thread = 20
	}

	var query string

	_, meta_info = Get_CSV_Partition_Address(file_path, meta_info)

	fmt.Println()
	fmt.Println("Partition Count: ", Num_Format(float64(meta_info.partition_count)))

	final_vector_group := make(map[int][]byte)
	final_meta_info_group := make(map[int]MetaInfo)

	partition_batch := meta_info.thread

	var processed_partition, streaming_batch int

	for processed_partition < meta_info.partition_count {

		if meta_info.partition_count-processed_partition < meta_info.thread {
			partition_batch = meta_info.partition_count - processed_partition
		}

		meta_info.processed_partition = processed_partition
		meta_info.streaming_batch = streaming_batch

		vector_group, meta_info := Read_CSV(partition_batch, meta_info, file_path)

		query = "Shop(S20..S50)Product(500..800)"
		vector_group, meta_info = Filter(*vector_group, meta_info, query)

		query = "Shop, Product => Count() Sum(Quantity) Sum(Base_Amount)"
		vector, meta_info := Groupby(*vector_group, meta_info, query)

		final_vector_group[streaming_batch] = *vector
		final_meta_info_group[streaming_batch] = meta_info
		processed_partition += partition_batch

		fmt.Print(processed_partition, " ")

		streaming_batch++
	}

	revised_query := Replace_Count_By_Sum_Count(query)
	vector, meta_info := FinalGroupby(final_vector_group, final_meta_info_group, revised_query)
	fmt.Println()

	return vector, meta_info
}

func Query_Stream_Append_File(file_path string, meta_info MetaInfo) error {

	Create_Log(meta_info)

	if meta_info.partition_size_mb == 0 {
		meta_info.partition_size_mb = 20
	}

	if meta_info.thread == 0 {
		meta_info.thread = 20
	}

	_, meta_info = Get_CSV_Partition_Address(file_path, meta_info)

	fmt.Println()
	fmt.Println("Partition Count: ", Num_Format(float64(meta_info.partition_count)))

	var processed_partition, streaming_batch int

	output_file_path := "Outbox/Result-" + filepath.Base(file_path)

	file, err := os.Create(output_file_path)
	if err != nil {
		return err
	}
	defer file.Close()

	partition_batch := meta_info.thread

	for processed_partition < meta_info.partition_count {

		if meta_info.partition_count-processed_partition < meta_info.thread {
			partition_batch = meta_info.partition_count - processed_partition
		}

		meta_info.processed_partition = processed_partition
		meta_info.streaming_batch = streaming_batch

		vector_group, meta_info := Read_CSV(partition_batch, meta_info, file_path)

		query := "Shop(S20..S50)Product(500..800)"
		vector_group, meta_info = Filter(*vector_group, meta_info, query)

		Append_CSV(*vector_group, meta_info, file, file_path)
		
		processed_partition += partition_batch
		fmt.Print(processed_partition, " ")
		streaming_batch++
	}
	return nil
}
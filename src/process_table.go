package peaks

import (	
	"fmt"
	"log"
	"os"
	"path/filepath"	
	"strings"
	"sync"
	"time"		
)

func ProcessTable(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *CachePartition {		

	var error_message string
	result_table_partition := make(map[int]Cache)		
    var table_name = strings.ToUpper(ir.source_table_name)	
	
	if strings.Contains(table_name, "*.") { // Process all csv files from a folder
		
		result_table_partition = *FileFolder(table_partition_store, task, rule, ir)

	} else if strings.Contains(table_name, ".") { // Process a file by an auto in-memory/streaming model

		result_table_partition = *AutoStreaming(table_partition_store, task, rule, ir)

	} else { // Process a table by an in-memory model

		result_table_partition = *InMemory(table_partition_store, task, rule, ir)
	}


	var total_row int32

	for current_partition := 0; current_partition < len(result_table_partition); current_partition++ {
		total_row += result_table_partition[current_partition].partition_row
	}

	var result_table CachePartition
	result_table.table_partition = *&result_table_partition
	result_table.table_name = ir.return_table_name
	result_table.error_message = error_message
	result_table.total_row = total_row

	return &result_table
}

func FileFolder(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *map[int]Cache {	

	current_folder := ir.source_table_name
	result_table_partition := *ParallelStreamingFolder(table_partition_store, task, rule, ir, current_folder)

	return &result_table_partition
}

func AutoStreaming(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *map[int]Cache {

	var input_folder string = "Input/"
	var current_file string
	result_table_partition := make(map[int]Cache)

	if strings.Contains(ir.source_table_name, "/") || strings.Contains(ir.source_table_name, "\\") {
		input_folder = ""
	}

	if len(ir.original_source_table_name) == 0 { // File is referred to previous "Select" command when combine query conditions are statisify
		ir.original_source_table_name = rule.current_query.source_table_name
		current_file = ir.original_source_table_name
	}

	file, err := os.Open(input_folder + ir.original_source_table_name)

	if err != nil {
		fmt.Println()
		fmt.Println("1** File", ir.source_table_name, "not found **")
		os.Exit(0)
	}
	defer file.Close()

	file2, err := os.Stat(input_folder + ir.original_source_table_name)

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	var total_byte = file2.Size()

	if total_byte < int64(rule.streamMB*1000000) { // Read file run in in-memory model

		source_table := ReadFile(task, rule, current_file)

		for _, column := range ir.validate_all_column_name {

			if _, found := source_table.table_partition[0].upper_column_name2id[strings.ToUpper(column)]; !found {

				if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
					if strings.ToUpper(column) != "$COLUMN" {
						fmt.Println("** Column", column, "not found **")
						os.Exit(0)
					}
				}
			}
		}

		ir.return_table_name = source_table.table_name
		table_partition := *&source_table.table_partition
		table_partition_store[strings.ToUpper(ir.return_table_name)] = *&table_partition

		ir.source_table_name = ir.return_table_name

		result_table := *CurrentStream(1, &table_partition_store, task, rule, ir)

		result_table_partition = *&result_table.table_partition
		//error_message = result_table.error_message

		// Write file to disk if return table is a file name
		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

			var f *os.File
			var csv_string strings.Builder

			csv_string.WriteString(result_table.table_partition[0].column_name[0])

			for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
				csv_string.WriteString(",")
				csv_string.WriteString(result_table.table_partition[0].column_name[x])
			}

			csv_string.WriteString("\r\n")

			write_partition := make(map[int]CachePartition)
			write_partition[1] = *&result_table

			CurrentWriteFile(ir, csv_string, 1, write_partition, err, f)
		}

	} else { // Read file to run in streaming model		

		result_table_partition = *ParallelStreaming(table_partition_store, task, rule, ir, total_byte, file)
	}

	return &result_table_partition
}

func InMemory(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *map[int]Cache {

	ParallelAddCellAddress2Table := func(table_partition_store map[string]map[int]Cache, ir InternalRule, rule Rule) *map[string]map[int]Cache {

		CurrentAddCellAddress2Table := func(table_partition_store map[string]map[int]Cache, ir InternalRule, rule Rule, current_partition int) *Cache {

			CurrentTablePartition := func(table_partition_store map[string]map[int]Cache, ir InternalRule, current_partition int) *Cache {
				var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][current_partition]
				return &result_table
			}

			CurrentPartitionByteStream2 := func(table_partition_store map[string]map[int]Cache, ir InternalRule, current_partition int) *[]byte {
				var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][current_partition]
				var bytestream = result_table.bytestream
				return &bytestream
			}

			read_csv_delimiter := rule.read_csv_delimiter
			source_table := *CurrentTablePartition(table_partition_store, ir, current_partition)

			var result_table Cache

			if len(table_partition_store[ir.source_table_name][current_partition].cell_address) <= 2 {

				var cell_address []uint32
				var bytestream []byte
				var double_quote_count int

				var row int

				bytestream = *CurrentPartitionByteStream2(table_partition_store, ir, current_partition)

				cell_address = append(cell_address, 0)

				for x := 0; x < len(bytestream); x++ {
					if bytestream[x] == read_csv_delimiter {
						if double_quote_count != 1 {
							cell_address = append(cell_address, uint32(x+1))
							double_quote_count = 0
						}
					} else if bytestream[x] == 10 {
						cell_address = append(cell_address, uint32(x+1))
						row++
					} else if bytestream[x] == 13 {
						cell_address = append(cell_address, uint32(x+1))
					} else if bytestream[x] == 34 {
						double_quote_count += 1
					}
				}

				result_table.cell_address = cell_address

			} else {
				result_table.cell_address = source_table.cell_address
			}

			result_table.bytestream = source_table.bytestream
			result_table.column_name = source_table.column_name
			result_table.data_type = source_table.data_type
			result_table.extra_line_br_char = source_table.extra_line_br_char
			result_table.keyvalue_table = source_table.keyvalue_table
			result_table.partition_row = source_table.partition_row
			result_table.total_column = source_table.total_column
			result_table.total_row = source_table.total_row
			result_table.upper_column_name2id = source_table.upper_column_name2id
			result_table.value_column_name = source_table.value_column_name

			return &result_table
		}

		result_table_partition := make(map[int]Cache)

		if len(table_partition_store[ir.source_table_name]) > 1 {

			var mutex sync.Mutex
			var parallel sync.WaitGroup
			p := len(table_partition_store[ir.source_table_name])
			parallel.Add(p)

			for current_partition := 0; current_partition < p; current_partition++ {

				go func(current_partition int) {
					defer parallel.Done()
					result_table := *CurrentAddCellAddress2Table(table_partition_store, ir, rule, current_partition)
					mutex.Lock()
					result_table_partition[current_partition] = *&result_table
					mutex.Unlock()

				}(current_partition)
			}

			parallel.Wait()

		} else {

			for current_partition := 0; current_partition < len(table_partition_store[ir.source_table_name]); current_partition++ {
				result_table := *CurrentAddCellAddress2Table(table_partition_store, ir, rule, current_partition)
				result_table_partition[current_partition] = *&result_table
			}

		}

		table_partition_store[ir.source_table_name] = result_table_partition

		return &table_partition_store
	}

	//var error_message string
	result_table_partition := make(map[int]Cache)

	// Find cell address of current CSV bytestream if it is missing
	table_partition_store = *ParallelAddCellAddress2Table(table_partition_store, ir, rule)

	result_table := *CurrentStream(0, &table_partition_store, task, rule, ir)
	result_table_partition = *&result_table.table_partition
	//error_message = result_table.error_message

	// Write file to disk if return table is a file name
	if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

		var f *os.File
		var csv_string strings.Builder

		csv_string.WriteString(result_table.table_partition[0].column_name[0])

		for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
			csv_string.WriteString(",")
			csv_string.WriteString(result_table.table_partition[0].column_name[x])
		}

		csv_string.WriteString("\r\n")

		write_partition := make(map[int]CachePartition)
		write_partition[1] = *&result_table

		CurrentWriteFile(ir, csv_string, 1, write_partition, nil, f)
	}

	return &result_table_partition
}

func ParallelStreaming(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule, total_byte int64, file *os.File) *map[int]Cache {
			
	rule.byte_per_stream = int64(rule.streamMB * 1000000)
	var streaming_count int = int(total_byte / rule.byte_per_stream)

	if streaming_count < 1 {
		streaming_count = 1
	}

	var total_partition int = streaming_count * rule.thread

	if total_byte > 50000000 {
		rule.thread = total_partition // divide the table into n partition
	} else {
		rule.thread = 10
	}

	ds := *FileStructure(rule, total_byte, file)
	rule.thread = total_partition / streaming_count
	fmt.Println("Total Bytes:", total_byte, "| Total Batches of Stream:", streaming_count)
	partition_address := *PartitionAddress(ds)
	bytestream_partition := make(map[int][]byte)
	extraction_batch := make(map[int]int)
	query_batch := make(map[int]int)
	combine_table_partition := make(map[int]Cache)
	result_table_partition_store := make(map[string]map[int]Cache)
	result_table_partition := make(map[int]Cache)

	go CurrentExtraction(extraction_batch, query_batch, bytestream_partition, streaming_count, rule, file, partition_address)

	var total_row int32
	var total_column int
	var csv_string strings.Builder
	var f *os.File
	var err error
	write_partition := make(map[int]CachePartition)
	var p int

	for batch := 1; batch <= streaming_count; batch++ {
		for current_partition := 0; current_partition < rule.thread*(len(query_batch)-1); current_partition++ {
			partition.Lock()
			if _, found := bytestream_partition[current_partition]; found {
				delete(bytestream_partition, current_partition)
			}
			partition.Unlock()
		}

		for batch > len(extraction_batch) {
			time.Sleep(100 * time.Millisecond)
		}

		table_partition := make(map[int]Cache)

		var mutex2 sync.Mutex
		var parallel2 sync.WaitGroup

		parallel2.Add(rule.thread)
		var i int = 0

		for current_partition := (batch - 1) * rule.thread; current_partition < rule.thread*batch; current_partition++ {
			go func(current_partition int) {
				defer parallel2.Done()
				result_table := *CellAddress2(ds, false, current_partition, rule, ir.source_table_name, bytestream_partition)
				mutex2.Lock()
				if batch == 1 {
					for _, column := range ir.validate_all_column_name {

						if _, found := result_table.upper_column_name2id[strings.ToUpper(column)]; !found {

							if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
								if strings.ToUpper(column) != "$COLUMN" {
									fmt.Println("** Column", column, "not found **")
									os.Exit(0)
								}
							}
						}
					}
				}
				table_partition[i] = *&result_table
				i++
				mutex2.Unlock()
			}(current_partition)
		}

		parallel2.Wait()

		table_partition_store[strings.ToUpper(ir.return_table_name)] = *&table_partition
		ir.source_table_name = ir.return_table_name

		result_table := *CurrentStream(batch, &table_partition_store, task, rule, ir)

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {
		} else {

			ir.column_id_seq = result_table.ir.column_id_seq
			ir.column_name = result_table.ir.column_name
			result_table_partition = *&result_table.table_partition

			for i := 0; i < len(result_table_partition); i++ {

				if len(result_table_partition[i].bytestream) > 0 {
					combine_table_partition[p] = result_table_partition[i]
					p++
				}
			}
		}

		query_batch[batch] = 1
		fmt.Print(batch, " ")

		if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

			if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

				for current_partition := 0; current_partition < len(result_table.table_partition); current_partition++ {
					total_row += result_table.table_partition[current_partition].partition_row
				}

				partition.Lock()
				write_partition[batch] = *&result_table
				partition.Unlock()

				for len(write_partition) > 5 {
					time.Sleep(100 * time.Millisecond)
				}

				if batch == 1 {

					total_column = len(result_table.table_partition[0].column_name)

					csv_string.WriteString(result_table.table_partition[0].column_name[0])

					for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
						csv_string.WriteString(",")
						csv_string.WriteString(result_table.table_partition[0].column_name[x])
					}

					csv_string.WriteString("\r\n")

					go CurrentWriteFile(ir, csv_string, streaming_count, write_partition, err, f)
				}
			}
		}
	}

	if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") { // full streaming
			var result_table Cache
			result_table.partition_row = total_row
			result_table.total_column = total_column
			combine_table_partition[0] = result_table

			for len(write_partition) > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			return &combine_table_partition

		} else { // output to in-memory table

			for current_partition := 0; current_partition < len(combine_table_partition); current_partition++ {
				total_row += combine_table_partition[current_partition].partition_row
			}

			total_column = len(combine_table_partition[0].column_name)

			return &combine_table_partition
		}

	} else { // Distinct or GroupBy

		result_table_partition_store["TEMP"] = *&combine_table_partition

		ir.source_table_name = "TEMP"

		column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq2(&result_table_partition_store, rule, ir)
		ir.column_id_seq = column_id_seq
		ir.column_name = column_name

		result_table_partition[0] = *CurrentCommand(1, combine_table_partition, 0, len(combine_table_partition), task, ir, rule, upper_column_name2id, data_type)

		return &result_table_partition
	}
}

func ParallelStreamingFolder(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule, current_folder string) *map[int]Cache {		

	var parameter, setting, return_table_name string

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {

		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	var file_list []string
	var file_size []int64

	ext := ".csv"

	source := strings.Replace(strings.ToUpper(current_folder), "*.CSV", "", 1)

	err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(info.Name(), ext) {
			file_list = append(file_list, path)
			file_size = append(file_size, info.Size())
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

	var batch_size = 100
	var total_batch = len(file_list) / batch_size
	var last_batch_size = total_batch - batch_size*total_batch
	if last_batch_size > 0 {
		total_batch++
	}

	//fmt.Println("total_batch ", total_batch)

	current_batch_stream := make(map[int]map[int]Cache)

	go CurrentExtraction2(batch_size, file_list, current_batch_stream, task, rule, current_folder)

	var total_row int32
	var total_column int
	result_table_partition := make(map[int]Cache)
	var result_table CachePartition
	var f *os.File
	var csv_string strings.Builder
	write_partition := make(map[int]CachePartition)
	result_table_partition_store := make(map[string]map[int]Cache)
	temp_combine_table_partition := make(map[int]Cache)
	combine_table_partition := make(map[int]Cache)
	var p int

	for current_batch := 1; current_batch <= total_batch; current_batch++ {

		for len(current_batch_stream) < 2 && current_batch < total_batch {
			time.Sleep(100 * time.Millisecond)
		}

		current_batch_size := len(current_batch_stream[current_batch])

		for current_partition := 0; current_partition < current_batch_size; current_partition++ {
			temp_combine_table_partition[current_partition] = current_batch_stream[current_batch][current_partition]
		}

		partition.Lock()
		if _, found := current_batch_stream[current_batch]; found {
			for current_partition := 0; current_partition < current_batch_size; current_partition++ {
				delete(current_batch_stream[current_batch], current_partition)
			}
			delete(current_batch_stream, current_batch)
		}
		partition.Unlock()

		/*
			for _, column := range ir.validate_all_column_name {

				if _, found := combine_table_partition[0].upper_column_name2id[strings.ToUpper(column)]; !found {

					if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
						if strings.ToUpper(column) != "$COLUMN" {
							fmt.Println("1** Column", column, "not found **")
							os.Exit(0)

						}
					}
				}
			}*/

		table_partition_store[strings.ToUpper(return_table_name)] = *&temp_combine_table_partition

		ir.source_table_name = ir.return_table_name

		result_table = *CurrentStream(current_batch, &table_partition_store, task, rule, ir)

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {
		} else {

			ir.column_id_seq = result_table.ir.column_id_seq
			ir.column_name = result_table.ir.column_name
			result_table_partition = *&result_table.table_partition

			for i := 0; i < len(result_table_partition); i++ {

				if len(result_table_partition[i].bytestream) > 0 {
					combine_table_partition[p] = result_table_partition[i]
					p++
				}
			}
		}

		// Write file to disk if return table is a file name
		if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

			if strings.Contains(strings.ToUpper(return_table_name), ".CSV") {

				for current_partition := 0; current_partition < len(result_table.table_partition); current_partition++ {
					total_row += result_table.table_partition[current_partition].partition_row
				}

				partition.Lock()
				write_partition[current_batch] = *&result_table
				partition.Unlock()

				for len(write_partition) > 5 {
					time.Sleep(100 * time.Millisecond)
				}

				if current_batch == 1 {

					total_column = len(result_table.table_partition[0].column_name)

					csv_string.WriteString(result_table.table_partition[0].column_name[0])

					for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
						csv_string.WriteString(",")
						csv_string.WriteString(result_table.table_partition[0].column_name[x])
					}

					csv_string.WriteString("\r\n")

					go CurrentWriteFile(ir, csv_string, total_batch, write_partition, err, f)

				}
			}
		}
	}

	if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") { // full streaming
			var result_table Cache
			result_table.partition_row = total_row
			result_table.total_column = total_column

			combine_table_partition[0] = result_table

			for len(write_partition) > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			return &combine_table_partition

		} else { // output to in-memory table

			for current_partition := 0; current_partition < len(combine_table_partition); current_partition++ {
				total_row += combine_table_partition[current_partition].partition_row
			}

			total_column = len(combine_table_partition[0].column_name)

			return &combine_table_partition
		}

	} else { // Distinct or GroupBy

		result_table_partition_store["TEMP"] = *&combine_table_partition

		ir.source_table_name = "TEMP"
		column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq2(&result_table_partition_store, rule, ir)
		ir.column_id_seq = column_id_seq
		ir.column_name = column_name

		result_table_partition[0] = *CurrentCommand(1, combine_table_partition, 0, len(combine_table_partition), task, ir, rule, upper_column_name2id, data_type)

		return &result_table_partition
	}
}

func CurrentStream(batch int, table_partition_store *map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *CachePartition {

	ParallelDataBending := func(batch int, table_partition_store map[string]map[int]Cache, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *map[int]Cache {

		SelectCurrentTable := func(table_partition_store map[string]map[int]Cache, source_table_name string) *map[int]Cache {

			result_table := make(map[int]Cache)

			for p := 0; p < len(table_partition_store[strings.ToUpper(source_table_name)]); p++ {
				result_table[p] = table_partition_store[strings.ToUpper(source_table_name)][p]
			}

			return &result_table
		}

		result_table_partition := make(map[int]Cache)
		var mutex sync.Mutex
		var parallel sync.WaitGroup

		parallel.Add(len(table_partition_store[strings.ToUpper(ir.source_table_name)]))

		current_table := *SelectCurrentTable(table_partition_store, ir.source_table_name)
		revised_table := make(map[int]Cache)

		var n int

		for current_partition := 0; current_partition < len(current_table); current_partition++ {

			if len(current_table[current_partition].bytestream) > 0 {
				revised_table[n] = current_table[current_partition]
				n++
			}
		}

		for current_partition := 0; current_partition < len(table_partition_store[strings.ToUpper(ir.source_table_name)]); current_partition++ {
			go func(current_partition int) {
				defer parallel.Done()
				result_table := *CurrentCommand(batch, revised_table, current_partition, 1, task, ir, rule, upper_column_name2id, data_type)
				mutex.Lock()
				result_table_partition[current_partition] = *&result_table
				n++
				mutex.Unlock()

			}(current_partition)
		}
		parallel.Wait()

		return &result_table_partition
	}

	result_table_partition := make(map[int]Cache)
	result_table_partition_store := make(map[string]map[int]Cache)

	filter_column_id_seq, column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq(table_partition_store, rule, ir)

	ir.filter_column_id_seq = filter_column_id_seq
	ir.column_id_seq = column_id_seq
	ir.column_name = column_name

	temp_table := *ParallelDataBending(batch, *table_partition_store, task, ir, rule, upper_column_name2id, data_type)

	if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

		var result_table CachePartition
		result_table.ir = ir
		result_table.table_partition = *&temp_table
		return &result_table

	} else {

		if len(temp_table) == 1 {
			result_table_partition[0] = temp_table[0]

		} else {

			result_table_partition_store["TEMP"] = *&temp_table
			ir.source_table_name = "TEMP"
			column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq2(&result_table_partition_store, rule, ir)
			ir.column_id_seq = column_id_seq
			ir.column_name = column_name

			result_table_partition[0] = *CurrentCommand(batch, temp_table, 0, len(temp_table), task, ir, rule, upper_column_name2id, data_type)
		}

		var result_table CachePartition
		result_table.ir = ir
		result_table.table_partition = *&result_table_partition
		return &result_table
	}
}
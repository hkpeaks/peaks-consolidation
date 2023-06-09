package peaks

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var partition sync.RWMutex

// Read a file or list of files
func ReadFile(task Task, rule Rule, current_file_name string) *CachePartition {

	// CombinePartition is nested inside ReadFile
	CombinePartition := func(current_batch_files map[int]map[int]Cache) *map[int]Cache {

		var total_file int
		result_table := make(map[int]Cache)

		for _, table := range current_batch_files {
			for _, partition := range table {
				result_table[total_file] = partition
				total_file++
			}
		}

		return &result_table
	}

	var parameter, setting, return_table_name, source string

	combine_table_partition := make(map[int]Cache)
	temp_table_partition := make(map[int]Cache)
	current_batch_files := make(map[int]map[int]Cache)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {

		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = setting
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	if len(current_file_name) > 0 {
		source = current_file_name
	}

	var file_list []string
	var file_size []int64

	if strings.Contains(strings.ToUpper(source), "*.CSV") { // Process multiple csv file in a file list

		source = strings.Replace(strings.ToUpper(source), "*.CSV", "", 1)
		ext := ".csv"

		err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if strings.HasSuffix(info.Name(), ext) {
				//fmt.Printf("%v %d bytes\n", path, info.Size())
				file_list = append(file_list, path)
				file_size = append(file_size, info.Size())
			}

			return nil
		})

		if err != nil {
			panic(err)
		}

		var current_batch, current_batch_max_file, total_file int
		var batch_size = 100

		for total_file < len(file_list) {

			if (current_batch+1)*batch_size <= len(file_list) {
				current_batch_max_file = (current_batch + 1) * batch_size
			} else {
				current_batch_max_file = len(file_list)
			}

			start_batch := current_batch * batch_size

			if current_batch_max_file-(current_batch*batch_size) < batch_size {
				batch_size = current_batch_max_file - (current_batch * batch_size)
			}

			current_batch_files = *ParallelReadFile(task, rule, file_list, batch_size, start_batch, current_batch_max_file, current_file_name)

			temp_table_partition = *CombinePartition(current_batch_files)

			for _, partition := range temp_table_partition {
				combine_table_partition[total_file] = partition
				total_file++
			}

			temp_table_partition = nil

			current_batch++

			fmt.Print(total_file, " ")
		}

		if total_file != current_batch_max_file {
			fmt.Print(current_batch_max_file, " ")
		}

	} else { // Process single csv file
		combine_table_partition = *ReadCurrentFile(0, nil, task, rule, current_file_name)
	}

	var total_row int32

	for current_partition := 0; current_partition < len(combine_table_partition); current_partition++ {
		total_row += combine_table_partition[current_partition].partition_row
	}

	var result_table CachePartition
	result_table.table_partition = *&combine_table_partition
	result_table.table_name = return_table_name
	result_table.total_row = total_row

	return &result_table
}

// Read file in parallel or in order
func ParallelReadFile(task Task, rule Rule, file_list []string, batch_size int, start_file int, end_file int, current_file_name string) *map[int]map[int]Cache {

	current_batch_files := make(map[int]map[int]Cache)
	var mutex sync.Mutex
	var parallel sync.WaitGroup
	parallel.Add(batch_size)

	if batch_size > 0 { // Read files in parallel

		for current_file := start_file; current_file < end_file; current_file++ {
			go func(current_file int) {
				defer parallel.Done()
				temp_table := *ReadCurrentFile(current_file, file_list, task, rule, current_file_name)
				mutex.Lock()
				current_batch_files[current_file] = *&temp_table
				mutex.Unlock()

			}(current_file)
		}
		parallel.Wait()

	} else { // Read files in order

		for current_file := start_file; current_file < end_file; current_file++ {
			temp_table_partition := *ReadCurrentFile(current_file, file_list, task, rule, current_file_name)
			current_batch_files[current_file] = *&temp_table_partition
		}
	}

	return &current_batch_files
}

func ReadCurrentFile(current_file int, file_list []string, task Task, rule Rule, current_file_name string) *map[int]Cache {

	var parameter, setting, source string

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {

		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = setting
		}
	}

	if len(file_list) > 0 { // one of many files
		source = file_list[current_file]
	} else if len(source) == 0 { // one file
		source = current_file_name
	}

	var input_folder string = "Input/"

	// read absolute file path or absolute file path
	if strings.Contains(source, "/") || strings.Contains(source, "\\") {
		input_folder = ""
	}

	file, err := os.Open(input_folder + source)
	defer file.Close()
	if err != nil {
		fmt.Println()
		fmt.Println("** File", source, "not found **")
		os.Exit(0)
	}

	file2, err := os.Stat(input_folder + source)
	if err != nil {
		//	log.Fatalf("unable to read file: %v", err)
	}

	//fmt.Println("rule.thread ", rule.thread)

	var total_byte = file2.Size()

	if total_byte > 100000000 {
		rule.thread = 10
	} else {
		rule.thread = 1
	}

	ds := *FileStructure(rule, total_byte, file)
	table_partition := make(map[int]Cache)
	var result_table Cache

	if rule.thread > 1 {

		var mutex sync.Mutex
		var parallel sync.WaitGroup
		parallel.Add(rule.thread)

		for current_partition := 0; current_partition < rule.thread; current_partition++ {
			go func(current_partition int) {
				defer parallel.Done()
				result_table = *CellAddress(ds, false, current_partition, rule, source, file)
				mutex.Lock()
				table_partition[current_partition] = *&result_table
				mutex.Unlock()
			}(current_partition)
		}
		parallel.Wait()

	} else {
		result_table = *CellAddress(ds, false, 0, rule, source, file)
		table_partition[0] = *&result_table
	}

	return &table_partition
}

func File2Bytestream(filename string) []byte {
	bytestream, err := os.ReadFile(filename)

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	return bytestream
}

func FileStructure(rule Rule, total_byte int64, file *os.File) *DataStructure {

	ColumnType := func(total_column int, column_name []string, start_address int64, rule Rule, file *os.File, total_byte int64) (int64, []string) {

		var current_byte int64

		if total_byte > 10000 {
			current_byte = 10000
		} else {
			current_byte = total_byte
		}

		header_byte := make([]byte, current_byte)
		file.ReadAt(header_byte, start_address+1)

		var n int64 = 0
		var current_column int = 0
		var _double_quote_count int = 0
		var temp_bytes []byte
		var data_type []string
		var row int = 0
		var cell int = 0

		for n < current_byte && row < 100 {

			temp_bytes = append(temp_bytes, header_byte[n])
			if header_byte[n] == byte(rule.read_csv_delimiter) && _double_quote_count%2 == 0 {

				if len(temp_bytes) > 0 {
					temp_bytes = temp_bytes[:len(temp_bytes)-1]
				}

				temp_bytes = nil
				current_column += 1
				cell++

			} else if header_byte[n] == 13 || header_byte[n] == 10 {

				if len(temp_bytes) > 0 {
					temp_bytes = temp_bytes[:len(temp_bytes)-1]
				}

				temp_bytes = nil
				current_column = 0
				row++
				cell++
			}
			n++
		}

		var estimated_cell int64 = total_byte / (int64(rule.thread) * n) * int64(cell)

		return estimated_cell, data_type
	}

	ColumnHeader := func(rule Rule, file *os.File, total_byte int64) (int, []string, map[string]int, int, int64) {

		read_csv_delimiter := rule.read_csv_delimiter

		if total_byte > 1000 {
			total_byte = 1000
		}

		header_byte := make([]byte, total_byte)
		file.ReadAt(header_byte, 0)

		var n int64 = 0
		var extra_line_br_char int
		var column_header_end_address int64
		var total_column int = 1
		var _double_quote_count int = 0
		var temp_bytes []byte
		var current_text string
		var column_name []string
		upper_column_name2id := make(map[string]int)

		for n < total_byte {
			temp_bytes = append(temp_bytes, header_byte[n])
			if header_byte[n] == read_csv_delimiter && _double_quote_count%2 == 0 {

				if len(temp_bytes) > 0 {
					temp_bytes = temp_bytes[:len(temp_bytes)-1]
				}

				current_text = strings.TrimSpace(string(temp_bytes))

				if len(current_text) == 0 {
					current_text = "Column" + strconv.Itoa(total_column-1)
				}
				column_name = append(column_name, current_text)
				upper_column_name2id[strings.ToUpper(current_text)] = total_column - 1

				temp_bytes = nil
				total_column += 1

			} else if header_byte[n] == 13 || header_byte[n] == 10 {

				if len(temp_bytes) > 0 {
					temp_bytes = temp_bytes[:len(temp_bytes)-1]
				}

				current_text = strings.TrimSpace(string(temp_bytes))

				if len(current_text) == 0 {

					current_text = "Column" + strconv.Itoa(total_column-1)

				}
				column_name = append(column_name, current_text)
				upper_column_name2id[strings.ToUpper(current_text)] = total_column - 1

				temp_bytes = nil

				if header_byte[n+1] == 13 || header_byte[n+1] == 10 {
					extra_line_br_char += 1
					column_header_end_address = n + 1
				}
				break
			}
			n++
		}

		return total_column, column_name, upper_column_name2id, extra_line_br_char, column_header_end_address
	}

	PartitionEndAddress := func(total_column int, rule Rule, column_header_end_address int64, total_byte int64, file *os.File) *map[int]int64 {

		partition_address := make(map[int]int64)
		var partition_quantity int = rule.thread
		var one_partition_size float64 = float64(total_byte / int64(partition_quantity))
		estimated_address := int64(math.Round(one_partition_size))
		var current_estimated_address int64

		var current_partition int = 0
		partition_address[current_partition] = column_header_end_address

		var buffer int64

		for x := 1; x <= partition_quantity; x++ {
			var i int64 = 0
			current_estimated_address = estimated_address * int64(x)

			if total_byte-current_estimated_address+1 < buffer {
				buffer = total_byte - current_estimated_address + 1
			} else {
				buffer = 10000
			}

			partition_ending_segment := make([]byte, buffer)

			file.ReadAt(partition_ending_segment, current_estimated_address)

			for {

				if i < buffer {

					if partition_ending_segment[i] == 13 || partition_ending_segment[i] == 10 {
						current_partition++
						partition_address[current_partition] = current_estimated_address + i + 1
						break
					}
					i++
				} else {
					break
				}
			}
		}

		if rule.thread == 1 {
			partition_address[1] = total_byte - 1
		} else {
			partition_address[current_partition] = total_byte - 1
		}

		return &partition_address
	}

	var total_column int = 1

	total_column, column_name, upper_column_name2id, extra_line_br_char, column_header_end_address := ColumnHeader(rule, file, total_byte)

	var estimated_cell int64
	estimated_cell, data_type := ColumnType(total_column, column_name, column_header_end_address, rule, file, total_byte)

	partition_address := *PartitionEndAddress(total_column, rule, column_header_end_address, total_byte, file)

	var ds DataStructure

	ds.total_column = total_column
	ds.column_name = column_name
	ds.data_type = data_type
	ds.extra_line_br_char = extra_line_br_char
	ds.partition_address = *&partition_address
	ds.upper_column_name2id = upper_column_name2id
	ds.estimated_cell = estimated_cell

	return &ds
}

func CellAddress(ds DataStructure, is_column_name_exist bool, current_partition int, rule Rule, source string, file *os.File) *Cache {

	read_csv_delimiter := rule.read_csv_delimiter
	partition_address := *PartitionAddress(ds)
	var buffer = partition_address[current_partition+1] - partition_address[current_partition]

	byte_stream := make([]byte, buffer)
	file.ReadAt(byte_stream, partition_address[current_partition]+1)

	current_partition_address := make(map[int]int64)
	current_partition_address[0] = -1
	current_partition_address[1] = buffer

	var _double_quote_count int = 0

	cell_address := make([]uint32, 0, ds.estimated_cell)

	cell_address = append(cell_address, uint32(current_partition_address[0])+1)

	var start_byte int = int(current_partition_address[0]) + 1

	var end_byte int = int(current_partition_address[1])

	var row int32

	for x := start_byte; x < end_byte; x++ {
		if byte_stream[x] == read_csv_delimiter {
			if _double_quote_count != 1 {
				cell_address = append(cell_address, uint32(x+1))
				_double_quote_count = 0
			}
		} else if byte_stream[x] == 10 {
			cell_address = append(cell_address, uint32(x+1))
			row++
		} else if byte_stream[x] == 13 {
			cell_address = append(cell_address, uint32(x+1))
		} else if byte_stream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var result_table Cache

	result_table.total_column = ds.total_column
	result_table.extra_line_br_char = ds.extra_line_br_char
	result_table.column_name = ds.column_name
	result_table.upper_column_name2id = ds.upper_column_name2id
	result_table.data_type = ds.data_type
	result_table.bytestream = byte_stream
	result_table.partition_row = row
	result_table.cell_address = cell_address

	return &result_table
}

// Parallel Streaming File

func CurrentExtraction(extraction_batch map[int]int, query_batch map[int]int, bytestream_partition map[int][]byte, streaming_count int, rule Rule, file *os.File, partition_address map[int]int64) {

	ReadByte := func(current_partition int, partition_address map[int]int64, file *os.File) *[]byte {

		var buffer = partition_address[current_partition+1] - partition_address[current_partition]

		bytestream := make([]byte, buffer)
		file.ReadAt(bytestream, partition_address[current_partition]+1)

		return &bytestream
	}

	for batch := 1; batch <= streaming_count; batch++ {

		for len(bytestream_partition)/rule.thread > 2 {
			time.Sleep(100 * time.Millisecond)
		}

		for current_partition := (batch - 1) * rule.thread; current_partition < rule.thread*batch; current_partition++ {
			partition.Lock()
			bytestream_partition[current_partition] = *ReadByte(current_partition, partition_address, file)
			partition.Unlock()
		}

		extraction_batch[batch] = 1
	}
}

func CellAddress2(ds DataStructure, is_column_name_exist bool, current_partition int, rule Rule, source string, bytestream_partition map[int][]byte) *Cache {

	CurrentPartitionBytestream := func(bytestream_partition map[int][]byte, current_partition int) *[]byte {
		partition.Lock()
		bytestream := bytestream_partition[current_partition]
		partition.Unlock()
		return &bytestream
	}

	bytestream := *CurrentPartitionBytestream(bytestream_partition, current_partition)
	partition_address := *PartitionAddress(ds)
	var buffer = partition_address[current_partition+1] - partition_address[current_partition]
	read_csv_delimiter := rule.read_csv_delimiter

	current_partition_address := make(map[int]int64)
	current_partition_address[0] = -1
	current_partition_address[1] = buffer

	var _double_quote_count int = 0

	cell_address := make([]uint32, 0, ds.estimated_cell)

	cell_address = append(cell_address, uint32(current_partition_address[0])+1)

	var start_byte int = int(current_partition_address[0]) + 1

	var end_byte int = int(current_partition_address[1])

	var row int32

	for x := start_byte; x < end_byte; x++ {
		if bytestream[x] == read_csv_delimiter {
			if _double_quote_count != 1 {
				cell_address = append(cell_address, uint32(x+1))
				_double_quote_count = 0
			}
		} else if bytestream[x] == 10 {
			cell_address = append(cell_address, uint32(x+1))
			row++
		} else if bytestream[x] == 13 {
			cell_address = append(cell_address, uint32(x+1))
		} else if bytestream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var result_table Cache

	result_table.total_column = ds.total_column
	result_table.extra_line_br_char = ds.extra_line_br_char
	result_table.column_name = ds.column_name
	result_table.upper_column_name2id = ds.upper_column_name2id
	result_table.data_type = ds.data_type
	result_table.bytestream = bytestream
	result_table.partition_row = row
	result_table.cell_address = cell_address

	return &result_table
}

// Parallel Streaming Folder
func CurrentExtraction2(batch_size int, file_list []string, current_batch_stream map[int]map[int]Cache, task Task, rule Rule, current_folder string) {

	// CombinePartition is nested inside ReadFile
	CombinePartition := func(current_batch_files map[int]map[int]Cache) *map[int]Cache {

		var total_file int
		result_table := make(map[int]Cache)

		for _, table := range current_batch_files {
			for _, partition := range table {
				result_table[total_file] = partition
				total_file++
			}
		}

		return &result_table
	}

	temp_table_partition := make(map[int]Cache)
	current_batch_files := make(map[int]map[int]Cache)

	var current_batch, current_batch_max_file, total_file int

	for total_file < len(file_list) {

		for len(current_batch_stream) > 3 {
			time.Sleep(100 * time.Millisecond)
		}

		if (current_batch+1)*batch_size <= len(file_list) {
			current_batch_max_file = (current_batch + 1) * batch_size
		} else {
			current_batch_max_file = len(file_list)
		}

		start_batch := current_batch * batch_size

		if current_batch_max_file-(current_batch*batch_size) < batch_size {
			batch_size = current_batch_max_file - (current_batch * batch_size)
		}

		current_batch_files = *ParallelReadFile(task, rule, file_list, batch_size, start_batch, current_batch_max_file, current_folder)

		temp_table_partition = *CombinePartition(current_batch_files)

		current_batch++

		current_batch_stream[current_batch] = temp_table_partition

		total_file += len(temp_table_partition)

		temp_table_partition = nil

		fmt.Print(total_file, " ")
	}

	if total_file != current_batch_max_file {
		fmt.Print(current_batch_max_file, " ")
	}
}

// SpliteFile

func SplitFile(task Task, rule Rule) string {

	var parameter, setting, source, return_table_name string

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	var input_folder string = "Input/"

	if strings.Contains(source, "/") || strings.Contains(source, "\\") {
		input_folder = ""
	}

	file, err := os.Open(input_folder + source)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	file2, err := os.Stat(input_folder + source)
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	var total_byte = file2.Size()

	if total_byte > 5662979 {
		if thread, err := strconv.Atoi(return_table_name); err == nil {
			rule.thread = thread
		}
	} else {
		rule.thread = 1
	}

	ds := *FileStructure(rule, total_byte, file)

	for current_file := 0; current_file < rule.thread; current_file++ {
		table := ByteStream(ds, false, current_file, rule, source, file)
		CurrentWriteFile2(*table, ds.column_name, current_file)

	}

	return return_table_name
}

func ByteStream(ds DataStructure, is_column_name_exist bool, current_partition int, rule Rule, source string, file *os.File) *Cache {

	var buffer = ds.partition_address[current_partition+1] - ds.partition_address[current_partition]

	partition_address := *PartitionAddress(ds)

	byte_stream := make([]byte, buffer)
	file.ReadAt(byte_stream, partition_address[current_partition]+1)

	var result_table Cache

	result_table.total_column = ds.total_column
	result_table.extra_line_br_char = ds.extra_line_br_char
	result_table.column_name = ds.column_name
	result_table.upper_column_name2id = ds.upper_column_name2id
	result_table.data_type = ds.data_type
	result_table.bytestream = byte_stream
	result_table.partition_row = 0
	result_table.cell_address = nil

	return &result_table
}

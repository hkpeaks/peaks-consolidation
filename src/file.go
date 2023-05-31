package peaks

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var partition sync.RWMutex

// Read File

func ReadFile(task Task, rule Rule) *CachePartition {

	var parameter, setting, return_table_name, source string

	current_table_partition := make(map[int]Cache)
	temp_table_partition := make(map[int]Cache)
	current_batch_partition := make(map[int]map[int]Cache)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {

		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = setting
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	var files []string
	var total_row int32

	if strings.Contains(strings.ToUpper(source), "*.CSV") {

		source = strings.Replace(strings.ToUpper(source), "*.CSV", "", 1)

		ext := ".csv"

		err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(info.Name(), ext) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}

		var current_batch int
		var batch_size = 1000
		var total_file int
		var current_batch_max_file int

		for total_file < len(files) {

			if (current_batch+1)*batch_size <= len(files) {
				current_batch_max_file = (current_batch + 1) * batch_size
			} else {
				current_batch_max_file = len(files)
			}

			start_batch := 0 + current_batch*batch_size

			if current_batch_max_file-(current_batch*batch_size) < batch_size {
				batch_size = current_batch_max_file - (current_batch * batch_size)
			}

			current_batch_partition = *ReadCurrentBatch(task, rule, files, batch_size, start_batch, current_batch_max_file)

			temp_table_partition = *CombinePartition(current_batch_partition)

			for _, v := range temp_table_partition {
				current_table_partition[total_file] = v
				//fmt.Print(total_file, " ")
				total_file++
			}

			temp_table_partition = nil
		
			fmt.Print((current_batch+1)*batch_size, " ")			

			current_batch++
		}

		if (current_batch+1)*batch_size != current_batch_max_file {
			fmt.Print(current_batch_max_file, " ")
		}

	} else {
		current_table_partition = *ReadCurrentFile(0, nil, task, rule)
	}

	for current_partition := 0; current_partition < len(current_table_partition); current_partition++ {
		total_row += current_table_partition[current_partition].partition_row
	}

	var result_table CachePartition
	result_table.table_partition = *&current_table_partition
	result_table.table_name = return_table_name
	result_table.total_row = total_row

	return &result_table
}

func CombinePartition(current_batch_partition map[int]map[int]Cache) *map[int]Cache {

	var total_file int
	result_table := make(map[int]Cache)

	/*
		for j := 0; j < len(current_batch_partition); j++ {
			for k := 0; k < len(current_batch_partition[j]); k++ {
				result_table[total_file] = current_batch_partition[j][k]
				total_file++
			}
		}
	*/
	for _, vv := range current_batch_partition {
		for _, v := range vv {
			result_table[total_file] = v
			total_file++
		}
	}

	return &result_table
}

func ReadCurrentBatch(task Task, rule Rule, files []string, batch_size int, start_file int, end_file int) *map[int]map[int]Cache {

	current_batch_partition := make(map[int]map[int]Cache)
	//temp_table_partition := make(map[int]Cache)

	//var total_file int

	if batch_size > 0 {

		var mutex sync.Mutex
		var parallel sync.WaitGroup
		parallel.Add(batch_size)

		for cur_file := start_file; cur_file < end_file; cur_file++ {
			go func(cur_file int) {
				defer parallel.Done()

				temp_table_partition := *ReadCurrentFile(cur_file, files, task, rule)

				mutex.Lock()
				current_batch_partition[cur_file] = *&temp_table_partition
				//total_file++
				mutex.Unlock()

			}(cur_file)
		}
		parallel.Wait()

	} else {

		for cur_file := start_file; cur_file < end_file; cur_file++ {

			//rule.current_file = files[cur_file]
			temp_table_partition := *ReadCurrentFile(cur_file, files, task, rule)
			current_batch_partition[cur_file] = *&temp_table_partition
			//total_file++
		}
	}

	return &current_batch_partition
}

func ReadCurrentFile(cur_file int, files []string, task Task, rule Rule) *map[int]Cache {

	var parameter, setting, source string

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {

		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = setting
		} else if strings.Contains(parameter, "$Return") {
			//return_table_name = setting
		}
	}

	if len(files) > 0 {
		source = files[cur_file]
	}

	var input_folder string = "Input/"

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
		//fmt.Println(source, ",", table_partition[0].partition_row)
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

func ColumnHeader(rule Rule, file *os.File, total_byte int64) (int, []string, map[string]int, int, int64) {

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

func ColumnType(total_column int, column_name []string, start_address int64, rule Rule, file *os.File, total_byte int64) (int64, []string) {

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

func PartitionEndAddress(total_column int, rule Rule, column_header_end_address int64, total_byte int64, file *os.File) *map[int]int64 {

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

func ReadByte(current_partition int, partition_address map[int]int64, file *os.File) *[]byte {

	var buffer = partition_address[current_partition+1] - partition_address[current_partition]

	bytestream := make([]byte, buffer)
	file.ReadAt(bytestream, partition_address[current_partition]+1)

	return &bytestream
}

func CellAddressSingleThread(ds DataStructure, rule Rule, filename string) *Cache {

	read_csv_delimiter := rule.read_csv_delimiter
	var _double_quote_count int = 0

	byte_stream := File2Bytestream(filename)

	var total_column int = 1
	var extra_line_br_char int = 0

	for x := 0; x < len(byte_stream); x++ {
		if byte_stream[x] == byte(read_csv_delimiter) && _double_quote_count%2 == 0 {
			total_column += 1
		} else if byte_stream[x] == 13 || byte_stream[x] == 10 {
			if byte_stream[x+1] == 13 || byte_stream[x+1] == 10 {
				extra_line_br_char += 1
			}
			break
		}
		if byte_stream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var cell_address []uint32
	cell_address = append(cell_address, 0)

	var row int32

	for x := 0; x < len(byte_stream); x++ {
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
		WriteCurrentFile(*table, ds.column_name, current_file)

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

func WriteCurrentFile(source_table Cache, column_name []string, file_number int) {

	var csv_string strings.Builder

	csv_string.WriteString(column_name[0])

	for x := 1; x < len(column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(column_name[x])
	}

	csv_string.WriteString("\r\n")

	if _, err := os.Stat("Output/SplitFile/"); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir("Output/SplitFile/", os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	f, err := os.Create("Output/SplitFile/" + strconv.Itoa(file_number) + ".csv")

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err = f.WriteString(csv_string.String())
	_, err = f.Write(source_table.bytestream)

	if err != nil {
		log.Fatal(err)
	}

}

func StreamPartitionEndAddress(total_column int, rule Rule, column_header_end_address int64, total_byte int64, source_table Cache) *map[int]int64 {

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
			buffer = 500
		}

		for {

			if i < buffer {

				if source_table.bytestream[i] == 13 || source_table.bytestream[i] == 10 {
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
	}

	return &partition_address
}

func StreamCellAddress(estimated_cell int64, is_column_name_exist bool, current_partition int, partition_address map[int]int64, rule Rule, column_name []string, upper_column_name2id map[string]int, data_type []string, total_column int, extra_line_br_char int, source string, source_table Cache) *Cache {

	var buffer = partition_address[current_partition+1] - partition_address[current_partition]

	current_partition_address := make(map[int]int64)
	current_partition_address[0] = -1
	current_partition_address[1] = buffer

	var _double_quote_count int = 0

	cell_address := make([]uint32, 0, estimated_cell)

	cell_address = append(cell_address, uint32(current_partition_address[0])+1)

	var start_byte int = int(current_partition_address[0]) + 1

	var end_byte int = int(current_partition_address[1])
	separator := byte(rule.read_csv_delimiter)
	var row int32

	for x := start_byte; x < end_byte; x++ {
		if source_table.bytestream[x] == separator {
			if _double_quote_count != 1 {
				cell_address = append(cell_address, uint32(x+1))
				_double_quote_count = 0
			}
		} else if source_table.bytestream[x] == 10 {
			cell_address = append(cell_address, uint32(x+1))
			row++
		} else if source_table.bytestream[x] == 13 {
			cell_address = append(cell_address, uint32(x+1))
		} else if source_table.bytestream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var result_table Cache

	result_table.total_column = total_column
	result_table.extra_line_br_char = extra_line_br_char
	result_table.column_name = column_name
	result_table.upper_column_name2id = upper_column_name2id
	result_table.data_type = data_type
	result_table.bytestream = source_table.bytestream
	result_table.partition_row = row
	result_table.cell_address = cell_address

	return &result_table
}

// Write File

func WriteFile(table_partition_store map[string]map[int]Cache, task Task, rule Rule) (string, int32, string) {

	var parameter, setting, source_table_name, source_table_name_upper, return_file_name, error_message string
	source_table_name_upper = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source_table_name_upper = strings.ToUpper(setting)
			source_table_name = setting
		} else if strings.Contains(parameter, "$Column") {
			//column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_file_name = setting
		}
	}

	var csv_string strings.Builder

	if _, found := table_partition_store[source_table_name_upper]; found {
	} else {
		fmt.Println()
		fmt.Println("** Table", source_table_name, "not found **")
		os.Exit(0)
	}

	if _, found := table_partition_store[source_table_name_upper]; found {
		csv_string.WriteString(table_partition_store[source_table_name_upper][0].column_name[0])
	} else {
		fmt.Println()
		fmt.Println("** Table", setting, "not found **")
		os.Exit(0)
	}

	for x := 1; x < len(table_partition_store[source_table_name_upper][0].column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(table_partition_store[source_table_name_upper][0].column_name[x])
	}

	csv_string.WriteString("\r\n")

	var output_folder = "Output/"

	if strings.Contains(return_file_name, "/") || strings.Contains(return_file_name, "\\") {
		output_folder = ""
	}

	if output_folder != "" {
		if _, err := os.Stat(output_folder); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(output_folder, os.ModePerm)
			if err != nil {
				log.Println(err)
			}
		}
	}

	f, err := os.Create(output_folder + return_file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err = f.WriteString(csv_string.String())

	var expand_factor int = 1

	if strings.Contains(return_file_name, "%ExpandBy") {
		if strings.Contains(return_file_name, "Time.csv") {
			expand := strings.Replace(return_file_name, "%ExpandBy", "", 1)
			expand = strings.Replace(expand, "Time.csv", "", 1)
			expand_factor, err = strconv.Atoi(expand)
			if err != nil {
				expand_factor = 1
			}

			if expand_factor < 1 {
				expand_factor = 1
			} else if expand_factor > 100 {
				expand_factor = 100
			}
		}
	}

	for x := 0; x < expand_factor; x++ {

		if expand_factor > 1 {
			fmt.Print(x+1, " ")
		}

		for current_partition := 0; current_partition < len(table_partition_store[source_table_name_upper]); current_partition++ {
			byte_stream := ByteStreamFromTableStore(table_partition_store, source_table_name_upper, current_partition)
			_, err = f.Write(*byte_stream)
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	var total_row int32 = 0

	for current_partition := 0; current_partition < len(table_partition_store[source_table_name_upper]); current_partition++ {
		total_row += table_partition_store[source_table_name_upper][current_partition].partition_row
	}

	return return_file_name, total_row, error_message
}

func ByteStreamFromTableStore(table_partition_store map[string]map[int]Cache, source_table_name string, current_partition int) *[]byte {

	result_table := table_partition_store[source_table_name][current_partition].bytestream
	return &result_table
}

func WriteCurrentStream(table_partition_store map[string]map[int]Cache, task Task, rule Rule) (string, int32, string) {

	var parameter, setting, source_table_name, return_file_name, error_message string
	//var column string
	source_table_name = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source_table_name = strings.ToUpper(setting)
		} else if strings.Contains(parameter, "$Column") {
			//column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_file_name = setting
		}
	}

	var csv_string strings.Builder

	csv_string.WriteString(table_partition_store[source_table_name][0].column_name[0])

	for x := 1; x < len(table_partition_store[source_table_name][0].column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(table_partition_store[source_table_name][0].column_name[x])
	}

	csv_string.WriteString("\r\n")

	if _, err := os.Stat("Output/"); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir("Output/", os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	f, err := os.Create("Output/" + return_file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err = f.WriteString(csv_string.String())

	for x := 0; x < 1; x++ {
		for current_partition := 0; current_partition < len(table_partition_store[source_table_name]); current_partition++ {
			byte_stream := ByteStreamFromTableStore(table_partition_store, source_table_name, current_partition)
			_, err = f.Write(*byte_stream)
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	var total_row int32 = 0

	for current_partition := 0; current_partition < len(table_partition_store[source_table_name]); current_partition++ {
		total_row += table_partition_store[source_table_name][current_partition].partition_row
	}

	return return_file_name, total_row, error_message
}

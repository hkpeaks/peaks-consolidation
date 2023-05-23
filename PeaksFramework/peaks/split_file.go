package peaks

import (
	"errors"
	"log"
	"math"
	"os"
	"strconv"
	"strings"	
)

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

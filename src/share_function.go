package peaks

import (		
	"strings"
	"errors"
	"log"
	"os"
	"strconv"	
)

type Task struct {
	block         string
	command       string
	current_table string
}

type Rule struct {
	thread                     int
	streamMB                   int
	read_csv_delimiter         byte
	write_csv_delimiter        byte
	force_integer              bool
	output_debug               bool
	byte_per_stream            int64
	bytestream                 []byte
	original_rule              []string
	command2rule               map[string]string
	block_sequence             []string
	command_sequence           []string
	parameter_sequence         []string
	block2command_sequence     map[string][]string
	command2parameter_sequence map[string][]string
	block                      map[string]map[string]map[string]string
	current_file               string
	current_query              InternalRule	
	filter2groupby             bool
}

type InternalRule struct {
	source_table_name                     string
	original_source_table_name            string
	return_table_name                     string
	column                                string
	filter_column                         string
	column_name                           []string
	column_id                             map[int]int
	column_id_seq                         []int
	filter_column_id_seq                  []int
	group_by_function                     []string
	join_table_function                   string
	calc_column_name                      []string
	x_column                              string
	y_column                              string
	keyvalue_table_name                   string
	table                                 Cache
	upper_column_name2operator            map[string][]string
	upper_column_name2data_type           map[string][]string
	upper_column_name2compare_value       map[string][]string
	upper_column_name2compare_alt_value   map[string][]string
	upper_column_name2compare_float64     map[string][]float64
	upper_column_name2compare_alt_float64 map[string][]float64
	full_streaming_command                map[string]int
	validate_all_column_name              []string	
}

type Cache struct {
	total_column         int
	total_row            int32
	partition_row        int32
	extra_line_br_char   int
	column_name          []string
	upper_column_name2id map[string]int
	data_type            []string
	bytestream           []byte
	cell_address         []uint32
	keyvalue_table       map[string][]byte
	value_column_name    []string
}

type CachePartition struct {
	error_message   string
	total_row       int32
	table_name      string
	table_partition map[int]Cache
	ir              InternalRule
}

type DataStructure struct {
	total_column         int
	extra_line_br_char   int
	column_name          []string
	upper_column_name2id map[string]int
	data_type            []string
	partition_address    map[int]int64
	estimated_cell       int64
}

func GetPrecision(command string) int {
	var precision = command[(strings.Index(command, ".") + 1):(len(command))]
	int, err := strconv.Atoi(precision)

	if err != nil {
		int = 0
	}
	return int
}

func String2Folder(folder_path string, file_name string, text string) {

	if _, err := os.Stat("Output\\"); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(folder_path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	f, err := os.Create(folder_path + file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	_, err2 := f.WriteString(text)

	defer f.Close()
	_, err2 = f.WriteString(text)

	if err2 != nil {
		log.Fatal(err2)
	}
}

func PartitionAddress(ds DataStructure) *map[int]int64 {
	partition_address := ds.partition_address
	return &partition_address
}

func RemoveCommandIndex(command string) string {
	return command[(strings.Index(command, ":") + 1):(len(command))]
}

func ByteArray2Float64(current_cell []byte) float64 {

	var float_number float64
	var multiply10Pow, divide10Pow, position, offset, current_byte, left_byte int
	var is_dot_exist, is_integer_complete, is_negative, is_invalid_number = false, false, false, false

	var total_byte = len(current_cell)

	for current_byte < total_byte {
		switch current_cell[current_byte] {
		case '.':
			is_dot_exist = true
		case '-':
			current_cell[current_byte] = '0'
			is_negative = true
		case '(':
			current_cell[current_byte] = '0'
			is_negative = true
		case ')':
			current_cell[current_byte] = '0'
			is_negative = true

		default:
			if current_cell[current_byte] < 48 || current_cell[current_byte] > 57 {
				is_invalid_number = true
			}
		}

		if !is_integer_complete {
			if is_dot_exist || (!is_dot_exist && current_byte == total_byte-1) {
				if !is_dot_exist && current_byte == total_byte-1 {
					multiply10Pow++
				}
				offset = 0
				for left_byte < multiply10Pow {
					current_digit := float64(current_cell[left_byte] - 48)
					position = 0
					for position+offset < multiply10Pow-1 {
						current_digit *= 10
						position++
					}
					offset++
					left_byte++
					float_number += current_digit
				}

				is_integer_complete = true
				divide10Pow++

			}
			multiply10Pow++

		} else if is_dot_exist {
			if current_byte == total_byte-1 {
				offset = 0

				for right_byte := total_byte - 1; right_byte >= total_byte-divide10Pow; right_byte-- {
					current_digit := float64(current_cell[right_byte]-48) * 0.1
					position = 0
					for position+offset < divide10Pow-1 {
						current_digit *= 0.1
						position++
					}
					offset++
					float_number += current_digit
				}
			}
			divide10Pow++
		}

		current_byte++
	}

	if is_negative == true {
		float_number *= -1

	}

	if is_invalid_number == true {
		float_number = 0
	}

	return float_number
}

func String2Slice(column string, is_upper bool, rule Rule) []string {

	var column_name []string
	var temp_rule strings.Builder
	column_byte := []byte(column)

	for x := 0; x < len(column_byte); x++ {
		if column_byte[x] == 44 {

			if is_upper == true {
				column_name = append(column_name, strings.ToUpper(strings.TrimSpace(temp_rule.String())))
			} else {
				column_name = append(column_name, strings.TrimSpace(temp_rule.String()))
			}
			temp_rule.Reset()
		} else {
			temp_rule.WriteString(string(column_byte[x]))
		}
	}

	if is_upper == true {
		column_name = append(column_name, strings.ToUpper(strings.TrimSpace(temp_rule.String())))
	} else {
		temp_column := strings.TrimSpace(temp_rule.String())
		column_name = append(column_name, temp_column)
	}

	return column_name
}

func String2Map(column string, is_upper bool, rule Rule) map[string]string {

	amend_column_name := make(map[string]string)
	var current_upper_column_name string
	var temp_rule strings.Builder
	column_byte := []byte(column)

	for x := 0; x < len(column_byte); x++ {
		if column_byte[x] == 61 { // =
			if is_upper == true {
				current_upper_column_name = strings.TrimSpace(strings.ToUpper(temp_rule.String()))
			} else {
				current_upper_column_name = strings.TrimSpace(temp_rule.String())
			}
			temp_rule.Reset()
		} else if column_byte[x] == 44 {
			amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())
			temp_rule.Reset()
		} else {
			temp_rule.WriteString(string(column_byte[x]))
		}
	}

	amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())

	return amend_column_name
}

package main

/*
How to use this app:

    If you set the go.mod where module run

    e.g. run data.csv

         run "d:\your folder\data.csv"

    I have tested this app and found that the minimum number of
    data rows it can handle is 1 (excluding the column name).
    I also tested its ability to process a large file with
    10 billion rows and a size of 41GB. However, I cannot confirm
    the upper limit of rows it can handle.

    How to change number of output sample rows to disk file:

    At the bottom of this code, you can change the default number
    from 100. However, it is not recommended to increase this
    value too much, as the app is designed to retrieve sample rows
    for validation rather than a large population of your dataset.

    csv_info, validate_byte, err := get_csv_info(file_path, 100)

	To see demo please refer to this python version: https://youtu.be/71GHzDnEYno
*/

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type CSV_Info struct {
	total_column        int
	validate_row        int64
	estimate_row        int64
	is_line_br_13_exist bool
	is_line_br_10_exist bool
	column_name         []string
	file_size           int64
	delimiter           byte
}

func get_byte_array_frequency_distribution(byte_array []byte) *map[byte]int {

	frequency_distribution := make(map[byte]int)

	for _, item := range byte_array {
		frequency_distribution[item]++
	}

	return &frequency_distribution
}

func get_current_row_frequency_distribution(filepath string, file *os.File, start_byte int64) (int, *map[byte]int, []byte) {

	frequency_distribution := make(map[byte]int)
	isValidRowExist := false
	var sample_size, double_quote_count int
	var current_row []byte

	for isValidRowExist == false && sample_size < 10000 {

		sample_size += 100

		byte_array := make([]byte, sample_size)
		file.ReadAt(byte_array, start_byte)

		var n int
		current_row = nil
		isFirstLineBreakExist := false
		isSecondLineBreakExist := false

		if start_byte == 0 {
			isFirstLineBreakExist = true
		}

		for n < sample_size && isSecondLineBreakExist == false {

			if isFirstLineBreakExist == false && byte_array[n] == 10 {
				isFirstLineBreakExist = true

			} else if isFirstLineBreakExist == true && byte_array[n] == 10 {
				isSecondLineBreakExist = true
			}

			if isFirstLineBreakExist == true {

				if len(current_row) == 0 && byte_array[n] == 10 {
				} else {

					if byte_array[n] == 34 {
						double_quote_count += 1
					}

					if double_quote_count%2 == 0 {
						current_row = append(current_row, byte_array[n])
					}
				}
			}
			n++
		}

		if isSecondLineBreakExist == true && len(current_row) > 0 {
			frequency_distribution = *get_byte_array_frequency_distribution(current_row)
			isValidRowExist = true
		}
	}

	return len(current_row), &frequency_distribution, current_row
}

func skip_white_space(byte_array []byte, start_byte int64, end_byte int64) (int64, int64) {

	for start_byte < end_byte && byte_array[start_byte] == 32 {
		start_byte++
	}

	for end_byte > start_byte && byte_array[end_byte-1] == 32 {
		end_byte--
	}

	return start_byte, end_byte
}

func get_column_name(filepath string, file *os.File, delimiter byte) *[]string {

	isValidRowExist := false
	var sample_size int64
	var column_name []string

	for isValidRowExist == false {

		var column_count int
		var double_quote_count int
		var cell_address []int64
		column_name = nil

		sample_size += 100

		byte_array := make([]byte, sample_size)
		file.ReadAt(byte_array, 0)

		var n int64

		cell_address = append(cell_address, 0)

		for n < sample_size {

			if byte_array[n] == delimiter {
				if double_quote_count%2 == 0 {
					cell_address = append(cell_address, n+1)
					column_count++
				}
			} else if byte_array[n] == 10 {
				cell_address = append(cell_address, n+1)
				isValidRowExist = true
				column_count++
				break
			} else if byte_array[n] == 13 {
				cell_address = append(cell_address, n+1)
			} else if byte_array[n] == 34 {
				double_quote_count += 1
			}
			n++
		}

		var start_byte, end_byte int64

		for i := 0; i < column_count; i++ {
			start_byte = cell_address[i]
			end_byte = cell_address[i+1] - 1
			start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)
			column_name = append(column_name, string(byte_array[start_byte:end_byte]))
		}
	}

	return &column_name
}

func get_csv_info(filepath string, sample_row int) (CSV_Info, []byte, error) {

	var error_message strings.Builder
	var csv_info CSV_Info

	file, err := os.Open(filepath)
	defer file.Close()

	file2, err := os.Stat(filepath)

	var frequency_distribution *map[byte]int
	var current_row_byte, validate_byte []byte
	var start_byte, n int64
	var current_row_byte_count, sample_byte_count int

	frequency_distribution_by_sample := make(map[int64]map[byte]int)
	delimiter_scenario := make(map[byte]int)

	if err != nil {
		err = errors.New("** File " + "\"" + filepath + "\"" + " not found **")

	} else {

		csv_info.file_size = file2.Size()

		if sample_row <= 0 || csv_info.file_size < 10000 {
			sample_row = 10
		}

		if csv_info.file_size < 1000 || sample_row < 2 {
			sample_row = 2
		}

		// Column Name
		current_row_byte_count, frequency_distribution, current_row_byte = get_current_row_frequency_distribution(filepath, file, 0)
		delimiter_scenario = *frequency_distribution

		// Data Row
		for n <= int64(sample_row)-1 {

			start_byte++
			current_row_byte_count, frequency_distribution, current_row_byte = get_current_row_frequency_distribution(filepath, file, start_byte)
			validate_byte = append(validate_byte, current_row_byte...)
			sample_byte_count += current_row_byte_count
			frequency_distribution_by_sample[n] = *frequency_distribution

			temp_delimiter_scenario := make(map[byte]int)

			for key := range delimiter_scenario {
				if _, found := frequency_distribution_by_sample[n][key]; found {
					if frequency_distribution_by_sample[n][key] == delimiter_scenario[key] {
						temp_delimiter_scenario[key] = delimiter_scenario[key]
					}
				}
			}

			delimiter_scenario = temp_delimiter_scenario

			start_byte = csv_info.file_size * n / int64(sample_row)
			n++
		}

		csv_info.validate_row = n

	}

	delimiter_exclude_line_br := make(map[byte]int)

	for key := range delimiter_scenario {
		if key != 10 && key != 13 {
			delimiter_exclude_line_br[key] = delimiter_scenario[key]
		} else {

			if key == 10 {
				csv_info.is_line_br_10_exist = true
			} else if key == 13 {
				csv_info.is_line_br_13_exist = true
			}

		}
	}

	delimiter_exclude_abc123 := make(map[byte]int)

	if _, found := delimiter_exclude_line_br[44]; found {

		csv_info.delimiter = 44
		csv_info.total_column = delimiter_exclude_line_br[44] + 1

	} else if len(delimiter_exclude_line_br) == 1 {

		for key := range delimiter_exclude_line_br {
			if (key >= 0 && key <= 47) || key >= 58 && key <= 64 || key >= 91 && key <= 96 || key >= 123 {
				csv_info.delimiter = key
				csv_info.total_column = delimiter_exclude_line_br[key] + 1
			}
		}

	} else if len(delimiter_exclude_line_br) > 1 {

		for key := range delimiter_exclude_line_br {
			if (key >= 0 && key <= 47) || key >= 58 && key <= 64 || key >= 91 && key <= 96 || key >= 123 {
				delimiter_exclude_abc123[key] = delimiter_exclude_line_br[key]
			}
		}

		if len(delimiter_exclude_abc123) == 1 {

			for key := range delimiter_exclude_abc123 {
				csv_info.delimiter = key
				csv_info.total_column = delimiter_exclude_line_br[key] + 1
			}

		} else if len(delimiter_exclude_abc123) > 1 {
			error_message.WriteString("** More than one possible delimiter ** \n")
			for key := range delimiter_exclude_abc123 {
				error_message.WriteString("   ASCII" + strconv.Itoa(int(key)) + " (" + string(key) + ")" + "\n")
			}

			err = errors.New(error_message.String())
		}
	}

	if err == nil {
		csv_info.column_name = *get_column_name(filepath, file, csv_info.delimiter)
		csv_info.estimate_row = int64(csv_info.file_size) / int64(sample_byte_count) * int64(sample_row)

		if len(delimiter_exclude_line_br) == 0 {
			error_message.WriteString("** Fail to find delimiter ** \n")
		} else {

			if csv_info.total_column == 0 {
				error_message.WriteString("** Fail to count number of column ** \n")
			}

			if csv_info.estimate_row == 0 {
				error_message.WriteString("** Fail to estimate number of row ** \n")
			}

			if len(csv_info.column_name) == 0 {
				error_message.WriteString("** Fail to find any column name ** \n")
			}

			if len(csv_info.column_name) != csv_info.total_column {
				error_message.WriteString("** Number of column name is ")
				error_message.WriteString(strconv.Itoa(len(csv_info.column_name)))
				error_message.WriteString(", but number of column is ")
				error_message.WriteString(strconv.Itoa(csv_info.total_column))
				error_message.WriteString(" ** \n")
			}
		}

		if len(error_message.String()) > 0 {
			err = errors.New(error_message.String())
		}
	}

	return csv_info, validate_byte, err
}

func number_display_format(num float64) string {

	numStr := strconv.FormatFloat(num, 'f', -1, 64)
	parts := strings.Split(numStr, ".")
	numOfDigits := len(parts[0])
	numOfCommas := (numOfDigits - 1) / 3

	for i := 1; i <= numOfCommas; i++ {
		commaIndex := numOfDigits - i*3
		parts[0] = parts[0][:commaIndex] + "," + parts[0][commaIndex:]
	}

	return strings.Join(parts, ".")
}

func cell_address(byte_array []byte, csv_info CSV_Info) *[]int64 {

	read_csv_delimiter := csv_info.delimiter

	var cell_address []int64
	var double_quote_count int

	cell_address = append(cell_address, 0)
	var row int64

	for i := 0; i < len(byte_array); i++ {
		if byte_array[i] == read_csv_delimiter {
			if double_quote_count != 1 {
				cell_address = append(cell_address, int64(i+1))
				double_quote_count = 0
			}
		} else if byte_array[i] == 10 {
			cell_address = append(cell_address, int64(i+1))
			row++
		} else if byte_array[i] == 13 {
			cell_address = append(cell_address, int64(i+1))
		} else if byte_array[i] == 34 {
			double_quote_count += 1
		}
	}

	return &cell_address
}

func current_view(byte_array []byte, csv_info CSV_Info, start_column int, end_column int, total_row int) {

	cell_address := *cell_address(byte_array, csv_info)

	var extra_line_br_char int
	if csv_info.is_line_br_13_exist == true {
		extra_line_br_char = 1
	}

	var start_byte, end_byte int64
	var max_col int = csv_info.total_column + extra_line_br_char - 1
	var cell uint32 = uint32(len(cell_address) - (max_col + 1))
	//var total_column = source_table[0].total_column
	var n uint32 = 0
	var current_row int32 = 0
	var temp_bytes []byte
	read_csv_delimiter := csv_info.delimiter
	max_text_length := make(map[int]int)
	max_integer_length := make(map[int]int)
	max_decimal_length := make(map[int]int)
	max_column_width := make(map[int]int)
	is_column_real_number := make(map[int]bool)

	for current_column := start_column; current_column < end_column; current_column++ {

		max_text_length[current_column] = 1
		max_column_width[current_column] = len(csv_info.column_name[current_column])
		is_column_real_number[current_column] = true
	}

	for n < cell {

		for current_column := start_column; current_column < end_column; current_column++ {

			if current_column > 0 {
				temp_bytes = append(temp_bytes, read_csv_delimiter)
			}

			current_cell := (int32(max_col)+1)*current_row + int32(current_column)
			start_byte = cell_address[current_cell]
			end_byte = cell_address[current_cell+1] - 1
			start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

			is_real_number := true
			is_float := false
			is_negative_sign_exist := false
			is_open_bracket := false
			is_close_bracket := false
			integer_length := 0
			decimal_length := 0
			is_dot_exist := false
			text_length := 0

			for current_byte := start_byte; current_byte < end_byte; current_byte++ {

				switch byte_array[current_byte] {
				case '.':
					if is_float == true { // exist more than one time
						is_float = false
						is_real_number = false
					}
					is_float = true
					if current_byte == start_byte || current_byte == end_byte-1 {
						is_real_number = false
					}
				case '-':
					if is_negative_sign_exist == true {
						is_negative_sign_exist = false
						is_real_number = false
					}
					if current_byte == start_byte {
						is_negative_sign_exist = true
					} else {
						is_real_number = false
					}
				case '(':
					if current_byte == start_byte {
						is_open_bracket = true
					} else {
						is_real_number = false
					}

				case ')':
					if current_byte == end_byte-1 {
						is_close_bracket = true
					} else {
						is_real_number = false
					}

				default:
					if byte_array[current_byte] < 48 || byte_array[current_byte] > 57 {
						is_real_number = false
					}
				}
			}

			if is_negative_sign_exist == true {
				is_real_number = true
			}

			if is_open_bracket == true && is_close_bracket == true {
				is_real_number = true
			}

			if is_real_number == true {

				for current_byte := start_byte; current_byte < end_byte; current_byte++ {

					if byte_array[current_byte] == 46 {
						is_dot_exist = true
					} else {
						if is_dot_exist == true {
							decimal_length++
						} else {
							integer_length++
						}
					}
				}

				if integer_length > max_integer_length[current_column] {
					max_integer_length[current_column] = integer_length
				}

				if decimal_length > max_decimal_length[current_column] {
					max_decimal_length[current_column] = decimal_length
				}

			}

			is_column_real_number[current_column] = is_column_real_number[current_column] && is_real_number

			text_length = int(end_byte - start_byte)

			if text_length > max_text_length[current_column] {
				max_text_length[current_column] = text_length
			}

			temp_bytes = append(temp_bytes, byte_array[start_byte:end_byte]...)
		}

		n += uint32(max_col) + 1
		current_row++
		if current_row >= int32(total_row) {
			break
		}
		temp_bytes = nil

	}

	for current_column := start_column; current_column < end_column; current_column++ {

		current_length := max_integer_length[current_column] + max_decimal_length[current_column] + 1

		if is_column_real_number[current_column] == true {

			if max_column_width[current_column] < current_length {
				max_column_width[current_column] = current_length
			}
		} else {

			if max_column_width[current_column] < max_text_length[current_column] {
				max_column_width[current_column] = max_text_length[current_column]
			}
		}
	}

	var result_bytes []byte

	result_bytes = append(result_bytes, 32)
	result_bytes = append(result_bytes, 32)

	for current_column := start_column; current_column < end_column; current_column++ { // Display Column Name

		current_columm_name := csv_info.column_name[current_column]
		current_length := len(current_columm_name)

		if is_column_real_number[current_column] == true {

			if max_column_width[current_column] > current_length {

				for i := 0; i < max_column_width[current_column]-current_length; i++ {
					result_bytes = append(result_bytes, 32)
				}
			}

			result_bytes = append(result_bytes, []byte(current_columm_name)...)

			result_bytes = append(result_bytes, 32)
			result_bytes = append(result_bytes, 32)

		} else {

			result_bytes = append(result_bytes, []byte(current_columm_name)...)

			if max_column_width[current_column] > current_length {

				for i := 0; i < max_column_width[current_column]-current_length; i++ {
					result_bytes = append(result_bytes, 32)
				}
			}

			result_bytes = append(result_bytes, 32)
			result_bytes = append(result_bytes, 32)
		}

	}

	result_bytes = append(result_bytes, 13)
	result_bytes = append(result_bytes, 10)

	n = 0
	current_row = 0

	for n < cell { // Display body

		result_bytes = append(result_bytes, 32)
		result_bytes = append(result_bytes, 32)

		for current_column := start_column; current_column < end_column; current_column++ {

			current_cell := (int32(max_col)+1)*current_row + int32(current_column)
			start_byte = cell_address[current_cell]
			end_byte = cell_address[current_cell+1] - 1
			start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

			var current_length int

			if is_column_real_number[current_column] == true {

				if max_decimal_length[current_column] == 0 { // Integer only

					current_length = int(end_byte - start_byte)

					if max_column_width[current_column] > current_length {

						for i := 0; i < max_column_width[current_column]-current_length; i++ {
							result_bytes = append(result_bytes, 32)
						}
					}

					for current_byte := start_byte; current_byte < end_byte; current_byte++ {
						result_bytes = append(result_bytes, byte_array[current_byte])

					}

					result_bytes = append(result_bytes, 32)
					result_bytes = append(result_bytes, 32)

				} else { // Decemial only

					current_length = int(end_byte - start_byte)
					current_integer_length := 0

					for current_byte := start_byte; current_byte < end_byte; current_byte++ {

						if byte_array[current_byte] == 46 {
							break
						}

						current_integer_length++
					}

					left_space := max_column_width[current_column] - max_integer_length[current_column] - max_decimal_length[current_column] - 1

					for i := 0; i < left_space; i++ {
						result_bytes = append(result_bytes, 32)
					}

					integer_space := 0

					if max_integer_length[current_column] > current_integer_length {

						for i := 0; i < max_integer_length[current_column]-current_integer_length; i++ {
							result_bytes = append(result_bytes, 32)
							integer_space++
						}
					}

					for current_byte := start_byte; current_byte < end_byte; current_byte++ {
						result_bytes = append(result_bytes, byte_array[current_byte])
					}

					right_space := max_column_width[current_column] - current_length - integer_space - left_space

					if max_column_width[current_column] > current_length {
						for i := 0; i < right_space; i++ {
							result_bytes = append(result_bytes, 32)
						}
					}

					result_bytes = append(result_bytes, 32)
					result_bytes = append(result_bytes, 32)

				}

			} else { // Text only

				current_length = int(end_byte - start_byte)

				for current_byte := start_byte; current_byte < end_byte; current_byte++ {
					result_bytes = append(result_bytes, byte_array[current_byte])
				}

				if max_column_width[current_column] > current_length {

					for i := 0; i < max_column_width[current_column]-current_length; i++ {
						result_bytes = append(result_bytes, 32)
					}
				}

				result_bytes = append(result_bytes, 32)
				result_bytes = append(result_bytes, 32)
			}
		}

		result_bytes = append(result_bytes, 13)
		result_bytes = append(result_bytes, 10)

		n += uint32(max_col) + 1
		current_row++
		if current_row >= int32(total_row) {
			break
		}
	}

	fmt.Println(string(result_bytes))
}

func max_column_width(byte_array []byte, csv_info CSV_Info) (bool, map[int]int) {

	cell_address := *cell_address(byte_array, csv_info)

	var is_zero_row bool

	if len(byte_array) == 0 {
		is_zero_row = true
	} else {
		is_zero_row = false
	}

	var start_byte, end_byte int64

	var extra_line_br_char int

	if csv_info.is_line_br_13_exist {
		extra_line_br_char = 1
	}

	var max_col int = csv_info.total_column + extra_line_br_char - 1
	var cell uint32 = uint32(len(cell_address) - (max_col + 1))
	var total_column = csv_info.total_column
	var n uint32 = 0
	var current_row int32 = 0
	var temp_bytes []byte
	read_csv_delimiter := csv_info.delimiter
	max_text_length := make(map[int]int)
	max_integer_length := make(map[int]int)
	max_decimal_length := make(map[int]int)
	max_column_width := make(map[int]int)
	is_column_real_number := make(map[int]bool)

	for current_column := 0; current_column < total_column; current_column++ {

		max_column_width[current_column] = len(csv_info.column_name[current_column])
		is_column_real_number[current_column] = true
	}

	if is_zero_row == false {

		for n < cell {

			for current_column := 0; current_column < total_column; current_column++ {

				if current_column > 0 {
					temp_bytes = append(temp_bytes, read_csv_delimiter)
				}

				current_cell := (int32(max_col)+1)*current_row + int32(current_column)
				start_byte = cell_address[current_cell]
				end_byte = cell_address[current_cell+1] - 1
				start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

				is_real_number := true
				is_float := false
				is_negative_sign_exist := false
				is_open_bracket := false
				is_close_bracket := false
				integer_length := 0
				decimal_length := 0
				is_dot_exist := false
				text_length := 0

				for current_byte := start_byte; current_byte < end_byte; current_byte++ {

					switch byte_array[current_byte] {
					case '.':
						if is_float == true { // exist more than one time
							is_float = false
							is_real_number = false
						}
						is_float = true
						if current_byte == start_byte || current_byte == end_byte-1 {
							is_real_number = false
						}
					case '-':
						if is_negative_sign_exist == true {
							is_negative_sign_exist = false
							is_real_number = false
						}
						if current_byte == start_byte {
							is_negative_sign_exist = true
						} else {
							is_real_number = false
						}
					case '(':
						if current_byte == start_byte {
							is_open_bracket = true
						} else {
							is_real_number = false
						}

					case ')':
						if current_byte == end_byte-1 {
							is_close_bracket = true
						} else {
							is_real_number = false
						}

					default:
						if byte_array[current_byte] < 48 || byte_array[current_byte] > 57 {
							is_real_number = false
						}
					}
				}

				if is_negative_sign_exist == true {
					is_real_number = true
				}

				if is_open_bracket == true && is_close_bracket == true {
					is_real_number = true
				}

				if is_real_number == true {

					for current_byte := start_byte; current_byte < end_byte; current_byte++ {

						if byte_array[current_byte] == 46 {
							is_dot_exist = true
						} else {
							if is_dot_exist == true {
								decimal_length++
							} else {
								integer_length++
							}
						}
					}

					if integer_length > max_integer_length[current_column] {
						max_integer_length[current_column] = integer_length
					}

					if decimal_length > max_decimal_length[current_column] {
						max_decimal_length[current_column] = decimal_length
					}

				}

				is_column_real_number[current_column] = is_column_real_number[current_column] && is_real_number

				text_length = int(end_byte - start_byte)

				if text_length > max_text_length[current_column] {
					max_text_length[current_column] = text_length
				}

				temp_bytes = append(temp_bytes, byte_array[start_byte:end_byte]...)
			}

			n += uint32(max_col) + 1
			current_row++
			if current_row > 10 {
				break
			}
			temp_bytes = nil

		}
	}

	for current_column := 0; current_column < total_column; current_column++ {

		current_length := max_integer_length[current_column] + max_decimal_length[current_column] + 1

		if is_column_real_number[current_column] == true {

			if max_column_width[current_column] < current_length {
				max_column_width[current_column] = current_length
			}
		} else {

			if max_column_width[current_column] < max_text_length[current_column] {
				max_column_width[current_column] = max_text_length[current_column]
			}
		}
	}
	return is_zero_row, max_column_width
}

func view(byte_array []byte, csv_info CSV_Info) {

	is_zero_row, max_column_width := max_column_width(byte_array, csv_info)

	if is_zero_row == false {

		var total_width, current_width int

		for i := 0; i < len(max_column_width); i++ {

			total_width += max_column_width[i]
		}

		total_row := 20
		table_count := 1

		fmt.Println()

		var current_column int

		start_column := 0

		if total_width > 150 {

			total_row = 8

			for current_column < len(max_column_width) {

				current_width += max_column_width[current_column]
				if current_width > 100*table_count {

					current_view(byte_array, csv_info, start_column, current_column, total_row)
					start_column = current_column
					table_count++
				}

				current_column++
			}

			current_view(byte_array, csv_info, start_column, len(max_column_width), total_row)

		} else if total_width > 100 {

			total_row = 8

			for current_column < len(max_column_width) {

				current_width += max_column_width[current_column]
				if current_width > total_width/2 {

					current_view(byte_array, csv_info, 0, current_column, total_row)
					start_column = current_column
					break
				}
				current_column++
			}

			current_view(byte_array, csv_info, start_column, len(max_column_width), total_row)

		} else {
			current_view(byte_array, csv_info, 0, len(max_column_width), total_row)
		}

	}
}

func write_csv_sample_file(byte_array []byte, csv_info CSV_Info) {

	var csv_string strings.Builder

	csv_string.WriteString(csv_info.column_name[0])

	for i := 1; i < len(csv_info.column_name); i++ {
		csv_string.WriteString(",")
		csv_string.WriteString(csv_info.column_name[i])
	}

	csv_string.WriteString("\r\n")

	f, err := os.Create("%Sample.csv")

	defer f.Close()

	if err != nil {
		log.Fatal(err)
	} else {
		_, err = f.WriteString(csv_string.String())
		_, err = f.Write(byte_array)
		fmt.Println("A file named %Sample.csv is created from the rows that executed validation.")
		fmt.Println()

	}

	if err != nil {
		log.Fatal(err)
		fmt.Println("*** Fail to write file ***")
	}
}

func main() {

	start := time.Now()

	var file_path string

	if len(os.Args) == 1 {
		file_path = "Fact.csv"
	} else if len(os.Args) == 2 {
		file_path = os.Args[1]
	}

	var validate_byte []byte

	csv_info, validate_byte, err := get_csv_info(file_path, 100)

	if err != nil {
		fmt.Println()
		fmt.Println(err)
	} else {

		view(validate_byte, csv_info)
		write_csv_sample_file(validate_byte, csv_info)

		fmt.Print("File Size: " + number_display_format(float64(csv_info.file_size)) + " bytes")
		fmt.Println("  Total Column: ", number_display_format(float64(csv_info.total_column)))
		fmt.Print("Validated Row: ", number_display_format(float64(csv_info.validate_row)))
		fmt.Println("  Estimated Row: ", number_display_format(float64(csv_info.estimate_row)))

		fmt.Print("Column Name: ")

		for i := 0; i < len(csv_info.column_name); i++ {

			if i < len(csv_info.column_name)-1 {
				fmt.Print(csv_info.column_name[i], ",")
			} else {
				fmt.Println(csv_info.column_name[i])
			}
		}

		if csv_info.delimiter == 0 {
			fmt.Println("Delimiter: ")
		} else {
			fmt.Println("Delimiter: " + number_display_format(float64(csv_info.delimiter)) + " [" + string(csv_info.delimiter) + "]")
		}

		fmt.Println("Is Line Br 10/13 Exist: ", csv_info.is_line_br_10_exist, "/", csv_info.is_line_br_13_exist)

	}

	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println()

	if elapsed.Seconds() <= 1 {
		fmt.Println("Duration:", math.Round((elapsed.Seconds())*1000)/1000, "second")
	} else {
		fmt.Println("Duration:", math.Round((elapsed.Seconds())*1000)/1000, "seconds")
	}
}

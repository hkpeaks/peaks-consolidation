package peaks

import (	
	"strconv"
	"strings"
	"errors"	
	"log"	
	"os"
)

func CombineTablePartition(source_table map[int]Cache) *Cache {
	result_table := source_table[0]
	return &result_table
}

func PartitionAddress(ds DataStructure) *map[int]int64 {
	partition_address := ds.partition_address
	return &partition_address
}

func SelectCurrentTableFirstPartition(table_partition_store map[string]map[int]Cache, ir InternalRule) *Cache {
	var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][0]
	return &result_table
}

func SelectCurrentTableEachPartition(table_partition_store map[string]map[int]Cache, ir InternalRule, current_partition int) *Cache {
	var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][current_partition]
	return &result_table
}

func SelectCurrentTableEachPartitionByteStream(table_partition_store map[string]map[int]Cache, ir InternalRule, current_partition int) *[]byte {
	var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][current_partition]
	var bytestream = result_table.bytestream
	return &bytestream
}

func SelectCurrentTableByName(table_partition_store map[string]map[int]Cache, source_table_name string) *Cache {
	var result_table = table_partition_store[strings.ToUpper(source_table_name)][0]
	return &result_table
}

func SelectCurrentTable(table_partition_store map[string]map[int]Cache, source_table_name string) *map[int]Cache {

	result_table := make(map[int]Cache)

	for p := 0; p < len(table_partition_store[strings.ToUpper(source_table_name)]); p++ {
		result_table[p] = table_partition_store[strings.ToUpper(source_table_name)][p]
	}

	return &result_table
}

func SelectCurrentPartition(source_table_partition map[int]Cache, current_partition int) *Cache {
	result_table := source_table_partition[current_partition]
	return &result_table
}

func CurrentBytestream(source_table Cache) *[]byte {
	return &source_table.bytestream
}

func CurrentCellAddress(source_table Cache) *[]uint32 {
	return &source_table.cell_address
}

func GetParameter(command string) string {
	var parameter string
	if strings.Contains(command, "[") {
		parameter = command[0:strings.Index(command, "[")]
	} else if strings.Contains(command, "(") {
		parameter = command[0:strings.Index(command, "(")]
	}
	parameter = strings.ReplaceAll(parameter, "{", "")
	return parameter
}

func GetPrecision(command string) int {
	var precision = command[(strings.Index(command, ".") + 1):(len(command))]
	int, err := strconv.Atoi(precision)

	if err != nil {
		int = 0
	}
	return int
}

func RemoveCommandIndex(command string) string {
	return command[(strings.Index(command, ":") + 1):(len(command))]
}

func String2File(file_name string, text string) {

	f, err := os.Create("Output/" + file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	_, err2 := f.WriteString(text)

	if err2 != nil {
		log.Fatal(err2)
	}
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

func File2Bytestream(filename string) []byte {

	bytestream, err := os.ReadFile(filename)

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	return bytestream
}


func ByteArray2Float64(current_cell []byte) float64 {

	var float_number float64	
	var multiply10Pow, divide10Pow, position, offset, current_byte, left_byte int	
	var is_dot_exist, is_integer_complete, is_negative, is_invalid_number  = false, false, false, false
	
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
					current_digit := float64(current_cell[right_byte] - 48) * 0.1
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

func ByteArray2Int(current_cell []byte) int {

	var integer_number int
	var multiply10Pow, position, offset, current_byte, left_byte int	
	var is_dot_exist, is_integer_complete, is_negative, is_invalid_number  = false, false, false, false
	
	var total_byte = len(current_cell)

	for current_byte < total_byte {
		switch current_cell[current_byte] {
		case '.':
			is_dot_exist = true
		case '-':
			current_cell[current_byte] = '0'
			is_negative = true
		case '(':
			fallthrough
		case ')':
			fallthrough
		
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
					current_digit := int(current_cell[left_byte] - 48)
					position = 0
					for position+offset < multiply10Pow-1 {
						current_digit *= 10
						position++
					}
					offset++
					left_byte++
					integer_number += current_digit
				}

				is_integer_complete = true
				
				
			}
			multiply10Pow++		
		}		

		current_byte++
	}

	if is_negative == true {
		integer_number *= -1
	}

	if is_invalid_number == true {
		integer_number = 0
	}
	return integer_number
}





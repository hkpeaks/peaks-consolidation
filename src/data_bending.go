package peaks

import (
	"fmt"	
	"strings"	
	"sync"		
	"os"
	"regexp"

)

func CurrentCommand(batch int, result_table_partition map[int]Cache, current_partition int, total_partition int, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

	var result_table Cache

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "JOINKEYVALUE" {

		if total_partition == 1 {
			result_table = *JoinKeyValue(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECT" || strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECTUNMATCH" {

		if total_partition == 1 {
			result_table = *CurrentSelect(result_table_partition, current_partition, total_partition, ir, rule, task, upper_column_name2id, data_type)
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "BUILDKEYVALUE" {

		if batch == 0 && total_partition == 1 {
			result_table = *BuildKeyValue(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		} else {

			result_keyvalue_table := make(map[string][]byte)
			var source_table Cache

			for p := 0; p < total_partition; p++ {

				source_table = *SelectCurrentPartition(result_table_partition, p)
				for key, value := range source_table.keyvalue_table {

					if _, found := result_keyvalue_table[key]; found {
						result_keyvalue_table[key] = append(result_keyvalue_table[key], value...)
					} else {
						result_keyvalue_table[key] = value
					}
				}
			}

			result_table.total_column = result_table_partition[0].total_column
			result_table.extra_line_br_char = result_table_partition[0].extra_line_br_char
			result_table.column_name = result_table_partition[0].column_name
			result_table.upper_column_name2id = result_table_partition[0].upper_column_name2id
			result_table.data_type = result_table_partition[0].data_type
			result_table.bytestream = nil
			result_table.partition_row = int32(len(result_keyvalue_table))
			result_table.cell_address = result_table_partition[0].cell_address
			result_table.keyvalue_table = result_keyvalue_table
			result_table.value_column_name = source_table.value_column_name
		}

	} else {
		if rule.force_integer == true {
			result_table = *CurrentDistinctGroupByInteger(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		} else {
			result_table = *CurrentDistinctGroupByFloat64(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		}

	}

	return &result_table
}

func CurrentRule(table_partition_store map[string]map[int]Cache, task Task, rule Rule) InternalRule {

	var ir InternalRule

	full_streaming_command := make(map[string]int)

	full_streaming_command["JOINKEYVALUE"] = 0
	full_streaming_command["SELECT"] = 1
	full_streaming_command["SELECTUNMATCH"] = 2

	parameter2setting := make(map[string]string)
	var parameter, setting string
	ir.source_table_name = strings.ToUpper(task.current_table)
	ir.original_source_table_name = task.current_table
	ir.return_table_name = task.current_table

	var combine_column_name strings.Builder
	var column_name string
	var filter_column_name []string
	upper_column_name2setting := make(map[string]string)
	upper_column_name2operator := make(map[string][]string)
	upper_column_name2data_type := make(map[string][]string)
	upper_column_name2compare_value := make(map[string][]string)
	upper_column_name2compare_alt_value := make(map[string][]string)
	upper_column_name2compare_float64 := make(map[string][]float64)
	upper_column_name2compare_alt_float64 := make(map[string][]float64)

	var n int

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			ir.source_table_name = strings.ToUpper(setting)
			ir.original_source_table_name = setting
			ir.return_table_name = strings.ToUpper(ir.source_table_name)

			if len(setting) > 0 {
				if !strings.Contains(strings.ToUpper(setting), ".CSV") {
					if _, found := table_partition_store[strings.ToUpper(setting)]; !found {
						fmt.Println("** Table", setting, "not found **")
						os.Exit(0)
					}
				}
			}

		} else if strings.Contains(parameter, "$Column") {
			ir.column = setting

		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "ALLMATCH($SETTING)") {
			ir.keyvalue_table_name = setting
			ir.join_table_function = "ALLMATCH"
			ir.table = *SelectCurrentTableByName(table_partition_store, ir.keyvalue_table_name)

		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "COUNT($SETTING)") {
			ir.calc_column_name = append(ir.calc_column_name, "Null")
			ir.group_by_function = append(ir.group_by_function, "Count")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "SUM($SETTING)") {
			ir.calc_column_name = append(ir.calc_column_name, setting)
			ir.group_by_function = append(ir.group_by_function, "Sum")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "MAX($SETTING)") {
			ir.calc_column_name = append(ir.calc_column_name, setting)
			ir.group_by_function = append(ir.group_by_function, "Max")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "MIN($SETTING)") {
			ir.calc_column_name = append(ir.calc_column_name, setting)
			ir.group_by_function = append(ir.group_by_function, "Min")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "X($SETTING)") {
			ir.x_column = setting
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter, "~", "")), "Y($SETTING)") {
			ir.y_column = setting
		} else if strings.Contains(parameter, "$Return") {
			ir.return_table_name = setting
		} else if strings.Contains(strings.ToUpper(parameter), "($SETTING)") {
			parameter2setting[strings.ToUpper(GetParameter(parameter))] = setting
		}

		if strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECT" || strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECTUNMATCH" {

			ir.calc_column_name = nil
			ir.group_by_function = nil

			if strings.Contains(parameter, "$Return") {
				column_name = ""
			} else if strings.Contains(parameter, "$Source") {
				column_name = ""
			} else {
				column_name = strings.Replace(parameter, "($Setting)", "", 1)
				column_name = strings.Trim(strings.Replace(column_name, "}", "", 1), " ")
				column_name = strings.Trim(strings.Replace(column_name, "{", "", 1), " ")
				column_name = strings.Trim(strings.Replace(column_name, "=>", "", 1), " ")

				upper_column_name2setting[strings.ToUpper(column_name)] = setting

				filter_column_name = append(filter_column_name, column_name)

				if n == 0 {
					combine_column_name.WriteString(column_name)
				} else {
					combine_column_name.WriteString(",")
					combine_column_name.WriteString(column_name)
				}
				n++

			}
		}
	}

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECT" || strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECTUNMATCH" {
		var start_value, end_value string
		var current_data_type string

		for column_name, setting := range upper_column_name2setting {

			col := strings.ToUpper(column_name)
			setting := String2Slice(setting, false, rule)

			for i := 0; i < len(setting); i++ {

				if strings.Contains(strings.ToUpper(setting[i]), "FLOAT") {
					current_data_type = "Float"
					remove := regexp.MustCompile(`(?i)float`)
					setting[i] = remove.ReplaceAllString(setting[i], "")
				} else {
					current_data_type = "Text"
				}

				if strings.Contains(setting[i], "..") {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "..")
					start_end := String2Slice(strings.Replace(setting[i], "..", ",", 1), false, rule)
					start_value = start_end[0]
					end_value = start_end[1]

					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(start_value)))
						upper_column_name2compare_alt_float64[col] = append(upper_column_name2compare_alt_float64[col], ByteArray2Float64([]byte(end_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], start_value)
						upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], end_value)
					}

				} else if setting[i][0:2] == ">=" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], ">=")
					compare_value := strings.Replace(setting[i], ">=", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:2] == "<=" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "<=")
					compare_value := strings.Replace(setting[i], "<=", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:2] == "==" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "==")
					compare_value := strings.Replace(setting[i], "==", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:2] == "!=" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "!=")
					compare_value := strings.Replace(setting[i], "!=", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:1] == "<" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "<")
					compare_value := strings.Replace(setting[i], "<", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:1] == ">" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], ">")
					compare_value := strings.Replace(setting[i], ">", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")

				} else if setting[i][0:1] == "=" {
					upper_column_name2operator[col] = append(upper_column_name2operator[col], "=")
					compare_value := strings.Replace(setting[i], "=", "", 3)
					upper_column_name2data_type[col] = append(upper_column_name2data_type[col], current_data_type)

					if current_data_type == "Float" {
						upper_column_name2compare_float64[col] = append(upper_column_name2compare_float64[col], ByteArray2Float64([]byte(compare_value)))
					} else {
						upper_column_name2compare_value[col] = append(upper_column_name2compare_value[col], compare_value)
					}

					upper_column_name2compare_alt_value[col] = append(upper_column_name2compare_alt_value[col], "$end_value")
				}
			}
		}

		ir.filter_column = strings.Replace(combine_column_name.String(), "$Column,", "", 1)
		ir.filter_column = strings.Replace(ir.filter_column, "$Column", "", 1)
		ir.upper_column_name2operator = upper_column_name2operator
		ir.upper_column_name2compare_value = upper_column_name2compare_value
		ir.upper_column_name2compare_alt_value = upper_column_name2compare_alt_value
		ir.upper_column_name2compare_float64 = upper_column_name2compare_float64
		ir.upper_column_name2compare_alt_float64 = upper_column_name2compare_alt_float64
		ir.upper_column_name2data_type = upper_column_name2data_type
	}

	ir.full_streaming_command = full_streaming_command

	ircolumn := String2Slice(ir.column, false, rule)

	var all_column_name []string

	all_column_name = append(all_column_name, column_name)

	for _, column := range filter_column_name {
		all_column_name = append(all_column_name, column)
	}

	for _, column := range ir.calc_column_name {
		all_column_name = append(all_column_name, column)
	}

	for _, column := range ircolumn {
		all_column_name = append(all_column_name, column)
	}

	if !strings.Contains(strings.ToUpper(ir.source_table_name), ".CSV") {
		for _, column := range all_column_name {

			if _, found := table_partition_store[strings.ToUpper(ir.source_table_name)][0].upper_column_name2id[strings.ToUpper(column)]; !found {

				if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
					if strings.ToUpper(column) != "$COLUMN" {
						fmt.Println("** Column", column, "not found **")
						os.Exit(0)
					}
				}
			}
		}
	}

	ir.validate_all_column_name = all_column_name

	return ir
}

func CurrentStream(batch int, table_partition_store *map[string]map[int]Cache, task Task, rule Rule, ir InternalRule) *CachePartition {

	result_table_partition := make(map[int]Cache)
	result_table_partition_store := make(map[string]map[int]Cache)

	filter_column_id_seq, column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq(table_partition_store, rule, ir)

	ir.filter_column_id_seq = filter_column_id_seq
	ir.column_id_seq = column_id_seq
	ir.column_name = column_name

	temp_table := *ParallelDistinct(batch, *table_partition_store, task, ir, rule, upper_column_name2id, data_type)

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

func ParallelDistinct(batch int, table_partition_store map[string]map[int]Cache, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *map[int]Cache {

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

func JoinKeyValue(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {	

	// This fucntion is reserved for proprietary
	var result_table Cache

	result_table = result_table_partition[0]

	return &result_table
}

func BuildKeyValue(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

	// This fucntion is reserved for proprietary
	var result_table Cache

	result_table = result_table_partition[0]

	return &result_table
}

func CurrentSelect(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, task Task, upper_column_name2id map[string]int, data_type []string) *Cache {

	// This fucntion is reserved for proprietary
	var result_table Cache

	result_table = result_table_partition[0]

	return &result_table
}

func CurrentDistinctGroupByFloat64(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

	// This fucntion is reserved for proprietary
	var result_table Cache

	result_table = result_table_partition[0]

	return &result_table
}

func CurrentDistinctGroupByInteger(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

	// This fucntion is reserved for proprietary
	var result_table Cache

	result_table = result_table_partition[0]

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

func AddColumnmNameWithSeq(table_partition_store *map[string]map[int]Cache, rule Rule, ir InternalRule) ([]int, []int, []string, map[string]int, []string) {

	source_table := SelectCurrentTableFirstPartition(*table_partition_store, ir)
	var column_name = String2Slice(ir.column, false, rule)
	var alt_column_name = String2Slice(ir.column, false, rule)

	var data_type []string
	upper_column_name2id := make(map[string]int)
	var column_id_seq []int

	if len(ir.column) > 0 {

		for x := 0; x < len(ir.group_by_function); x++ {
			if strings.ToUpper(ir.calc_column_name[x]) == "NULL" {

				column_name = append(column_name, "Count()")

				alt_column_name = append(alt_column_name, "Count()")
			} else {
				column_name = append(column_name, ir.calc_column_name[x])
				alt_column_name = append(alt_column_name, ir.group_by_function[x]+"("+ir.calc_column_name[x]+")")
			}
		}

		for x := 0; x < len(column_name); x++ {

			if strings.ToUpper(column_name[x]) == "COUNT()" {
				column_id_seq = append(column_id_seq, -1)
				data_type = append(data_type, "Text")
				upper_column_name2id["COUNT()"] = -1

			} else {
				column_id_seq = append(column_id_seq, source_table.upper_column_name2id[strings.ToUpper(column_name[x])])
				data_type = append(data_type, "Text")
				upper_column_name2id[strings.ToUpper(alt_column_name[x])] = x
			}
		}
	}

	var filter_column_id_seq []int

	if len(ir.filter_column) > 0 {

		var filter_column_name = String2Slice(ir.filter_column, false, rule)

		for x := 0; x < len(filter_column_name); x++ {
			filter_column_id_seq = append(filter_column_id_seq, source_table.upper_column_name2id[strings.ToUpper(filter_column_name[x])])
		}
	}

	return filter_column_id_seq, column_id_seq, alt_column_name, upper_column_name2id, data_type
}

func AddColumnmNameWithSeq2(table_partition_store *map[string]map[int]Cache, rule Rule, ir InternalRule) ([]int, []string, map[string]int, []string) {

	source_table := SelectCurrentTableFirstPartition(*table_partition_store, ir)

	var data_type []string
	upper_column_name2id := make(map[string]int)
	var column_id_seq []int

	for x := 0; x < len(ir.column_name); x++ {

		column_id_seq = append(column_id_seq, source_table.upper_column_name2id[strings.ToUpper(ir.column_name[x])])
		data_type = append(data_type, "Text")
		upper_column_name2id[strings.ToUpper(ir.column_name[x])] = x
	}

	return column_id_seq, ir.column_name, upper_column_name2id, data_type
}

func ParallelAddCellAddressToTablePartition(table_partition_store map[string]map[int]Cache, ir InternalRule, rule Rule) *map[string]map[int]Cache {

	result_table_partition := make(map[int]Cache)

	if len(table_partition_store[ir.source_table_name]) > 1 {

		var mutex sync.Mutex
		var parallel sync.WaitGroup
		p := len(table_partition_store[ir.source_table_name])
		parallel.Add(p)

		for current_partition := 0; current_partition < p; current_partition++ {

			go func(current_partition int) {
				defer parallel.Done()
				result_table := *CurrentAddCellAddressToTablePartition(table_partition_store, ir, rule, current_partition)
				mutex.Lock()
				result_table_partition[current_partition] = *&result_table
				mutex.Unlock()

			}(current_partition)
		}

		parallel.Wait()

	} else {

		for current_partition := 0; current_partition < len(table_partition_store[ir.source_table_name]); current_partition++ {
			result_table := *CurrentAddCellAddressToTablePartition(table_partition_store, ir, rule, current_partition)
			result_table_partition[current_partition] = *&result_table
		}

	}

	table_partition_store[ir.source_table_name] = result_table_partition

	return &table_partition_store
}

func CurrentAddCellAddressToTablePartition(table_partition_store map[string]map[int]Cache, ir InternalRule, rule Rule, current_partition int) *Cache {

	read_csv_delimiter := rule.read_csv_delimiter
	source_table := *SelectCurrentTableEachPartition(table_partition_store, ir, current_partition)

	var result_table Cache

	if len(table_partition_store[ir.source_table_name][current_partition].cell_address) <= 2 {

		var cell_address []uint32
		var bytestream []byte
		var double_quote_count int

		var row int

		bytestream = *SelectCurrentTableEachPartitionByteStream(table_partition_store, ir, current_partition)

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
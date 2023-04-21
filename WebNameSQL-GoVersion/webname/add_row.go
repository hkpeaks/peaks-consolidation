package webname

import (
	"strconv"
	"strings"
	"sync"
)

func AddRow(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) (WebNameTable, string, string) {

	parameter2setting := make(map[string]string)
	var parameter, table, setting, source, error_message, return_table_name string
	source = strings.ToUpper(task.current_table)
	return_table_name = task.current_table

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
			return_table_name = source
		} else if strings.Contains(parameter, "$Column") {
			table = setting
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		} else if strings.Contains(strings.ToUpper(parameter), "($SETTING)") {
			parameter2setting[strings.ToUpper(GetParameter(parameter))] = setting
		}
	}

	var result_table WebNameTable

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "MERGETABLE" {
		result_table = MergeTable(table_store, table)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "COMBINETABLEBYCOMMONCOLUMN" {
		result_table, error_message = CombineTableByCommonColumn(table_store, source, table)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "APPENDROW" {
		result_table, error_message = AppendRow(table_store, cell_store, source, parameter2setting, table)
	}

	return result_table, return_table_name, error_message
}

func AppendRow(table_store map[string]WebNameTable, cell_store map[string]string, source string, parameter2setting map[string]string, table string) (WebNameTable, string) {

	total_column := table_store[strings.ToUpper(source)].total_column
	fact_table := table_store[strings.ToUpper(source)].fact_table
	key2value := table_store[strings.ToUpper(source)].key2value
	value2key := table_store[strings.ToUpper(source)].value2key
	column_name := table_store[strings.ToUpper(source)].column_name
	data_type := table_store[strings.ToUpper(source)].data_type
	upper_column_name2id := table_store[strings.ToUpper(source)].upper_column_name2id
	var key uint64
	var current_text string
	var error_message string
	var column_missing strings.Builder
	var error int = 0
	var result_table WebNameTable

	for k, v := range parameter2setting {
		if _, found := cell_store[strings.ToUpper(v)]; found {
			parameter2setting[k] = cell_store[strings.ToUpper(v)]
		} else {
			if _, found := upper_column_name2id[strings.ToUpper(k)]; found {
			} else {
				if error > 0 {
					column_missing.WriteString(", \"" + k + "\"")
				} else {
					column_missing.WriteString("\"" + k + "\"")
				}
				error++
			}
		}
	}

	if error > 0 {
		error_message = "Column name " + column_missing.String() + " not found in table " + source
	} else {

		for x := 0; x < total_column; x++ {
			if _, found := parameter2setting[strings.ToUpper(column_name[x])]; found {
				current_text = parameter2setting[strings.ToUpper(column_name[x])]
			} else {
				if data_type[x] != "Number" {
					current_text = "null"
				} else {
					current_text = "0"
				}
			}

			if data_type[x] != "Number" {
				if _, found := value2key[x][current_text]; found {
					fact_table[x] = append(fact_table[x], float64(value2key[x][current_text]))
				} else {
					key = uint64(len(key2value[x]))
					value2key[x][current_text] = key
					key2value[x][key] = current_text
					fact_table[x] = append(fact_table[x], float64(key))
				}
			} else {
				current_number, _ := strconv.ParseFloat(current_text, 64)
				fact_table[x] = append(fact_table[x], current_number)
			}
		}

		result_table.total_column = table_store[source].total_column
		result_table.extra_line_br_char = table_store[source].extra_line_br_char
		result_table.column_name = table_store[source].column_name
		result_table.upper_column_name2id = table_store[source].upper_column_name2id
		result_table.data_type = table_store[source].data_type
		result_table.fact_table = fact_table
		result_table.key2value = key2value
		result_table.value2key = value2key
	}

	return result_table, error_message
}

func CombineTableByCommonColumn(table_store map[string]WebNameTable, source string, table string) (WebNameTable, string) {

	var table_name = String2Slice(table, false)

	var column_id int
	var match_column strings.Builder
	is_match_column := make(map[string]bool)
	var current_column string

	for i := 0; i < len(table_store[strings.ToUpper(table_name[0])].column_name); i++ {
		current_column = table_store[strings.ToUpper(table_name[0])].column_name[i]
		is_match_column[current_column] = true
	}

	for i := 1; i < len(table_name); i++ {
		for j := 0; j < len(table_store[strings.ToUpper(table_name[0])].column_name); j++ {
			current_column = table_store[strings.ToUpper(table_name[0])].column_name[j]

			if _, found := table_store[strings.ToUpper(table_name[i])].upper_column_name2id[strings.ToUpper(current_column)]; found {
				column_id = table_store[strings.ToUpper(table_name[i])].upper_column_name2id[strings.ToUpper(current_column)]
				if table_store[strings.ToUpper(table_name[i])].data_type[column_id] == table_store[strings.ToUpper(table_name[0])].data_type[j] {
					is_match_column[current_column] = is_match_column[current_column] && true
				} else {
					is_match_column[current_column] = is_match_column[current_column] && false
				}
			} else {
				is_match_column[current_column] = is_match_column[current_column] && false
			}
		}
	}

	for k, v := range is_match_column {
		if v == true {
			if len(match_column.String()) == 0 {
				match_column.WriteString(k)
			} else {
				match_column.WriteString(", " + k)
			}
		}
	}

	temp_data_store := make(map[string]WebNameTable)
	var error_message string

	for i := 0; i < len(table_name); i++ {
		var current_table, message = SelectColumn(table_store, strings.ToUpper(table_name[i]), match_column.String())
		error_message = error_message + "\n" + message
		temp_data_store[strings.ToUpper(table_name[i])] = current_table
	}

	var result_table = MergeTable(temp_data_store, table)

	return result_table, strings.TrimSpace(error_message)
}

func MergeTable(table_store map[string]WebNameTable, table string) WebNameTable {

	var table_name = String2Slice(table, false)

	var result_column_name, result_data_type []string
	result_upper_column_name2id := make(map[string]int)
	result_upper_column_name_data_type2id := make(map[string]int)

	var result_total_column int
	var current_column_name, current_data_type string
	var table_column_name2id = make(map[string]int)
	var column_id int = 0
	var same_column_name int = 2

	for i := 0; i < len(table_name); i++ {
		for j := 0; j < len(table_store[strings.ToUpper(table_name[i])].column_name); j++ {
			current_column_name = table_store[strings.ToUpper(table_name[i])].column_name[j]
			current_data_type = table_store[strings.ToUpper(table_name[i])].data_type[j]

			if _, found := result_upper_column_name_data_type2id[strings.ToUpper(current_column_name+"_"+current_data_type)]; found {
				table_column_name2id[table_name[i]+"_"+current_column_name] = j
			} else {

				if _, found := result_upper_column_name2id[strings.ToUpper(current_column_name)]; found {
					result_upper_column_name2id[strings.ToUpper(current_column_name)+"_"+strconv.Itoa(same_column_name)] = column_id
					result_column_name = append(result_column_name, current_column_name+"_"+strconv.Itoa(same_column_name))
					table_column_name2id[table_name[i]+"_"+current_column_name+"_"+strconv.Itoa(same_column_name)] = j
					same_column_name++
				} else {
					result_upper_column_name2id[strings.ToUpper(current_column_name)] = column_id
					result_column_name = append(result_column_name, current_column_name)
					table_column_name2id[table_name[i]+"_"+current_column_name] = j
				}
				result_data_type = append(result_data_type, current_data_type)
				result_upper_column_name_data_type2id[strings.ToUpper(current_column_name+"_"+current_data_type)] = column_id
				column_id++
			}
		}
	}

	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)

	result_total_column = len(result_column_name)
	var mutex sync.Mutex
	var parallel sync.WaitGroup
	parallel.Add(result_total_column)

	for current_column := 0; current_column < result_total_column; current_column++ {
		go func(current_column int) {
			defer parallel.Done()
			var fact_table, key2value, value2key = MergeOneColumn(current_column, table_store, table_name, result_data_type, table_column_name2id, result_column_name)
			mutex.Lock()
			result_key2value[current_column] = key2value
			result_value2key[current_column] = value2key
			result_fact_table[current_column] = fact_table
			mutex.Unlock()
		}(current_column)
	}
	parallel.Wait()

	var result_table WebNameTable
	result_table.total_column = result_total_column
	result_table.extra_line_br_char = table_store[strings.ToUpper(table_name[0])].extra_line_br_char
	result_table.column_name = result_column_name
	result_table.upper_column_name2id = result_upper_column_name2id
	result_table.data_type = result_data_type
	result_table.fact_table = result_fact_table
	result_table.key2value = result_key2value
	result_table.value2key = result_value2key

	return result_table
}

func MergeOneColumn(current_column int, table_store map[string]WebNameTable, table_name []string, result_data_type []string, table_column_name2id map[string]int, result_column_name []string) ([]float64, map[uint64]string, map[string]uint64) {

	var fact_table []float64
	key2value := make(map[uint64]string)
	value2key := make(map[string]uint64)
	var current_text string
	var column_id, total_row int
	var current_number float64
	var key uint64 = 0

	for j := 0; j < len(table_name); j++ {

		if _, found := table_column_name2id[table_name[j]+"_"+result_column_name[current_column]]; found {
			column_id = table_column_name2id[table_name[j]+"_"+result_column_name[current_column]]

			if result_data_type[current_column] != "Number" {
				total_row = len(table_store[strings.ToUpper(table_name[j])].fact_table[0])
				for k := 0; k < total_row; k++ {
					current_number = table_store[strings.ToUpper(table_name[j])].fact_table[column_id][k]
					current_text = table_store[strings.ToUpper(table_name[j])].key2value[column_id][uint64(current_number)]

					if _, found := value2key[current_text]; found {
						fact_table = append(fact_table, float64(value2key[current_text]))
					} else {
						value2key[current_text] = key
						key2value[key] = current_text
						fact_table = append(fact_table, float64(key))
						key++
					}
				}
			} else {
				for k := 0; k < len(table_store[strings.ToUpper(table_name[j])].fact_table[0]); k++ {
					current_number = table_store[strings.ToUpper(table_name[j])].fact_table[column_id][k]
					fact_table = append(fact_table, current_number)
				}
			}
		} else {
			if result_data_type[current_column] != "Number" {
				total_row = len(table_store[strings.ToUpper(table_name[j])].fact_table[0])
				for k := 0; k < total_row; k++ {
					current_text = "null"

					if _, found := value2key[current_text]; found {
						fact_table = append(fact_table, float64(value2key[current_text]))
					} else {
						value2key[current_text] = key
						key2value[key] = current_text
						fact_table = append(fact_table, float64(key))
						key++
					}
				}
			} else {
				for k := 0; k < len(table_store[strings.ToUpper(table_name[j])].fact_table[0]); k++ {
					current_number = 0
					fact_table = append(fact_table, current_number)
				}
			}
		}
	}
	return fact_table, key2value, value2key
}

package webname

import (
	"strconv"
	"strings"
)

func AmendColumn(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string, string) {

	var parameter, column, setting, source, return_table_name string
	source = strings.ToUpper(task.current_table)
	return_table_name = task.current_table

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
			return_table_name = source
		} else if strings.Contains(parameter, "$Column") {
			column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	var result_table WebNameTable
	var message string

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECTCOLUMN" {
		result_table, message = SelectColumn(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "NUMBER2TEXT" {
		result_table = Number2Text(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "REMOVECOLUMN" {
		result_table = RemoveColumn(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "REVERSENUMBER" {
		result_table = ReverseNumber(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "AMENDCOLUMNNAME" {
		result_table = AmendColumnName(table_store, source, column)
	}

	return result_table, return_table_name, message
}

func AmendColumnName(table_store map[string]WebNameTable, source string, column string) WebNameTable {

	var amend_column_name = String2Map(column, true)
	var result_column_name []string
	result_upper_column_name2id := make(map[string]int)

	for x := 0; x < len(table_store[source].column_name); x++ {

		var key = strings.ToUpper(table_store[source].column_name[x])

		if _, found := amend_column_name[key]; found {
			result_column_name = append(result_column_name, amend_column_name[key])
			result_upper_column_name2id[amend_column_name[key]] = x
		} else {
			result_column_name = append(result_column_name, table_store[source].column_name[x])
			result_upper_column_name2id[key] = x
		}
	}

	var result_table WebNameTable
	result_table.total_column = table_store[source].total_column
	result_table.extra_line_br_char = table_store[source].extra_line_br_char
	result_table.column_name = result_column_name
	result_table.upper_column_name2id = result_upper_column_name2id
	result_table.data_type = table_store[source].data_type
	result_table.fact_table = table_store[source].fact_table
	result_table.key2value = table_store[source].key2value
	result_table.value2key = table_store[source].value2key

	return result_table
}

func Number2Text(table_store map[string]WebNameTable, source string, column string) WebNameTable {

	var column_name = String2Slice(column, false)
	var column_id int
	var current_text string
	var temp float64
	var result_data_type []string
	column_id_map := make(map[int]int)
	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)

	for x := 0; x < len(column_name); x++ {

		column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
		if table_store[source].data_type[column_id] == "Number" {
			column_id_map[column_id] = x

			var key uint64 = 0
			var fact_table []float64
			key2value := make(map[uint64]string)
			value2key := make(map[string]uint64)

			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[column_id][y]
				current_text = strconv.FormatFloat(temp, 'f', -1, 64)

				if _, found := value2key[current_text]; found {
					fact_table = append(fact_table, float64(value2key[current_text]))
				} else {
					value2key[current_text] = key
					key2value[key] = current_text
					fact_table = append(fact_table, float64(key))
					key += 1
				}
			}

			result_fact_table[column_id] = fact_table
			result_key2value[column_id] = key2value
			result_value2key[column_id] = value2key
		}

	}

	for x := 0; x < table_store[source].total_column; x++ {
		if _, found := column_id_map[x]; found {
			result_data_type = append(result_data_type, "Text")
		} else {
			result_data_type = append(result_data_type, table_store[source].data_type[x])
			result_fact_table[x] = table_store[source].fact_table[x]
			result_key2value[x] = table_store[source].key2value[x]
			result_value2key[x] = table_store[source].value2key[x]
		}
	}

	var result_table WebNameTable
	result_table.total_column = table_store[source].total_column
	result_table.extra_line_br_char = table_store[source].extra_line_br_char
	result_table.column_name = table_store[source].column_name
	result_table.upper_column_name2id = table_store[source].upper_column_name2id
	result_table.data_type = result_data_type
	result_table.fact_table = result_fact_table
	result_table.key2value = result_key2value
	result_table.value2key = result_value2key

	return result_table
}

func RemoveColumn(table_store map[string]WebNameTable, source string, column string) WebNameTable {

	var column_name = String2Slice(column, false)
	upper_column_name := make(map[string]int)

	for x := 0; x < len(column_name); x++ {
		upper_column_name[strings.ToUpper(column_name[x])] = x
	}

	var result_column_name, result_data_type []string
	result_upper_column_name2id := make(map[string]int)
	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)
	var source_column_ID int
	var result_column_ID int = 0

	for x := 0; x < len(table_store[source].column_name); x++ {
		if _, found := upper_column_name[strings.ToUpper(table_store[source].column_name[x])]; found {
		} else {
			result_column_name = append(result_column_name, table_store[source].column_name[x])
			source_column_ID = table_store[source].upper_column_name2id[strings.ToUpper(table_store[source].column_name[x])]
			result_upper_column_name2id[strings.ToUpper(table_store[source].column_name[x])] = result_column_ID
			result_data_type = append(result_data_type, table_store[source].data_type[source_column_ID])
			result_fact_table[result_column_ID] = table_store[source].fact_table[source_column_ID]
			result_key2value[result_column_ID] = table_store[source].key2value[source_column_ID]
			result_value2key[result_column_ID] = table_store[source].value2key[source_column_ID]
			result_column_ID++
		}
	}

	var result_table WebNameTable
	result_table.total_column = len(result_column_name)
	result_table.extra_line_br_char = table_store[source].extra_line_br_char
	result_table.column_name = result_column_name
	result_table.upper_column_name2id = result_upper_column_name2id
	result_table.data_type = result_data_type
	result_table.fact_table = result_fact_table
	result_table.key2value = result_key2value
	result_table.value2key = result_value2key

	return result_table
}

func ReverseNumber(table_store map[string]WebNameTable, source string, column string) WebNameTable {

	var column_name = String2Slice(column, false)
	var column_id int
	var temp float64
	column_id_map := make(map[int]int)
	result_fact_table := make(map[int][]float64)

	for x := 0; x < len(column_name); x++ {

		column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
		if table_store[source].data_type[column_id] == "Number" {
			column_id_map[column_id] = x

			var fact_table []float64

			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[column_id][y] * -1
				fact_table = append(fact_table, temp)
			}
			result_fact_table[column_id] = fact_table
		}
	}

	for x := 0; x < table_store[source].total_column; x++ {
		if _, found := column_id_map[x]; found {
		} else {
			result_fact_table[x] = table_store[source].fact_table[x]
		}
	}

	var result_table WebNameTable
	result_table.total_column = table_store[source].total_column
	result_table.extra_line_br_char = table_store[source].extra_line_br_char
	result_table.column_name = table_store[source].column_name
	result_table.upper_column_name2id = table_store[source].upper_column_name2id
	result_table.data_type = table_store[source].data_type
	result_table.fact_table = result_fact_table
	result_table.key2value = table_store[source].key2value
	result_table.value2key = table_store[source].value2key

	return result_table
}

func SelectColumn(table_store map[string]WebNameTable, source string, column string) (WebNameTable, string) {

	var result_table WebNameTable
	var column_name = String2Slice(column, false)

	var error_message string
	var column_missing strings.Builder
	var error int = 0
	for x := 0; x < len(column_name); x++ {
		if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
		} else {
			if error > 0 {
				column_missing.WriteString(", \"" + column_name[x] + "\"")
			} else {
				column_missing.WriteString("\"" + column_name[x] + "\"")
			}
			error++
		}
	}
	if error > 0 {
		error_message = "Column name " +  column_missing.String() + " not found in table " + source
	} else {

		upper_column_name := make(map[string]int)

		for x := 0; x < len(column_name); x++ {
			upper_column_name[strings.ToUpper(column_name[x])] = x
     	}

		var result_column_name, result_data_type []string
		result_upper_column_name2id := make(map[string]int)
		result_fact_table := make(map[int][]float64)
		result_key2value := make(map[int]map[uint64]string)
		result_value2key := make(map[int]map[string]uint64)
		var source_column_ID int
		var result_column_ID int = 0

		for x := 0; x < len(column_name); x++ {
			if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
				result_column_name = append(result_column_name, column_name[x])
				source_column_ID = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
				result_upper_column_name2id[strings.ToUpper(column_name[x])] = result_column_ID
				result_data_type = append(result_data_type, table_store[source].data_type[source_column_ID])
				result_fact_table[result_column_ID] = table_store[source].fact_table[source_column_ID]
				result_key2value[result_column_ID] = table_store[source].key2value[source_column_ID]
				result_value2key[result_column_ID] = table_store[source].value2key[source_column_ID]
				result_column_ID++
			}
		}

		result_table.total_column = len(result_column_name)
		result_table.extra_line_br_char = table_store[source].extra_line_br_char
		result_table.column_name = result_column_name
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.data_type = result_data_type
		result_table.fact_table = result_fact_table
		result_table.key2value = result_key2value
		result_table.value2key = result_value2key
	}

	return result_table, error_message
}

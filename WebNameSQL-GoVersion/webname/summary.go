package webname

import (
	"math"
	"strings"
	"sync"
)

func Summary(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string, string) {

	parameter2setting := make(map[string]string)
	var parameter, column, x_column, y_column, setting, source, add_column_name, error_message, return_table_name string
	var calc_column_name, group_by_function []string
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
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "COUNT($SETTING)") {
			calc_column_name = append(calc_column_name, "Null")
			group_by_function = append(group_by_function, "Count")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "SUM($SETTING)") {
			calc_column_name = append(calc_column_name, setting)
			group_by_function = append(group_by_function, "Sum")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "MAX($SETTING)") {
			calc_column_name = append(calc_column_name, setting)
			group_by_function = append(group_by_function, "Max")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "MIN($SETTING)") {
			calc_column_name = append(calc_column_name, setting)
			group_by_function = append(group_by_function, "Min")
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "X($SETTING)") {
			x_column = setting		
		} else if strings.Contains(strings.ToUpper(strings.ReplaceAll(parameter,"~","")), "Y($SETTING)") {
			y_column = setting	
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		} else if strings.Contains(strings.ToUpper(parameter), "($SETTING)") {
			parameter2setting[strings.ToUpper(GetParameter(parameter))] = setting
		}
	}

	var result_table WebNameTable

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "DISTINCT" {
		result_table, error_message = Distinct(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "GROUPBY" {
		result_table, error_message = GroupBy(table_store, source, column, add_column_name, calc_column_name, group_by_function)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "CROSSTAB" {
		result_table, error_message = Crosstab(table_store, source, x_column, y_column, add_column_name, calc_column_name, group_by_function)
	}

	return result_table, return_table_name, error_message
}

func Distinct(table_store map[string]WebNameTable, source string, column string) (WebNameTable, string) {

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
		error_message = "Column name " + column_missing.String() + " not found in table " + source
	} else {
		add_column_name := "CompositKey"
		arrow_calc_function := "CompositKey"
		precision := 0

		composit_key_table := ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)

		result_fact_table := make(map[int][]float64)
		result_key2value := make(map[int]map[uint64]string)
		result_value2key := make(map[int]map[string]uint64)
		result_upper_column_name2id := make(map[string]int)
		var result_column_name []string
		var result_data_type []string

		var current_composit_key float64
		var reference_column []int
		var column_id int

		for x := 0; x < len(column_name); x++ {
			if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
				column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
				reference_column = append(reference_column, column_id)
			}
		}

		var composit_key_column_id int = composit_key_table.upper_column_name2id["COMPOSITKEY"]
		composit_key_map := make(map[float64]int)
		var composit_key_sequence []float64

		for y := 0; y < len(composit_key_table.fact_table[composit_key_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			if _, found := composit_key_map[current_composit_key]; found {
			} else {
				composit_key_map[current_composit_key] = y				
				composit_key_sequence = append(composit_key_sequence, current_composit_key)
			}
		}

		for x := 0; x < len(column_name); x++ {
			var fact_table []float64
			for i := 0; i < len(composit_key_sequence); i++ {
				fact_table = append(fact_table, composit_key_table.fact_table[reference_column[x]][composit_key_map[composit_key_sequence[i]]])
			}
			result_fact_table[x] = fact_table
			result_key2value[x] = composit_key_table.key2value[reference_column[x]]
			result_value2key[x] = composit_key_table.value2key[reference_column[x]]
			result_column_name = append(result_column_name, composit_key_table.column_name[reference_column[x]])
			result_data_type = append(result_data_type, composit_key_table.data_type[reference_column[x]])
			result_upper_column_name2id[strings.ToUpper(composit_key_table.column_name[reference_column[x]])] = x
		}

		result_table.fact_table = result_fact_table
		result_table.key2value = result_key2value
		result_table.value2key = result_value2key
		result_table.column_name = result_column_name
		result_table.data_type = result_data_type
		result_table.extra_line_br_char = 0
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.total_column = len(column_name)
		result_table.composit_key_map = composit_key_map
	}

	return result_table, error_message
}

func GroupBy(table_store map[string]WebNameTable, source string, column string, add_column_name string, calc_column_name []string, group_by_function []string) (WebNameTable, string) {

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
		error_message = "Column name " + column_missing.String() + " not found in table " + source
	} else {
		add_column_name := "CompositKey"
		arrow_calc_function := "CompositKey"
		precision := 0

		composit_key_table := ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)

		result_fact_table := make(map[int][]float64)
		result_key2value := make(map[int]map[uint64]string)
		result_value2key := make(map[int]map[string]uint64)
		result_upper_column_name2id := make(map[string]int)
		var result_column_name []string
		var result_data_type []string

		var current_composit_key float64
		var reference_column []int
		var column_id int

		for x := 0; x < len(column_name); x++ {
			if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
				column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
				reference_column = append(reference_column, column_id)
			}
		}

		var composit_key_column_id int = composit_key_table.upper_column_name2id["COMPOSITKEY"]
		composit_key_map := make(map[float64]int)
		result_table_row2first_key_exist_row := make(map[int]int)
		var composit_key_sequence []float64
		var first_key_exist_row, k int

		for y := 0; y < len(composit_key_table.fact_table[composit_key_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			if _, found := composit_key_map[current_composit_key]; found {
			} else {
				composit_key_map[current_composit_key] = k
				result_table_row2first_key_exist_row[k] = y
				composit_key_sequence = append(composit_key_sequence, current_composit_key)
				k++
			}
		}

		for x := 0; x < len(column_name); x++ {
			var fact_table []float64
			for i := 0; i < len(composit_key_sequence); i++ {
				first_key_exist_row = result_table_row2first_key_exist_row[composit_key_map[composit_key_sequence[i]]]
				fact_table = append(fact_table, composit_key_table.fact_table[reference_column[x]][first_key_exist_row])
			}
			result_fact_table[x] = fact_table
			result_key2value[x] = composit_key_table.key2value[reference_column[x]]
			result_value2key[x] = composit_key_table.value2key[reference_column[x]]
			result_column_name = append(result_column_name, composit_key_table.column_name[reference_column[x]])
			result_data_type = append(result_data_type, composit_key_table.data_type[reference_column[x]])
			result_upper_column_name2id[strings.ToUpper(composit_key_table.column_name[reference_column[x]])] = x
		}		

		var mutex sync.Mutex
		var parallel sync.WaitGroup
		parallel.Add(len(calc_column_name))

		for x := 0; x < len(calc_column_name); x++ {
			go func(x int) {
				defer parallel.Done()              
				fact_table := GroupByEachColumn(x, composit_key_table, calc_column_name, group_by_function, composit_key_map, current_composit_key, composit_key_column_id, result_table_row2first_key_exist_row)
				mutex.Lock()
				result_fact_table[len(column_name)+x] = fact_table
				mutex.Unlock()
			}(x)
		}
		parallel.Wait()

		for x := 0; x < len(calc_column_name); x++ {
			result_data_type = append(result_data_type, "Number")

			if group_by_function[x] == "Count" {
				result_column_name = append(result_column_name, group_by_function[x])
				result_upper_column_name2id[strings.ToUpper(group_by_function[x])] = len(column_name) + x
			} else if group_by_function[x] == "Sum" {
				result_column_name = append(result_column_name, calc_column_name[x])
			} else {
				result_column_name = append(result_column_name, group_by_function[x]+":"+calc_column_name[x])
				result_upper_column_name2id[strings.ToUpper(group_by_function[x]+":"+calc_column_name[x])] = len(column_name) + x
			}
		}

		result_table.fact_table = result_fact_table
		result_table.key2value = result_key2value
		result_table.value2key = result_value2key
		result_table.column_name = result_column_name
		result_table.data_type = result_data_type
		result_table.extra_line_br_char = 0
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.total_column = len(result_column_name)
	}

	return result_table, error_message
}

func GroupByEachColumn(x int, composit_key_table WebNameTable, calc_column_name []string, group_by_function []string, composit_key_map map[float64]int, current_composit_key float64, composit_key_column_id int, result_table_row2first_key_exist_row map[int]int) []float64 {

	var calc_column_id int
	var current_number float64
	var current_row int

	var fact_table []float64
	if calc_column_name[x] == "Null" {
		calc_column_id = 0
	} else {
		calc_column_id = composit_key_table.upper_column_name2id[strings.ToUpper(calc_column_name[x])]
	}

	if group_by_function[x] == "Count" {

		for y := 0; y < len(composit_key_map); y++ {
			fact_table = append(fact_table, 0)
		}
		for y := 0; y < len(composit_key_table.fact_table[calc_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			current_row = composit_key_map[current_composit_key]
			fact_table[current_row] = fact_table[current_row] + 1
		}
	} else if group_by_function[x] == "Sum" {

		for y := 0; y < len(composit_key_map); y++ {
			fact_table = append(fact_table, 0)
		}
		for y := 0; y < len(composit_key_table.fact_table[calc_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			current_row = composit_key_map[current_composit_key]
			fact_table[current_row] = math.Round((fact_table[current_row]+composit_key_table.fact_table[calc_column_id][y])*100) / 100
		}
	} else if group_by_function[x] == "Max" {

		for y := 0; y < len(composit_key_map); y++ {
			fact_table = append(fact_table, composit_key_table.fact_table[calc_column_id][result_table_row2first_key_exist_row[y]])
		}

		for y := 0; y < len(composit_key_table.fact_table[calc_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			current_number = composit_key_table.fact_table[calc_column_id][y]
			current_row = composit_key_map[current_composit_key]

			if current_number > fact_table[current_row] {
				fact_table[current_row] = current_number
			}

		}
	} else if group_by_function[x] == "Min" {

		for y := 0; y < len(composit_key_map); y++ {
			fact_table = append(fact_table, composit_key_table.fact_table[calc_column_id][result_table_row2first_key_exist_row[y]])
		}

		for y := 0; y < len(composit_key_table.fact_table[calc_column_id]); y++ {
			current_composit_key = composit_key_table.fact_table[composit_key_column_id][y]
			current_number = composit_key_table.fact_table[calc_column_id][y]
			current_row = composit_key_map[current_composit_key]

			if current_number < fact_table[current_row] {
				fact_table[current_row] = current_number
			}
		}
	}

	return fact_table
}

func Crosstab(table_store map[string]WebNameTable, source string, x_column string, y_column string, add_column_name string, calc_column_name []string, group_by_function []string) (WebNameTable, string) {
	
	column := x_column + "," + y_column		
	group_by_table, error_message := GroupBy(table_store, source, column, add_column_name, calc_column_name, group_by_function)
     
   

	return group_by_table, error_message
}

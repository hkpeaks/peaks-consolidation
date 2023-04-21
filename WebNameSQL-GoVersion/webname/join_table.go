package webname

import (
	"strings"
	"sync"
)

func Join(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string) {

	var parameter, setting, return_table_name string
	var join_table_name, join_column_name []string
	return_table_name = task.current_table
	var error_message string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		} else if strings.Contains(strings.ToUpper(parameter), "($SETTING)") {
			join_table_name = append(join_table_name, GetParameter(parameter))
			join_column_name = append(join_column_name, setting)
		}
	}

	var result_table WebNameTable

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "JOINTABLE" {
		result_table, error_message = JoinTable(table_store, join_table_name, join_column_name)

		if len(error_message) > 0 {
			println(error_message)
		}
	}
	return result_table, return_table_name
}

func JoinTable(table_store map[string]WebNameTable, join_table_name []string, join_column_name []string) (WebNameTable, string) {

	var result_table, distinct_table, transaction_table, master_table, left_distinct_table, right_distinct_table WebNameTable
	var join_column string
	var error_message string
	var is_left_master bool
	var is_right_master bool
	var source []string

	source = append(source, strings.ToUpper(strings.ReplaceAll(join_table_name[0], "@", "")))
	source = append(source, strings.ToUpper(strings.ReplaceAll(join_table_name[1], "@", "")))
	master_table_store := make(map[string]WebNameTable)

	for x := 0; x < len(join_table_name); x++ {
		distinct_table, error_message = Distinct(table_store, source[x], join_column_name[x])

		if x == 0 {
			left_distinct_table = distinct_table

			if len(table_store[source[x]].fact_table[0]) > len(left_distinct_table.fact_table[0]) {
				is_left_master = false
			} else {
				is_left_master = true
			}
		} else {
			right_distinct_table = distinct_table

			if len(table_store[source[x]].fact_table[0]) > len(right_distinct_table.fact_table[0]) {
				is_right_master = false
			} else {
				is_right_master = true
			}
		}
	}

	if is_left_master == false && is_right_master == false {
		error_message = "Both tables contain repeating combinations with links to common columns, please proide at lease one table which does not contain repeating combinations"

	} else {
		add_column_name := "CompositKey"
		arrow_calc_function := "CompositKey"
		precision := 0
		var composit_key_map map[float64]int
		var master_join_column []string
		var master_join_column_id []int
		var transaction_join_column []string
		var transaction_join_column_id []int
		master_join_column_map := make(map[int]int)

		if is_left_master == true {
			transaction_table = ComputeColumn(table_store, source[1], join_column_name[1], arrow_calc_function, add_column_name, precision)
			join_column = join_column_name[0]
			master_table = table_store[source[0]]
			master_join_column = String2Slice(join_column_name[0], true)
			transaction_join_column = String2Slice(join_column_name[1], true)
		} else {
			transaction_table = ComputeColumn(table_store, source[0], join_column_name[0], arrow_calc_function, add_column_name, precision)
			join_column = join_column_name[1]
			master_table = table_store[source[1]]
			master_join_column = String2Slice(join_column_name[1], true)
			transaction_join_column = String2Slice(join_column_name[0], true)
		}

		for i := 0; i < master_table.total_column; i++ {
			for j := 0; j < len(master_join_column); j++ {
				if master_join_column[j] == strings.ToUpper(master_table.column_name[i]) {
					master_join_column_id = append(master_join_column_id, i)
					master_join_column_map[i] = i
				}
			}
		}

		for i := 0; i < transaction_table.total_column; i++ {
			for j := 0; j < len(transaction_join_column); j++ {
				if transaction_join_column[j] == strings.ToUpper(transaction_table.column_name[i]) {
					transaction_join_column_id = append(transaction_join_column_id, i)
				}
			}
		}

		var current_text string
		var z int

		for x := 0; x < master_table.total_column; x++ {
			var revised_fact_table []float64

			if _, found := master_join_column_map[x]; found {
				for y := 0; y < len(master_table.fact_table[0]); y++ {
					current_text = master_table.key2value[x][uint64(master_table.fact_table[x][y])]
					revised_fact_table = append(revised_fact_table, float64(transaction_table.value2key[transaction_join_column_id[z]][current_text]))
				}
				master_table.key2value[x] = transaction_table.key2value[transaction_join_column_id[z]]
				master_table.value2key[x] = transaction_table.value2key[transaction_join_column_id[z]]
				z++

			} else {
				for y := 0; y < len(master_table.fact_table[0]); y++ {
					revised_fact_table = append(revised_fact_table, master_table.fact_table[x][y])
				}
			}
			master_table.fact_table[x] = revised_fact_table
		}

		master_table_store["MASTER"] = master_table
		distinct_table, error_message = Distinct(master_table_store, "MASTER", join_column)
		composit_key_map = distinct_table.composit_key_map
		master_table = RemoveColumn(master_table_store, "MASTER", join_column)

		var composit_key_column_id = transaction_table.upper_column_name2id["COMPOSITKEY"]

		result_fact_table := make(map[int][]float64)
		var result_column_name, result_data_type []string
		result_upper_column_name2id := make(map[string]int)
		result_key2value := make(map[int]map[uint64]string)
		result_value2key := make(map[int]map[string]uint64)

		for x := 0; x < transaction_table.total_column; x++ {
			result_column_name = append(result_column_name, transaction_table.column_name[x])
			result_upper_column_name2id[strings.ToUpper(transaction_table.column_name[x])] = x
			result_data_type = append(result_data_type, transaction_table.data_type[x])
			result_fact_table[x] = transaction_table.fact_table[x]

			if transaction_table.data_type[x] != "Number" {
				result_key2value[x] = transaction_table.key2value[x]
				result_value2key[x] = transaction_table.value2key[x]
			}
		}

		for x := 0; x < master_table.total_column; x++ {
			result_column_name = append(result_column_name, master_table.column_name[x])
			result_upper_column_name2id[strings.ToUpper(master_table.column_name[x])] = transaction_table.total_column + x
			result_data_type = append(result_data_type, master_table.data_type[x])

			if transaction_table.data_type[x] != "Number" {
				result_key2value[transaction_table.total_column+x] = master_table.key2value[x]
				result_value2key[transaction_table.total_column+x] = master_table.value2key[x]
			}
		}

		var mutex sync.Mutex
		var parallel sync.WaitGroup
		parallel.Add(master_table.total_column)

		for current_column := 0; current_column < master_table.total_column; current_column++ {
			go func(current_column int) {
				defer parallel.Done()

				fact_table := Join_One_Column(current_column, master_table, transaction_table, composit_key_map, composit_key_column_id)

				mutex.Lock()
				result_fact_table[transaction_table.total_column+current_column] = fact_table
				mutex.Unlock()
			}(current_column)
		}

		parallel.Wait()

		result_table.total_column = len(result_column_name)
		result_table.extra_line_br_char = 0
		result_table.column_name = result_column_name
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.data_type = result_data_type
		result_table.fact_table = result_fact_table
		result_table.key2value = result_key2value
		result_table.value2key = result_value2key

	}

	master_table_store["COMPOSITKEY"] = result_table
	result_table = RemoveColumn(master_table_store, "COMPOSITKEY", "COMPOSITKEY")

	return result_table, error_message
}

func Join_One_Column(x int, master_table WebNameTable, transaction_table WebNameTable, composit_key_map map[float64]int, composit_key_column_id int) []float64 {
	var fact_table []float64
	var current_row int
	var current_key float64


	for y := 0; y < len(transaction_table.fact_table[0]); y++ {
		current_key = transaction_table.fact_table[composit_key_column_id][y]
	    	//println(current_key)

		if _, found := composit_key_map[current_key]; found {
			current_row = composit_key_map[current_key]
			fact_table = append(fact_table, master_table.fact_table[x][current_row])
		}
	}
	return fact_table
}

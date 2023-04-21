package webname

import (	
	"strings"
	"sync"	
)


func CopyTable(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string) {

	var parameter, setting, source, return_table_name string
	source = task.current_table
	return_table_name = task.current_table

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
			return_table_name = source
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	var result_table = CopyOneTable(table_store, source)

//	var result_table WebNameTable = table_store[source]

	return result_table, return_table_name
}

func CopyOneTable(table_store map[string]WebNameTable, source string) WebNameTable {

	var result_table WebNameTable

	result_table.column_name = table_store[source].column_name
	result_table.total_column = table_store[source].total_column
	result_table.data_type = table_store[source].data_type
	result_table.extra_line_br_char = table_store[source].extra_line_br_char
	result_table.upper_column_name2id = table_store[source].upper_column_name2id	
	
	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)

	var mutex sync.Mutex
    var parallel sync.WaitGroup
    parallel.Add(table_store[source].total_column)
	
	for current_column := 0; current_column < table_store[source].total_column; current_column++ {		
		go func(current_column int) {
			defer parallel.Done()
		var fact_table, key2value, value2key = CopyOneColumn(table_store, source, current_column)
		mutex.Lock()
		result_fact_table[current_column] = fact_table
		result_key2value[current_column] = key2value
		result_value2key[current_column] = value2key		
		mutex.Unlock()
		}(current_column)		
	}	

	parallel.Wait()

	result_table.fact_table = result_fact_table
	result_table.key2value = result_key2value
	result_table.value2key = result_value2key
	return result_table
}

func CopyOneColumn(table_store map[string]WebNameTable, source string, current_column int) ([]float64, map[uint64]string, map[string]uint64) {
	var fact_table []float64
	key2value := make(map[uint64]string)
	value2key := make(map[string]uint64)
	
	for y := 0; y < len(table_store[source].fact_table[0]); y++ {
		fact_table = append(fact_table, table_store[source].fact_table[current_column][y])
	}

	for k,v := range table_store[source].key2value[current_column] {
		key2value[k] = v
	}

	for k,v := range table_store[source].value2key[current_column] {
		value2key[k] = v
	}

    return fact_table, key2value, value2key
}

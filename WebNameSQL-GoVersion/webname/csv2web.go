package webname

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"	
)

type WebNameTable struct {
	total_column         int
	extra_line_br_char   int	
	column_name          []string	
	upper_column_name2id map[string]int
	data_type            []string
	fact_table           map[int][]float64
	key2value            map[int]map[uint64]string
	value2key            map[int]map[string]uint64
	composit_key_map     map[float64]int
}

func CSVFile2Web(task Task, rule Rule) (WebNameTable, string) {

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

	var bytestream = File2Bytestream("Input\\" + source)
	var web = CSVByte2Web(rule, bytestream)

	/*
	for x := 0; x < len(web.fact_table[0]); x++ {
	println(web.key2value[0][uint64(web.fact_table[0][x])])
	}
*/
	return web, return_table_name
}

func File2Bytestream(filename string) []byte {
	bytestream, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	return bytestream
}

func CSVByte2Web(rule Rule, bytestream []byte) WebNameTable {

	var total_column, extra_line_br_char, address = CellAddress(rule, bytestream)
	var column_name, upper_column_name2id, data_type = DataSchema(total_column, extra_line_br_char, address, bytestream)
	fact_table := make(map[int][]float64)
	key2value := make(map[int]map[uint64]string)
	value2key := make(map[int]map[string]uint64)
	var mutex sync.Mutex
	var parallel sync.WaitGroup
	parallel.Add(total_column)

	for current_column := 0; current_column < total_column; current_column++ {
		go func(current_column int) {
			defer parallel.Done()
			if data_type[current_column] == "Text" || data_type[current_column] == "Date" {
				var ft, k2v, v2k = CellText(total_column, extra_line_br_char, current_column, address, bytestream)
				mutex.Lock()
				fact_table[current_column] = ft
				key2value[current_column] = k2v
				value2key[current_column] = v2k
				mutex.Unlock()
			} else {
				var ft, k2v, v2k = CellNumber(total_column, extra_line_br_char, current_column, address, bytestream)
				mutex.Lock()
				fact_table[current_column] = ft
				key2value[current_column] = k2v
				value2key[current_column] = v2k
				mutex.Unlock()
			}
		}(current_column)
	}

	parallel.Wait()

	var result_table WebNameTable
	result_table.total_column = total_column	
	result_table.extra_line_br_char = extra_line_br_char
	result_table.column_name = column_name
	result_table.upper_column_name2id = upper_column_name2id
	result_table.data_type = data_type
	result_table.fact_table = fact_table
	result_table.key2value = key2value
	result_table.value2key = value2key

	return result_table
}

func CellAddress(rule Rule, bytestream []byte) (int, int, []uint64) {

	var total_column int = 1
	var extra_line_br_char int = 0
	var _double_quote_count int = 0

	for x := 0; x < len(bytestream); x++ {
		if bytestream[x] == byte(rule.csv_separator) && _double_quote_count%2 == 0 {
			total_column += 1
		} else if bytestream[x] == 13 || bytestream[x] == 10 {
			if bytestream[x+1] == 13 || bytestream[x+1] == 10 {
				extra_line_br_char += 1
			}
			break
		}
		if bytestream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var address []uint64
	address = append(address, 0)

	for x := 0; x < len(bytestream); x++ {
		if bytestream[x] == byte(rule.csv_separator) && _double_quote_count%2 == 0 {
			address = append(address, uint64(x+1))
		} else if bytestream[x] == 13 || bytestream[x] == 10 {
			address = append(address, uint64(x+1))
		}

		if bytestream[x] == 34 {
			_double_quote_count += 1
		}
	}

	return total_column, extra_line_br_char, address
}

func DataSchema(total_column int, extra_line_br_char int, cell_address []uint64, _bytestream []byte) ([]string, map[string]int, []string) {
	var current_text string
	var data_type []string
	var temp_cell_address strings.Builder
	var valiate_row int = len(cell_address) - 1
	column_id_map := make(map[int]int)

	if valiate_row > 100 {
		valiate_row = 100
	}

	var n int = 0

	for _current_column := 0; _current_column < total_column; _current_column++ {
		data_type = append(data_type, "Text")
		n = _current_column + total_column + extra_line_br_char

		for n < valiate_row {
			for x := cell_address[n]; x < (cell_address[n+1] - 1); x++ {
				temp_cell_address.WriteString(string(_bytestream[x]))
			}

			current_text = temp_cell_address.String()
			_, is_num := strconv.ParseFloat(current_text, 64)

			if is_num != nil {
				data_type[_current_column] = "Text"
				n = len(cell_address) - 1
			} else {
				data_type[_current_column] = "Number"
			}
			temp_cell_address.Reset()
			n += total_column + extra_line_br_char
		}
	}

	var column_name []string
	upper_column_name2id := make(map[string]int)
	text_column := []string{"A/C", "NUMBER", "INVOICE", "ACCOUNT", "DOCUMENT"}

	for _current_column := 0; _current_column < total_column; _current_column++ {
		for x := cell_address[_current_column]; x < cell_address[_current_column+1]-1; x++ {
			temp_cell_address.WriteString(string(_bytestream[x]))
		}
		
		current_text = strings.TrimSpace(temp_cell_address.String())

		if len(current_text) == 0 {
			current_text = "Column" + strconv.Itoa(_current_column)
		}

		if strings.Contains(strings.ToUpper(current_text), "DATE") {
			data_type[_current_column] = "Date"
		}

		for i := 0; i < len(text_column); i++ {
			if strings.Contains(strings.ToUpper(current_text), text_column[i]) {
				data_type[_current_column] = "Text"
			}
		}

		column_name = append(column_name, current_text)
		column_id_map[_current_column] = _current_column
		upper_column_name2id[strings.ToUpper(current_text)] = _current_column
		temp_cell_address.Reset()
	}

	return column_name, upper_column_name2id, data_type
}

func CellText(total_column int, extra_line_br_char int, current_column int, cell_address []uint64, bytestream []byte) ([]float64, map[uint64]string, map[string]uint64) {
	var current_text string
	var fact_table []float64
	var temp_cell_address strings.Builder
	var n int = current_column
	key2value := make(map[uint64]string)
	value2key := make(map[string]uint64)
	var key uint64 = 0

	n += total_column + extra_line_br_char

	for n < len(cell_address)-1 {		
		for x := cell_address[n]; x < cell_address[n+1]-1; x++ {
			temp_cell_address.WriteString(string(bytestream[x]))
		}

		current_text = strings.TrimSpace(temp_cell_address.String())

		if _, found := value2key[current_text]; found {
			fact_table = append(fact_table, float64(value2key[current_text]))
		} else {
			value2key[current_text] = key
			key2value[key] = current_text
			fact_table = append(fact_table, float64(key))
			key++
		}

		temp_cell_address.Reset()
		n += total_column + extra_line_br_char
	}
	
	return fact_table, key2value, value2key
}

func CellNumber(total_column int, extra_line_br_char int, current_column int, cell_address []uint64, bytestream []byte) ([]float64, map[uint64]string, map[string]uint64) {

	var current_text string
	var fact_table []float64
	var temp_cell_address strings.Builder
	var n int = current_column
	key2value := make(map[uint64]string)
	value2key := make(map[string]uint64)

	n += total_column + extra_line_br_char

	for n < len(cell_address)-1 {
		for x := cell_address[n]; x < cell_address[n+1]-1; x++ {
			temp_cell_address.WriteString(string(bytestream[x]))
		}		
		current_text = temp_cell_address.String()
		current_number, _ := strconv.ParseFloat(current_text, 64)
		fact_table = append(fact_table, current_number)
		temp_cell_address.Reset()
		n += total_column + extra_line_br_char
	}
	
	return fact_table, key2value, value2key
}

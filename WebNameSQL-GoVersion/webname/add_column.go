package webname

import (
	"math"
	"strconv"
	"strings"
)

func AddColumn(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string) {

	var parameter, column, setting, source, arrow_calc_function, add_column_name, return_table_name string
	var join_table_name, join_column_name []string
	//arrow_calc_function := map[string]int{"COUNT": 0, "SUM": 1, "MAX": 2, "MIN": 3, "AVERAGE": 4, "ADD": 5, "SUBTRACT": 6, "MULTIPLY": 7, "DIVIDE": 8, "COMBINETEXT": 12}
	var precision int
	source = strings.ToUpper(task.current_table)
	return_table_name = task.current_table
	var error_message string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
			return_table_name = source
		} else if strings.Contains(parameter, "$Column") {
			column = setting
		} else if strings.Contains(strings.ToUpper(parameter), "=>ADD($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Add"
		} else if strings.Contains(strings.ToUpper(parameter), "=>SUBTRACT($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Subtract"
		} else if strings.Contains(strings.ToUpper(parameter), "=>MULTIPLY($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Multiply"
		} else if strings.Contains(strings.ToUpper(parameter), "=>DIVIDE($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Divide"
		} else if strings.Contains(strings.ToUpper(parameter), "=>COMBINETEXT($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "CombineText"
		} else if strings.Contains(strings.ToUpper(parameter), "=>COMPOSITKEY($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "CompositKey"
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		} else if strings.Contains(strings.ToUpper(parameter), "($SETTING)") {
			join_table_name = append(join_table_name, GetParameter(parameter))
			join_column_name = append(join_column_name, setting)
		}
	}

	var result_table WebNameTable

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "COMPUTECOLUMN" {
		result_table = ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "NUMBER2TEXT" {
		result_table = Number2Text2(table_store, source, column)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "POSITIVENEGATIVE2DC" {
		arrow_calc_function = "PositiveNegative2DC"
		result_table = ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "NEGATIVEPOSITIVE2DC" {
		arrow_calc_function = "NegativePositive2DC"
		result_table = ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "DC2POSITIVENEGATIVE" {
		arrow_calc_function = "DC2PositiveNegative"
		result_table = ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "DC2NEGATIVEPOSITIVE" {
		arrow_calc_function = "DC2NegativePositive"
		result_table = ComputeColumn(table_store, source, column, arrow_calc_function, add_column_name, precision)	

		if len(error_message) > 0 {
			println(error_message)
		}
	}
	return result_table, return_table_name
}

func ComputeColumn(table_store map[string]WebNameTable, source string, column string, arrow_calc_function string, add_column_name string, precision int) WebNameTable {

	var column_name = String2Slice(column, false)

	var column_id int
	var temp float64
	var current_text string
	var reference_column []int
	negative_key2text := make(map[int]string)

	for x := 0; x < len(column_name); x++ {

		if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
			column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
			reference_column = append(reference_column, column_id)
		} else if strings.Contains(column_name[x], "\"") {
			reference_column = append(reference_column, (x - 9999))
			negative_key2text[(x - 9999)] = strings.ReplaceAll(column_name[x], "\"", "")
		}
	}

	var fact_table []float64
	var result_total_column int
	var result_column_name, result_data_type []string
	result_upper_column_name2id := make(map[string]int)
	result_fact_table := make(map[int][]float64)
	var result_table WebNameTable
	var are_all_positive bool
	var are_all_negative bool
	add_text_column := map[string]int{"CombineText": 0, "PositiveNegative2DC": 1, "NegativePositive2DC": 2, "DC2PositiveNegative": 3, "DC2NegativePositive": 4, "CompositKey": 5}
	replace_number_column := map[string]int{"PositiveNegative2DC": 0, "NegativePositive2DC": 1, "DC2PositiveNegative": 2, "DC2NegativePositive": 3}

	if _, found := add_text_column[arrow_calc_function]; found {
		result_key2value := make(map[int]map[uint64]string)
		result_value2key := make(map[int]map[string]uint64)
		key2value := make(map[uint64]string)
		value2key := make(map[string]uint64)
		var key uint64 = 0
		var factor []float64
		var current_factor_balance float64
		var factor_balance []float64
		var reverse_factor_balance []float64
		var key2value_count int

		var revised_fact_table WebNameTable

		if arrow_calc_function == "CompositKey" {

			for x := 0; x < len(column_name); x++ {
				key2value_count = len(table_store[source].key2value[reference_column[x]])
				factor = append(factor, math.Floor(math.Log10(float64(key2value_count))+1))
			}

			for x := len(column_name) - 1; x >= 0; x-- {

				if x == len(column_name)-1 {
					reverse_factor_balance = append(reverse_factor_balance, 0)
				} else {
					reverse_factor_balance = append(reverse_factor_balance, current_factor_balance)
				}
				current_factor_balance = current_factor_balance + factor[x]
			}

			for x := len(reverse_factor_balance) - 1; x >= 0; x-- {
				factor_balance = append(factor_balance, math.Pow(10, reverse_factor_balance[x]))
			}

			var composit_key float64

			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				composit_key = 0
				for x := 0; x < len(column_name); x++ {
					composit_key = composit_key + (table_store[source].fact_table[reference_column[x]][y]+1)*factor_balance[x]
				}

				fact_table = append(fact_table, composit_key)
			}

		} else if arrow_calc_function == "PositiveNegative2DC" || arrow_calc_function == "NegativePositive2DC" {
			revised_fact_table = CopyOneTable(table_store, source)

			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				if table_store[source].fact_table[reference_column[0]][y] > 0 {
					are_all_positive = true
					are_all_negative = false
				} else if table_store[source].fact_table[reference_column[0]][y] < 0 {
					are_all_positive = false
					are_all_negative = true
				} else if table_store[source].fact_table[reference_column[0]][y] == 0 {
					are_all_positive = true
					are_all_negative = true
				}

				for x := 1; x < len(column_name); x++ {
					if table_store[source].fact_table[reference_column[x]][y] > 0 {
						are_all_positive = are_all_positive && true
						are_all_negative = are_all_negative && false
					} else if table_store[source].fact_table[reference_column[x]][y] < 0 {
						are_all_positive = are_all_positive && false
						are_all_negative = are_all_negative && true
					} else if table_store[source].fact_table[reference_column[x]][y] == 0 {
						are_all_positive = are_all_positive && true
						are_all_negative = are_all_negative && true
					}
				}

				for x := 0; x < len(column_name); x++ {
					if are_all_negative == true {
						revised_fact_table.fact_table[reference_column[x]][y] = table_store[source].fact_table[reference_column[x]][y] * -1
					} else {
						revised_fact_table.fact_table[reference_column[x]][y] = table_store[source].fact_table[reference_column[x]][y]
					}
				}

				if are_all_positive == true {
					if arrow_calc_function == "PositiveNegative2DC" {
						current_text = "D"
					} else if arrow_calc_function == "NegativePositive2DC" {
						current_text = "C"
					}
					if _, found := value2key[current_text]; found {
						fact_table = append(fact_table, float64(value2key[current_text]))
					} else {
						value2key[current_text] = key
						key2value[key] = current_text
						fact_table = append(fact_table, float64(key))
						key += 1
					}
				} else if are_all_negative == true {

					if arrow_calc_function == "PositiveNegative2DC" {
						current_text = "C"
					} else if arrow_calc_function == "NegativePositive2DC" {
						current_text = "D"
					}
					if _, found := value2key[current_text]; found {
						fact_table = append(fact_table, float64(value2key[current_text]))
					} else {
						value2key[current_text] = key
						key2value[key] = current_text
						fact_table = append(fact_table, float64(key))
						key += 1
					}

				} else {
					if arrow_calc_function == "PositiveNegative2DC" {
						current_text = "D"
					} else if arrow_calc_function == "NegativePositive2DC" {
						current_text = "C"
					}
					if _, found := value2key[current_text]; found {
						fact_table = append(fact_table, float64(value2key[current_text]))
					} else {
						value2key[current_text] = key
						key2value[key] = current_text
						fact_table = append(fact_table, float64(key))
						key += 1
					}
				}
			}
			add_column_name = "D/C"

		} else if arrow_calc_function == "DC2PositiveNegative" || arrow_calc_function == "DC2NegativePositive" {
			revised_fact_table = CopyOneTable(table_store, source)
			DC := make(map[string]float64)

			var DC_columnID int = table_store[source].upper_column_name2id["D/C"]

			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				if table_store[source].fact_table[reference_column[0]][y] > 0 {
					are_all_positive = true
					are_all_negative = false
				} else if table_store[source].fact_table[reference_column[0]][y] < 0 {
					are_all_positive = false
					are_all_negative = true
				} else if table_store[source].fact_table[reference_column[0]][y] == 0 {
					are_all_positive = true
					are_all_negative = true
				}

				for x := 1; x < len(column_name); x++ {
					if table_store[source].fact_table[reference_column[x]][y] > 0 {
						are_all_positive = are_all_positive && true
						are_all_negative = are_all_negative && false
					} else if table_store[source].fact_table[reference_column[x]][y] < 0 {
						are_all_positive = are_all_positive && false
						are_all_negative = are_all_negative && true
					} else if table_store[source].fact_table[reference_column[x]][y] == 0 {
						are_all_positive = true
						are_all_negative = true
					}
				}

				for x := 0; x < len(column_name); x++ {
					if are_all_negative == true || are_all_positive == true {

						if arrow_calc_function == "DC2PositiveNegative" {
							DC["D"] = 1
							DC["C"] = -1
						} else if arrow_calc_function == "DC2NegativePositive" {
							DC["D"] = -1
							DC["C"] = 1
						}
						var temp = table_store[source].fact_table[DC_columnID][y]
						revised_fact_table.fact_table[reference_column[x]][y] = table_store[source].fact_table[reference_column[x]][y] * DC[table_store[source].key2value[DC_columnID][uint64(temp)]]
					}
				}
			}

		} else if arrow_calc_function == "CombineText" {
			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				if reference_column[0] >= 0 {
					temp = table_store[source].fact_table[reference_column[0]][y]

					if table_store[source].data_type[reference_column[0]] == "Text" {
						current_text = table_store[source].key2value[reference_column[0]][uint64(temp)]
					} else {
						current_text = strconv.FormatFloat(temp, 'f', -1, 64)
					}
				} else {
					current_text = negative_key2text[(0 - 9999)]
				}

				for x := 1; x < len(column_name); x++ {

					if reference_column[x] >= 0 {
						temp = table_store[source].fact_table[reference_column[x]][y]

						if table_store[source].data_type[reference_column[x]] == "Text" {
							current_text = current_text + table_store[source].key2value[reference_column[x]][uint64(temp)]
						} else {
							current_text = current_text + strconv.FormatFloat(temp, 'f', -1, 64)
						}
					} else {
						current_text = current_text + negative_key2text[(x-9999)]
					}
				}

				if _, found := value2key[current_text]; found {
					fact_table = append(fact_table, float64(value2key[current_text]))
				} else {
					value2key[current_text] = key
					key2value[key] = current_text
					fact_table = append(fact_table, float64(key))
					key += 1
				}
			}
		}

		for x := 0; x < table_store[source].total_column; x++ {
			result_column_name = append(result_column_name, table_store[source].column_name[x])
			result_upper_column_name2id[strings.ToUpper(table_store[source].column_name[x])] = x
			result_data_type = append(result_data_type, table_store[source].data_type[x])
			result_key2value[x] = table_store[source].key2value[x]
			result_value2key[x] = table_store[source].value2key[x]

			if _, found := replace_number_column[arrow_calc_function]; found {
				result_fact_table[x] = revised_fact_table.fact_table[x]
			} else {
				result_fact_table[x] = table_store[source].fact_table[x]
			}
		}

		if arrow_calc_function != "DC2PositiveNegative" && arrow_calc_function != "DC2NegativePositive" {
			result_column_name = append(result_column_name, add_column_name)
			result_total_column = len(result_column_name)
			result_upper_column_name2id[strings.ToUpper(add_column_name)] = table_store[source].total_column

			if arrow_calc_function == "CompositKey" {
				result_data_type = append(result_data_type, "Number")
			} else {
				result_data_type = append(result_data_type, "Text")
			}
			result_fact_table[table_store[source].total_column] = fact_table
			result_key2value[table_store[source].total_column] = key2value
			result_value2key[table_store[source].total_column] = value2key
		}

		result_table.total_column = result_total_column
		result_table.extra_line_br_char = table_store[source].extra_line_br_char
		result_table.column_name = result_column_name
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.data_type = result_data_type
		result_table.fact_table = result_fact_table
		result_table.key2value = result_key2value
		result_table.value2key = result_value2key

		if arrow_calc_function == "DC2PositiveNegative" || arrow_calc_function == "DC2NegativePositive" {
			result_table_store := make(map[string]WebNameTable)
			result_table_store["DC"] = result_table
			result_table = RemoveColumn(result_table_store, "DC", "D/C")
		}

		return result_table

	} else {
		if arrow_calc_function == "Add" {
			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[reference_column[0]][y]

				for x := 1; x < len(column_name); x++ {
					var power = math.Pow(10., float64(precision))
					temp = math.Round((temp+table_store[source].fact_table[reference_column[x]][y])*power) / power
				}
				fact_table = append(fact_table, temp)
			}

		} else if arrow_calc_function == "Subtract" {
			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[reference_column[0]][y]

				for x := 1; x < len(column_name); x++ {
					var power = math.Pow(10., float64(precision))
					temp = math.Round((temp-table_store[source].fact_table[reference_column[x]][y])*power) / power
				}
				fact_table = append(fact_table, temp)
			}
		} else if arrow_calc_function == "Multiply" {
			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[reference_column[0]][y]

				for x := 1; x < len(column_name); x++ {
					var power = math.Pow(10., float64(precision))
					temp = math.Round(temp*table_store[source].fact_table[reference_column[x]][y]*power) / power
				}
				fact_table = append(fact_table, temp)
			}
		} else if arrow_calc_function == "Divide" {
			for y := 0; y < len(table_store[source].fact_table[0]); y++ {
				temp = table_store[source].fact_table[reference_column[0]][y]

				for x := 1; x < len(column_name); x++ {
					var power = math.Pow(10., float64(precision))
					temp = math.Round(temp/table_store[source].fact_table[reference_column[x]][y]*power) / power
				}
				fact_table = append(fact_table, temp)
			}
		}

		for x := 0; x < table_store[source].total_column; x++ {
			result_column_name = append(result_column_name, table_store[source].column_name[x])
			result_upper_column_name2id[strings.ToUpper(table_store[source].column_name[x])] = x
			result_data_type = append(result_data_type, table_store[source].data_type[x])
			result_fact_table[x] = table_store[source].fact_table[x]
		}

		result_column_name = append(result_column_name, add_column_name)
		result_total_column = len(result_column_name)
		result_upper_column_name2id[strings.ToUpper(add_column_name)] = table_store[source].total_column
		result_data_type = append(result_data_type, "Number")
		result_fact_table[table_store[source].total_column] = fact_table

		result_table.total_column = result_total_column
		result_table.extra_line_br_char = table_store[source].extra_line_br_char
		result_table.column_name = result_column_name
		result_table.upper_column_name2id = result_upper_column_name2id
		result_table.data_type = result_data_type
		result_table.fact_table = result_fact_table
		result_table.key2value = table_store[source].key2value
		result_table.value2key = table_store[source].value2key
	}
	return result_table
}

func Number2Text2(table_store map[string]WebNameTable, source string, column string) WebNameTable {

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

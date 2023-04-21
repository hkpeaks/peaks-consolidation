package webname

import (
	"math"
	"strconv"
	"strings"
)

func Cell(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) (string, string, int, string) {

	var parameter, column, column_setting, setting, source, arrow_calc_function, add_column_name string
	var precision int
	var result_cell string	
	source = strings.ToUpper(task.current_table)
	
	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		//	println(x, parameter, setting)
		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
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
		} else if strings.Contains(strings.ToUpper(parameter), "=>COUNT($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Count"
		} else if strings.Contains(strings.ToUpper(parameter), "=>SUM($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Sum"
		} else if strings.Contains(strings.ToUpper(parameter), "=>AVERAGE($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Average"
		} else if strings.Contains(strings.ToUpper(parameter), "=>MIN($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Min"
		} else if strings.Contains(strings.ToUpper(parameter), "=>MAX($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "Max"
		} else if strings.Contains(strings.ToUpper(parameter), "=>CELLADDRESS($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "CellAddress"
		} else if strings.Contains(strings.ToUpper(parameter), "=>COMBINETEXT($SETTING)") {
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
			arrow_calc_function = "CombineText"
		} else if strings.Contains(strings.ToUpper(parameter), "[$SETTING]") {
			column = GetParameter(parameter)
			column_setting = setting
			precision = GetPrecision(setting)
			var suffix = "." + strconv.Itoa(precision)
			add_column_name = strings.ReplaceAll(setting, suffix, "")
		}
	}

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "COMPUTECELL" {
		result_cell = ComputeCell(table_store, cell_store, source, column, arrow_calc_function, add_column_name, precision)
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "TABLE2CELL" {
		result_cell = Table2Cell(table_store, cell_store, source, column, column_setting, arrow_calc_function, add_column_name, precision)
	}

	return result_cell, add_column_name, precision, source
}

func ComputeCell(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, arrow_calc_function string, add_column_name string, precision int) string {

	var current_text, next_text, result_cell string
	var column_name = String2Slice(column, false)
	var temp, next_temp float64

	if arrow_calc_function != "CombineText" {
		temp = 0
		if _, found := cell_store[strings.ToUpper(column_name[0])]; found {
			number, _ := strconv.ParseFloat(cell_store[strings.ToUpper(column_name[0])], 64)
			temp = number
		} else {
			number2, _ := strconv.ParseFloat(strings.ReplaceAll(column_name[0], "\"", ""), 64)
			temp = number2
		}

		for x := 1; x < len(column_name); x++ {
			next_temp = 0
			if _, found := cell_store[strings.ToUpper(column_name[x])]; found {
				number, _ := strconv.ParseFloat(cell_store[strings.ToUpper(column_name[x])], 64)
				next_temp = number
			} else {
				number2, _ := strconv.ParseFloat(strings.ReplaceAll(column_name[x], "\"", ""), 64)
				next_temp = number2
			}

			var power = math.Pow(10., float64(precision))

			if arrow_calc_function == "Add" {
				temp = math.Round((temp+next_temp)*power) / power
			} else if arrow_calc_function == "Subtract" {
				temp = math.Round((temp-next_temp)*power) / power
			} else if arrow_calc_function == "Multiply" {
				temp = math.Round(temp*next_temp*power) / power
			} else if arrow_calc_function == "Divide" {
				temp = math.Round(temp/next_temp*power) / power
			}
		}
		result_cell = strconv.FormatFloat(temp, 'f', -1, 64)

	} else if arrow_calc_function == "CombineText" {

		if _, found := cell_store[strings.ToUpper(column_name[0])]; found {
			current_text = cell_store[strings.ToUpper(column_name[0])]
		} else {
			current_text = strings.ReplaceAll(column_name[0], "\"", "")
		}

		for x := 1; x < len(column_name); x++ {
			if _, found := cell_store[strings.ToUpper(column_name[x])]; found {
				next_text = cell_store[strings.ToUpper(column_name[x])]
			} else {
				next_text = strings.ReplaceAll(column_name[x], "\"", "")
			}
			current_text = current_text + next_text
		}

		result_cell = current_text
	}
	return result_cell
}

func Table2Cell(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, arrow_calc_function string, add_column_name string, precision int) string {	
	var result_cell string
	var column_name = String2Slice(column, false)
	var column_id int
	var temp, sum float64
	var reference_column []int	

	Sum := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				temp = temp + table_store[source].fact_table[reference_column[x]][y]
			}
		}
	}

	Count := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				temp = temp + 1
			}
		}
	}

	Average := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				sum = sum + table_store[source].fact_table[reference_column[x]][y]
			}
		}

		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				temp = temp + 1
			}
		}
		temp = sum / temp
	}

	
	Max := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		temp = table_store[source].fact_table[reference_column[0]][0]
		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				if table_store[source].fact_table[reference_column[x]][y] > temp {
					temp = table_store[source].fact_table[reference_column[x]][y]
				}
			}
		}
	}

	Min := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		temp = table_store[source].fact_table[reference_column[0]][0]
		for y := 0; y < len(table_store[source].fact_table[0]); y++ {
			for x := 0; x < len(column_name); x++ {
				if table_store[source].fact_table[reference_column[x]][y] < temp {
					temp = table_store[source].fact_table[reference_column[x]][y]
				}
			}
		}
	}
	
	CellAddress := func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int) {
		number, err := strconv.Atoi(column_setting)

		if err != nil {
			if strings.ToUpper(column_setting) == "FIRST" {
				number = 1
			} else if strings.ToUpper(column_setting) == "LAST" {
				number = len(table_store[source].fact_table[0])
			}
		}
		temp = table_store[source].fact_table[reference_column[0]][number - 1]
	}

	for x := 0; x < len(column_name); x++ {
		if _, found := table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]; found {
			column_id = table_store[source].upper_column_name2id[strings.ToUpper(column_name[x])]
			reference_column = append(reference_column, column_id)
		}
	}

	run := make(map[string](func(table_store map[string]WebNameTable, cell_store map[string]string, source string, column string, column_setting string, add_column_name string, precision int)))
	run["Sum"] = Sum
	run["Count"] = Count
	run["Average"] = Average
	run["Max"] = Max
	run["Min"] = Min
	run["CellAddress"] = CellAddress

	run[arrow_calc_function](table_store, cell_store, source, column, column_setting, add_column_name, precision)

	if table_store[source].data_type[reference_column[0]] == "Text" {
		result_cell = table_store[source].key2value[reference_column[0]][uint64(temp)]
	} else {

		var power = math.Pow(10., float64(precision))
		temp = math.Round(temp*power) / power
		result_cell = strconv.FormatFloat(temp, 'f', -1, 64)
	}

	return result_cell
}

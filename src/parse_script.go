package peaks

import (
	"os"
	"strconv"
	"strings"
	"log"
	"fmt"
	"regexp"
)

func ValidateRule(bytestream []byte) (int, Rule) {

	var rule = BuildRuleMap(bytestream)
	var recognized_rule, recognized_rule_table = RecognizeRule(rule)
	var error_count = CompareRule(rule, recognized_rule, recognized_rule_table)

	if error_count > 0 {
		println("Error found in", error_count, "rules")
	}

	return error_count, rule
}

func BuildRuleMap(bytestream []byte) Rule {

	var original_command, current_command, current_block, current_parameter string
	current_block = "Main"

	var block_sequence, command_sequence, parameter_sequence []string
	block2command_sequence := make(map[string][]string)
	command2parameter_sequence := make(map[string][]string)
	block_sequence = append(block_sequence, current_block)

	parameter := make(map[string]string)
	command := make(map[string]map[string]string)
	block := make(map[string]map[string]map[string]string)
	var original_rule []string
	command2rule := make(map[string]string)

	//arrow_calc_function := map[string]int{"COUNT": 0, "SUM": 1, "MAX": 2, "MIN": 3, "AVERAGE": 4, "ADD": 5, "SUBTRACT": 6, "MULTIPLY": 7, "DIVIDE": 8, "COMBINETEXT": 12}
	//	other_calc_function := map[string]int{"DEBIT": 0, "CREDIT": 1, "BALANCE": 2, "METHOD": 3, "DATECOLUMN": 4, "STARTMONTH": 5, "X": 16, "Y": 17}

	var temp_rule strings.Builder
	var temp_rule2 strings.Builder
	var curly_bracket, block_char, tilde, arrow, vertical_bar, at bool
	var command_index int
	var is_remark bool = false

	special_char := map[byte]string{35: "#", 40: "(", 41: ")", 64: "@", 91: "[", 93: "]", 123: "{", 124: "|", 125: "}", 126: "~", 10: "LF", 13: "CR"}
	var copy_string strings.Builder

	for x := 0; x < len(bytestream); x++ {

		if is_remark == true {
			if bytestream[x] == 10 || bytestream[x] == 13 {

				is_remark = false
				x++
			}

		} else if is_remark == false {

			if bytestream[x] == 35 {
				if x < len(bytestream)-1 {
					if bytestream[x+1] == 35 {
						is_remark = true
						bytestream[x] = 32
						bytestream[x+1] = 32
					}
				}
			}

			if bytestream[x] != 10 && bytestream[x] != 13 {
				if curly_bracket == false && bytestream[x] == 32 {
				} else {
					temp_rule2.WriteString(string(bytestream[x]))
				}
			}

			if bytestream[x] == 61 && bytestream[x+1] == 62 { // =>
				arrow = true
				if len(strings.TrimSpace(temp_rule.String())) > 0 {
					if vertical_bar == true {
						parameter["$Column"] = strings.TrimSpace(temp_rule.String())
						parameter_sequence = append(parameter_sequence, "$Column")
					} else {
						parameter["{$Column"] = strings.TrimSpace(temp_rule.String())
						parameter_sequence = append(parameter_sequence, "{$Column")
					}
				}
				x++
				temp_rule2.WriteString(string(bytestream[x]))
				temp_rule.Reset()

			} else if _, found := special_char[bytestream[x]]; found {

				if bytestream[x] == 123 { // {
					copy_string.Reset()
					curly_bracket = true
					arrow = false
					at = false
					vertical_bar = false
					original_command = strings.TrimSpace(temp_rule.String())
					current_command = strconv.Itoa(command_index) + ":" + original_command
					command_index++
					command_sequence = append(command_sequence, current_command)
					temp_rule.Reset()
				}

				if curly_bracket == true {
					if bytestream[x] == 124 && len(parameter) == 0 { // |
						vertical_bar = true
						parameter["{$Source|"] = strings.TrimSpace(temp_rule.String())
						parameter_sequence = append(parameter_sequence, "{$Source|")
						temp_rule.Reset()

					} else if bytestream[x] == 64 { // @
						at = true
						if len(strings.TrimSpace(temp_rule.String())) > 0 {
							if len(parameter) == 0 {
								parameter["{$Column"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "{$Column")
							} else if len(parameter) == 1 {
								parameter["$Column"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "$Column")
							}
						}
						temp_rule.Reset()

					} else if bytestream[x] == 40 { // (

						current_parameter = strings.TrimSpace(temp_rule.String())

						if len(parameter) == 0 {
							current_parameter = "{" + current_parameter
						}

						if arrow == true {
							current_parameter = "=>" + current_parameter
							arrow = false
						}

						if at == true {
							current_parameter = "@" + current_parameter
							at = false
						}

						current_parameter = current_parameter + "($Setting)"
						temp_rule.Reset()

					} else if bytestream[x] == 91 { // [

						current_parameter = strings.TrimSpace(temp_rule.String())

						if len(parameter) == 0 {
							current_parameter = "{" + current_parameter
						}

						if arrow == true {
							current_parameter = "=>" + current_parameter
							arrow = false
						}

						if at == true {
							current_parameter = "@" + current_parameter
							at = false
						}

						current_parameter = current_parameter + "[$Setting]"
						temp_rule.Reset()

					} else if bytestream[x] == 41 || bytestream[x] == 93 { // )
						at = false

						for bytestream[x+1] == 32 || bytestream[x+1] == 10 || bytestream[x+1] == 13 {
							x++
						}

						if bytestream[x+1] == 125 { // }
							current_parameter = current_parameter + "}"
						}

						if _, found := parameter[current_parameter]; found {
							copy_string.WriteString("`")
							current_parameter = copy_string.String() + current_parameter
						}

						parameter[current_parameter] = strings.TrimSpace(temp_rule.String())
						parameter_sequence = append(parameter_sequence, current_parameter)

						temp_rule.Reset()

					} else if bytestream[x] == 126 { // ~
						tilde = true

						if len(strings.TrimSpace(temp_rule.String())) > 0 {
							if at == true {
								at = false
								if len(parameter) == 0 {
									parameter["{@$Common"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "{@$Common")
								} else {
									parameter["@$Common"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "@$Common")
								}
							} else if strings.ToUpper(original_command) == "READFILE" || strings.ToUpper(original_command) == "WRITEFILE" || strings.ToUpper(original_command) == "SPLITFILE" || strings.ToUpper(original_command) == "ONECOLUMN2CACHE" {
								if len(parameter) == 0 {
									parameter["{$Source"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "{$Source")

								}
							} else if vertical_bar == true && arrow == false {
								parameter["$Column"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "$Column")
							} else if vertical_bar == false && arrow == false {
								parameter["{$Column"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "{$Column")
							} else if arrow == true {
								parameter["=>$Effect"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "=>$Effect")
							}
						}
						temp_rule.Reset()

					} else if bytestream[x] == 125 { // }
						curly_bracket = false
						original_rule = append(original_rule, temp_rule2.String())
						command2rule[current_command] = temp_rule2.String()
						temp_rule2.Reset()
						if tilde == true { // ~
							parameter["~$Return}"] = strings.TrimSpace(temp_rule.String())
							parameter_sequence = append(parameter_sequence, "~$Return}")
							tilde = false
						} else if len(strings.TrimSpace(temp_rule.String())) > 0 {
							if at == true {
								at = false
								if len(parameter) == 0 {
									parameter["{@$Common}"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "{@$Common}")
								} else {
									parameter["@$Common}"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "@$Common}")
								}
							} else if len(parameter) == 0 && strings.ToUpper(current_command) == strconv.Itoa(command_index-1)+":PROCESS" {
								parameter["{$Run}"] = strings.TrimSpace(temp_rule.String())
								parameter_sequence = append(parameter_sequence, "{$Run}")
							} else {
								if vertical_bar == true {
									parameter["$Column}"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "$Column}")
								} else if arrow == true {
									parameter["=>$Effect}"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "=>$Effect}")
								} else {
									parameter["{$Column}"] = strings.TrimSpace(temp_rule.String())
									parameter_sequence = append(parameter_sequence, "{$Column}")
								}
							}
						} else {
							if len(parameter) == 0 {
								parameter["{$Nil}"] = ""
								parameter_sequence = append(parameter_sequence, "{$Nil}")
							}
						}
						command[current_command] = parameter
						command2parameter_sequence[current_command] = parameter_sequence
						parameter = make(map[string]string)
						parameter_sequence = nil
						temp_rule.Reset()
					}
				} else {

					if bytestream[x] == 35 { // #
						block_char = true

						temp_rule.Reset()

					} else if block_char == true && (bytestream[x] == 10 || bytestream[x] == 13) {
						original_rule = append(original_rule, temp_rule2.String())
						temp_rule2.Reset()
						block2command_sequence[current_block] = command_sequence
						block[current_block] = command
						current_block = strings.TrimSpace(temp_rule.String())
						block_sequence = append(block_sequence, current_block)
						command_sequence = nil
						command = make(map[string]map[string]string)
						block_char = false
						temp_rule.Reset()
					}
				}

			} else {
				temp_rule.WriteString(string(bytestream[x]))
			}
		}
	}

	block[current_block] = command
	block2command_sequence[current_block] = command_sequence

	var result_rule Rule
	result_rule.bytestream = bytestream
	result_rule.original_rule = original_rule
	result_rule.command2rule = command2rule
	result_rule.block_sequence = block_sequence
	result_rule.command_sequence = command_sequence
	result_rule.parameter_sequence = parameter_sequence
	result_rule.block2command_sequence = block2command_sequence
	result_rule.command2parameter_sequence = command2parameter_sequence
	result_rule.block = block

	return result_rule
}

func RecognizeRule(rule Rule) ([]string, strings.Builder) {

	var recognized_rule []string
	var combining_rule strings.Builder

	var recognized_rule_table strings.Builder
	var current_command, revised_command, current_block, current_parameter, current_setting, current_parameter_setting string
	//parameter := map[string]int{"Source": 1, "Column": 2, "Common": 3, "Setting": 4, "Return": 5, "Run": 6, "Nil": 7}

	for i := 0; i < len(rule.block_sequence); i++ {
		current_block = rule.block_sequence[i]
		if current_block != "Main" {
			recognized_rule = append(recognized_rule, "#"+current_block+"\n")
		}

		for j := 0; j < len(rule.block2command_sequence[rule.block_sequence[i]]); j++ {
			current_command = rule.block2command_sequence[current_block][j]
			revised_command = RemoveCommandIndex(current_command)
			combining_rule.WriteString(revised_command)

			for k := 0; k < len(rule.command2parameter_sequence[current_command]); k++ {
				current_parameter = rule.command2parameter_sequence[current_command][k]
				current_setting = rule.block[current_block][current_command][current_parameter]

				if strings.Contains(current_parameter, "$Source") {
					current_parameter_setting = strings.Replace(current_parameter, "$Source", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Column") {
					current_parameter_setting = strings.Replace(current_parameter, "$Column", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Common") {
					current_parameter_setting = strings.Replace(current_parameter, "$Common", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Setting") {
					current_parameter_setting = strings.Replace(current_parameter, "$Setting", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Effect") {
					current_parameter_setting = strings.Replace(current_parameter, "$Effect", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Return") {
					current_parameter_setting = strings.Replace(current_parameter, "$Return", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Run") {
					current_parameter_setting = strings.Replace(current_parameter, "$Run", current_setting, 1)
				} else if strings.Contains(current_parameter, "$Nil") {
					current_parameter_setting = strings.Replace(current_parameter, "$Nil", "", 1)
				}
				combining_rule.WriteString(current_parameter_setting)
				recognized_rule_table.WriteString(current_block + "," + revised_command + "," + current_parameter + "," + current_setting + "\n")
			}
			recognized_rule = append(recognized_rule, combining_rule.String())
			combining_rule.Reset()
		}
	}

	return recognized_rule, recognized_rule_table
}

func CompareRule(rule Rule, recognized_rule []string, recognized_rule_table strings.Builder) int {

	String2File := func (file_name string, text string) {

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

	var text strings.Builder
	var trim_original_rule, trim_recognized_rule string
	rule.output_debug = true

	if strings.Contains(recognized_rule_table.String(), "OutputDebug") {
		rule.output_debug = true
	} else {
		rule.output_debug = false
	}

	if rule.output_debug == true { // for debug purpose

		String2File("recognized_rule_table.csv", recognized_rule_table.String())

		for i := 0; i < len(rule.original_rule); i++ {
			text.WriteString(rule.original_rule[i] + "\n")
		}

		String2File("original_rule.txt", text.String())
		text.Reset()

		for i := 0; i < len(recognized_rule); i++ {
			text.WriteString(recognized_rule[i] + "\n")
		}

		String2File("recoginized_rule.txt", text.String())

		text.Reset()
	}

	var error_count int

	for i := 0; i < len(rule.original_rule); i++ {
		trim_original_rule = strings.TrimSpace(strings.ReplaceAll(rule.original_rule[i], " ", ""))
		trim_recognized_rule = strings.TrimSpace(strings.ReplaceAll(recognized_rule[i], " ", ""))
		trim_recognized_rule = strings.ReplaceAll(trim_recognized_rule, "`", "")

		if trim_original_rule != trim_recognized_rule {

			error_count++

			if rule.output_debug == true { // for debug purpose
				text.WriteString("Recognized Rule: " + trim_recognized_rule + "\n")
				text.WriteString("Original Rule   : " + trim_original_rule)
			}

			println("Original Rule  : ", trim_original_rule)
			println("Recognized Rule: ", trim_recognized_rule, "\n")
			println("** Fail to recognize this rule **")
			println("** Usually due to incomplete () or {} **")

			os.Exit(0)
		}
	}

	if rule.output_debug == true { // for debug purpose
		String2File("rule_variance.txt", text.String())
	}

	return error_count
}

func CurrentRule(table_partition_store map[string]map[int]Cache, task Task, rule Rule) InternalRule {

	GetParameter := func(command string) string {
		var parameter string
		if strings.Contains(command, "[") {
			parameter = command[0:strings.Index(command, "[")]
		} else if strings.Contains(command, "(") {
			parameter = command[0:strings.Index(command, "(")]
		}
		parameter = strings.ReplaceAll(parameter, "{", "")
		return parameter
	}

	SelectCurrentTableByName := func(table_partition_store map[string]map[int]Cache, source_table_name string) *Cache {
		var result_table = table_partition_store[strings.ToUpper(source_table_name)][0]
		return &result_table
	}

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

	if len(ir.source_table_name) == 0 {
		ir.source_table_name = rule.current_query.source_table_name
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
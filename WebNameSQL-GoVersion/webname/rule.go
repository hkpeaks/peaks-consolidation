package webname

import (
	"strconv"
	"strings"
)

type Rule struct {
	bytestream                 []byte
	original_rule              []string
	command2rule               map[string]string
	block_sequence             []string
	command_sequence           []string
	parameter_sequence         []string
	block2command_sequence     map[string][]string
	command2parameter_sequence map[string][]string
	block                      map[string]map[string]map[string]string
	csv_separator              int
}

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

	special_char := map[byte]string{35: "#", 40: "(", 41: ")", 64: "@", 91: "[", 93: "]", 123: "{", 124: "|", 125: "}", 126: "~", 10: "LF", 13: "CR"}
	var copy_string strings.Builder

	for x := 0; x < len(bytestream); x++ {

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
						} else if strings.ToUpper(original_command) == "CSV2WEB" || strings.ToUpper(original_command) == "ONECOLUMN2WEB" {
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

	var text strings.Builder
	var trim_original_rule, trim_recognized_rule string

	if 1 == 1 { // for debug purpose

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
			println("Original Rule  : ", trim_original_rule)
			println("Recognized Rule: ", trim_recognized_rule, "\n")

			if 1 == 2 { // for debug purpose
				text.WriteString("Recognized Rule: " + trim_recognized_rule + "\n")
				text.WriteString("Original Rule   : " + trim_original_rule)
			}
		}
	}

	if 1 == 2 { // for debug purpose
		String2File("rule_variance.txt", text.String())
	}

	return error_count
}

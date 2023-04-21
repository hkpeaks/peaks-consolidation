package webname

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func Rule2Web(table_store map[string]WebNameTable, task Task, rule Rule) (WebNameTable, string) {

	var parameter2, setting, column, source, return_file_name string

	source = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter2 = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter2]

		if strings.Contains(parameter2, "$Source") {
			source = strings.ToUpper(setting)
		} else if strings.Contains(parameter2, "$Column") {
			column = strings.ToUpper(setting)
		} else if strings.Contains(parameter2, "$Return") {
			return_file_name = setting
		}
	}

	var key0, key1, key2, key3 uint64

	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)

	var fact_table_file_name []float64
	key2value_file_name := make(map[uint64]string)
	value2key_file_name := make(map[string]uint64)

	var fact_table_block []float64
	key2value_block := make(map[uint64]string)
	value2key_block := make(map[string]uint64)

	var fact_table_command []float64
	key2value_command := make(map[uint64]string)
	value2key_command := make(map[string]uint64)

	var fact_table_rule []float64
	key2value_rule := make(map[uint64]string)
	value2key_rule := make(map[string]uint64)

	var current_text, current_file string
	var file_path_column_id = table_store[source].upper_column_name2id[column]

	for y := 0; y < len(table_store[source].fact_table[file_path_column_id]); y++ {
		current_file = table_store[source].key2value[file_path_column_id][uint64(table_store[source].fact_table[file_path_column_id][y])]

		//println(current_file)

		var bytestream = File2Bytestream(current_file)

		var original_command, current_command, current_block string
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

		var temp_rule strings.Builder
		var temp_rule2 strings.Builder
		var curly_bracket, block_char bool
		var command_index int

		for x := 0; x < len(bytestream); x++ {

			if bytestream[x] != 10 && bytestream[x] != 13 {
				if curly_bracket == false && bytestream[x] == 32 {
				} else {
					temp_rule2.WriteString(string(bytestream[x]))
				}
			}

			if bytestream[x] == 123 { // {
				curly_bracket = true
				original_command = strings.TrimSpace(temp_rule.String())
				current_command = strconv.Itoa(command_index) + ":" + original_command
				command_index++
				command_sequence = append(command_sequence, current_command)
				temp_rule.Reset()

			} else if bytestream[x] == 125 { // }
				curly_bracket = false
				original_rule = append(original_rule, temp_rule2.String())
				command2rule[current_command] = temp_rule2.String()

				//println(current_block, RemoveCommandIndex(current_command), temp_rule2.String())

				current_text = strings.TrimSpace(current_file)

				if _, found := value2key_file_name[current_text]; found {
					fact_table_file_name = append(fact_table_file_name, float64(value2key_file_name[current_text]))
				} else {
					value2key_file_name[current_text] = key0
					key2value_file_name[key0] = current_text
					fact_table_file_name = append(fact_table_file_name, float64(key0))
					key0++
				}

				current_text = strings.TrimSpace(current_block)

				if _, found := value2key_block[current_text]; found {
					fact_table_block = append(fact_table_block, float64(value2key_block[current_text]))
				} else {
					value2key_block[current_text] = key1
					key2value_block[key1] = current_text
					fact_table_block = append(fact_table_block, float64(key1))
					key1++
				}

				current_text = strings.TrimSpace(RemoveCommandIndex(current_command))

				if _, found := value2key_command[current_text]; found {
					fact_table_command = append(fact_table_command, float64(value2key_command[current_text]))
				} else {
					value2key_command[current_text] = key2
					key2value_command[key2] = current_text
					fact_table_command = append(fact_table_command, float64(key2))
					key2++
				}

				current_text = strings.TrimSpace(temp_rule2.String())

				if _, found := value2key_rule[current_text]; found {
					fact_table_rule = append(fact_table_rule, float64(value2key_rule[current_text]))
				} else {
					value2key_rule[current_text] = key3
					key2value_rule[key3] = current_text
					fact_table_rule = append(fact_table_rule, float64(key3))
					key3++
				}

				temp_rule2.Reset()
				command[current_command] = parameter
				command2parameter_sequence[current_command] = parameter_sequence
				parameter = make(map[string]string)
				parameter_sequence = nil
				temp_rule.Reset()

			} else if bytestream[x] == 35 { // #
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
			} else {
				temp_rule.WriteString(string(bytestream[x]))
			}
		}

		block[current_block] = command
		block2command_sequence[current_block] = command_sequence
	}
	/*
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
	*/

	result_fact_table[0] = fact_table_file_name
	result_fact_table[1] = fact_table_block
	result_fact_table[2] = fact_table_command
	result_fact_table[3] = fact_table_rule
	result_key2value[0] = key2value_file_name
	result_key2value[1] = key2value_block
	result_key2value[2] = key2value_command
	result_key2value[3] = key2value_rule
	result_value2key[0] = value2key_file_name
	result_value2key[1] = value2key_block
	result_value2key[2] = value2key_command
	result_value2key[3] = value2key_rule

	var result_table WebNameTable

	result_table.fact_table = result_fact_table
	result_table.key2value = result_key2value
	result_table.value2key = result_value2key
	result_table.column_name = []string{"Rule File Path", "Block", "Command", "Rule"}
	result_table.data_type = []string{"Text", "Text", "Text", "Text"}
	result_table.extra_line_br_char = 0
	result_table.upper_column_name2id = map[string]int{"FILE NAME": 0, "BLOCK": 1, "COMMAND": 2, "RULE": 3}
	result_table.total_column = 4

	return result_table, return_file_name
}

func CurrentTable(table_store map[string]WebNameTable, task Task, rule Rule) (string, string) {

	var parameter, setting, current_table, error_message string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Column") {
			current_table = setting
		}
	}

	if _, found := table_store[strings.ToUpper(current_table)]; found {

	} else {
		error_message = "Table name " + "\"" + current_table + "\"" + " not found"
	}

	return current_table, error_message
}

func EnableDisable(task Task, rule Rule) (map[string]bool, string) {
	var parameter, setting, error_message string
	is_enable := make(map[string]bool)
	enable_disable_function := map[string]int{"MESSAGE2SCREEN": 0, "MESSAGE2FILE": 1}
	var function_missing strings.Builder
	var error int = 0

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = strings.ToUpper(rule.block[task.block][task.command][parameter])

		if _, found := enable_disable_function[strings.ToUpper(setting)]; found {

			if strings.Contains(parameter, "$Column") {

				current_upper_command := strings.ToUpper(RemoveCommandIndex(task.command))
				if current_upper_command == "ENABLE" {
					is_enable[setting] = true
				} else if current_upper_command == "DISABLE" {
					is_enable[setting] = false
				}
			}
		} else {

			if error > 0 {
				function_missing.WriteString(", \"" + setting + "\"")
			} else {
				function_missing.WriteString("\"" + setting + "\"")
			}
			error++
		}
	}

	if error > 0 {
		error_message = "Enable/disable function " + function_missing.String() + " not found"
	}

	return is_enable, error_message
}

func Process(task Task, rule Rule) string {

	var parameter, setting, run string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Run") {
			run = setting
		}
	}

	return run
}

func ReplaceRule(task Task, rule Rule) []byte {

	var parameter, column, setting string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Column") {
			column = setting
		}
	}

	var replace_rule_map = String2Map(column, false)
	text := string(rule.bytestream)

	for k, v := range replace_rule_map {
		text = strings.ReplaceAll(text, k, v)
	}

	bytestream := []byte(text)
	return bytestream
}

func FileList2Web(task Task, rule Rule) (WebNameTable, string, string) {

	var result_table WebNameTable

	var parameter, setting, return_table_name, current_text, include string
	file_setting := make(map[string]string)

	var key0, key1, key2 uint64

	result_fact_table := make(map[int][]float64)
	result_key2value := make(map[int]map[uint64]string)
	result_value2key := make(map[int]map[string]uint64)

	var fact_table_source_folder []float64
	key2value_source_folder := make(map[uint64]string)
	value2key_source_folder := make(map[string]uint64)

	var fact_table_file_path []float64
	key2value_file_path := make(map[uint64]string)
	value2key_file_path := make(map[string]uint64)

	var fact_table_file_name []float64
	key2value_file_name := make(map[uint64]string)
	value2key_file_name := make(map[string]uint64)

	var error_message string

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(strings.ToUpper(parameter), "FOLDERPATH($SETTING)") {
			file_setting["FolderPath"] = setting
		} else if strings.Contains(strings.ToUpper(parameter), "FILEFILTER($SETTING)") {
			file_setting["FileFilter"] = setting
		} else if strings.Contains(strings.ToUpper(parameter), "SUBDIRECTORY($SETTING)") {
			file_setting["Subdirectory"] = setting
		} else if strings.Contains(parameter, "$Return") {
			return_table_name = setting
		}
	}

	if len(file_setting["FolderPath"]) > 3 {

		_, dir_err := os.Stat(file_setting["FolderPath"])
		if dir_err != nil {
			error_message = "Folder path " + "\"" + file_setting["FolderPath"] + "\"" + " is not a vaild folder path"
		} else {

			err := filepath.Walk(file_setting["FolderPath"], func(path string, info os.FileInfo, err error) error {

				matched, err := filepath.Match(file_setting["FileFilter"], info.Name())
				if err != nil {
					fmt.Println(err)
					return err
				}
				if matched {
					if strings.ToUpper(file_setting["Subdirectory"]) == "INCLUDE" {
						include = strings.TrimSpace(path)
					} else {
						include = file_setting["FolderPath"] + strings.TrimSpace(info.Name())
					}

					if strings.TrimSpace(path) == include {
						current_text = strings.TrimSpace(file_setting["FolderPath"])

						if _, found := value2key_source_folder[current_text]; found {
							fact_table_source_folder = append(fact_table_source_folder, float64(value2key_source_folder[current_text]))
						} else {
							value2key_source_folder[current_text] = key0
							key2value_source_folder[key0] = current_text
							fact_table_source_folder = append(fact_table_source_folder, float64(key0))
							key0++
						}

						current_text = strings.TrimSpace(path)

						if _, found := value2key_file_path[current_text]; found {
							fact_table_file_path = append(fact_table_file_path, float64(value2key_file_path[current_text]))
						} else {
							value2key_file_path[current_text] = key1
							key2value_file_path[key1] = current_text
							fact_table_file_path = append(fact_table_file_path, float64(key1))
							key1++
						}

						current_text = strings.TrimSpace(info.Name())

						if _, found := value2key_file_name[current_text]; found {
							fact_table_file_name = append(fact_table_file_name, float64(value2key_file_name[current_text]))
						} else {
							value2key_file_name[current_text] = key2
							key2value_file_name[key2] = current_text
							fact_table_file_name = append(fact_table_file_name, float64(key2))
							key2++
						}
					}
				}
				return nil
			})
			if err != nil {
				fmt.Println(err)
			}

			result_fact_table[0] = fact_table_source_folder
			result_fact_table[1] = fact_table_file_path
			result_fact_table[2] = fact_table_file_name
			result_key2value[0] = key2value_source_folder
			result_key2value[1] = key2value_file_path
			result_key2value[2] = key2value_file_name
			result_value2key[0] = value2key_source_folder
			result_value2key[1] = value2key_file_path
			result_value2key[2] = value2key_file_name

			result_table.fact_table = result_fact_table
			result_table.key2value = result_key2value
			result_table.value2key = result_value2key
			result_table.column_name = []string{"Source Folder", "Result File Path", "Result File Name"}
			result_table.data_type = []string{"Text", "Text", "Text"}
			result_table.extra_line_br_char = 0
			result_table.upper_column_name2id = map[string]int{"SOURCE FOLDER": 0, "RESULT FILE PATH": 1, "RESULT FILE NAME": 2}
			result_table.total_column = 3

		}
	} else {
		error_message = "Folder path must be at lease 4 characters, root folder is not permitted"
	}

	return result_table, return_table_name, error_message
}

package peaks

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func RunTask(rule Rule) {

	var task Task
	var current_command, command string

	current_command = rule.block2command_sequence["Main"][0]
	command = RemoveCommandIndex(current_command)

	if strings.ToUpper(command) == "REPLACERULE" {
		task.block = "Main"
		task.command = current_command
		var revised_bytestream = ReplaceRule(task, rule)

		var error_count, rule = ValidateRule(revised_bytestream)
		if error_count == 0 {
			RunTaskByCommand(rule)
		}
	} else if strings.ToUpper(command) == "DEBUG" {
		RunTaskByCommand(rule)

	} else {
		RunTaskByCommand(rule)
	}
}

func RunTaskByCommand(rule Rule) {

	var command, current_command, current_table, current_file string
	var current_streamMB int = 500
	var current_thread int = 10
	var current_force_integer bool = false
	var current_read_csv_delimiter byte = 44
	var current_write_csv_delimiter byte = 44
	var error_message, message, message2, upper_command, return_file_name, return_table_name string
	var column_count int
	var task Task
	cell_store := make(map[string]string)
	table_store := make(map[string]Cache)
	table_partition := make(map[int]Cache)
	table_partition_store := make(map[string]map[int]Cache)
	var total_row int32

	run := make(map[string](func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule)))
	is_enable := map[string]bool{"MESSAGE2SCREEN": true, "MESSAGE2FILE": true}

	ReadFile := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {
		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		source_table := ReadFile(task, rule)
		table_partition = *&source_table.table_partition
		return_table_name = source_table.table_name
		total_row = source_table.total_row
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["READFILE"] = ReadFile
	run["ONECOLUMN2CACHE"] = ReadFile

	CurrentSetting := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {
		table_partition, current_rule := CurrentSetting(task, rule)
		current_streamMB = current_rule.streamMB
		current_thread = current_rule.thread
		current_force_integer = current_rule.force_integer
		current_read_csv_delimiter = current_rule.read_csv_delimiter
		error_message = table_partition.error_message
	}

	run["CURRENTSETTING"] = CurrentSetting

	CurrentTable := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {
		current_table, error_message = CurrentTable(table_store, task, rule)
		current_file = "Nil"
	}

	run["CURRENTTABLE"] = CurrentTable

	JoinKeyValue := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		rule.write_csv_delimiter = current_write_csv_delimiter
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["JOINKEYVALUE"] = JoinKeyValue

	BuildKeyValue := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["BUILDKEYVALUE"] = BuildKeyValue

	Select := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["SELECT"] = Select

	SelectUnmatch := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["SELECTUNMATCH"] = SelectUnmatch

	Distinct := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["DISTINCT"] = Distinct

	GroupBy := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.streamMB = current_streamMB
		rule.thread = current_thread
		rule.read_csv_delimiter = current_read_csv_delimiter
		rule.force_integer = current_force_integer
		table_partition := *Streaming(table_partition_store, task, rule)
		total_row = table_partition.total_row
		error_message = table_partition.error_message
		return_table_name = table_partition.table_name
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition.table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["GROUPBY"] = GroupBy

	SplitFile := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {

		rule.read_csv_delimiter = current_read_csv_delimiter
		return_table_name = SplitFile(task, rule)
		table_partition_store[strings.ToUpper(return_table_name)] = *&table_partition
		current_table = return_table_name
		current_file = "Nil"
	}

	run["SPLITFILE"] = SplitFile

	WriteFile := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {
		return_file_name, total_row, error_message = WriteFile(*&table_partition_store, task, rule)
		current_file = return_file_name
	}

	run["WRITEFILE"] = WriteFile	

	run_block := func(current_block string) {

		for i := 0; i < len(rule.block2command_sequence[current_block]); i++ {

			current_command = rule.block2command_sequence[current_block][i]
			command = RemoveCommandIndex(current_command)
			upper_command = strings.ToUpper(command)

			if upper_command != "REPLACERULE" && upper_command != "OUTPUTDEBUG" {

				if is_enable["MESSAGE2SCREEN"] == true {
					current_upper_command := strings.ToUpper(RemoveCommandIndex(current_command))
					if current_upper_command != "DISABLE" && current_upper_command != "ENABLE" {
						println(rule.command2rule[current_command])
					}
				}

				task.block = current_block
				task.command = current_command
				task.current_table = current_table

				if _, found := run[upper_command]; found {
					run[upper_command](table_partition_store, cell_store, task, rule)					

					if len(error_message) > 0 {
						println("Error: " + error_message)
						break
					}
				} else {

					fmt.Println("** Command", command, "not found **")
					os.Exit(0)

				}

				if len(current_table) > 0 {

					if current_file == "Nil" {

						message = current_table + "(" + strconv.Itoa(table_partition_store[strings.ToUpper(current_table)][0].total_column) + " x " + strconv.Itoa(int(total_row)) + ")"
					} else {
						if upper_command == "WRITEFILE" && column_count != -999 {
							message = current_file + "(" + strconv.Itoa(table_partition_store[strings.ToUpper(current_table)][0].total_column) + " x " + strconv.Itoa(int(total_row)) + ")"
						} else {
							message = current_file + "(" + strconv.Itoa(table_partition_store[strings.ToUpper(current_table)][0].total_column) + " x " + strconv.Itoa(int(total_row)) + ")"
						}
					}
					if is_enable["MESSAGE2SCREEN"] == true {
						print("  ", message)

					}
				}

				if upper_command == "COMPUTECELL" || upper_command == "TABLE2CELL" {
					if is_enable["MESSAGE2SCREEN"] == true {
						println(message2, "\n")
					}
				} else {
					if is_enable["MESSAGE2SCREEN"] == true {
						println("")
						println("")
					}
				}
			}
		}
	}

	PROCESS := func(table_partition_store map[string]map[int]Cache, cell_store map[string]string, task Task, rule Rule) {
		var current_block = Process(task, rule)
		current_table = "Nil"
		current_file = "Nil"
		for i := 0; i < len(rule.block2command_sequence[current_block]); i++ {
			current_command = rule.block2command_sequence[current_block][i]
			command = current_command[(strings.Index(current_command, ":") + 1):(len(current_command))]
			if is_enable["MESSAGE2SCREEN"] == true {
				println(rule.command2rule[current_command])
			}

			upper_command = strings.ToUpper(command)

			task.block = current_block
			task.command = current_command
			task.current_table = current_table

			/*
				if current_file == "Nil" {
					message = current_table + "(" + strconv.Itoa(table_store[current_table].total_column) + " x " + strconv.Itoa(len(table_store[current_table].fact_table[0])) + ")"
				} else {
					if upper_command == "CACHE2TABLE" && column_count != -999 {
						message = current_file + "(" + strconv.Itoa(column_count) + " x " + strconv.Itoa(len(table_store[current_table].fact_table[0])) + ")"
					} else {
						message = current_file + "(" + strconv.Itoa(table_store[current_table].total_column) + " x " + strconv.Itoa(len(table_store[current_table].fact_table[0])) + ")"
					}
				}

				println(" ", message, "\n")
			*/
		}
	}

	run["PROCESS"] = PROCESS

	run_block("Main")

}

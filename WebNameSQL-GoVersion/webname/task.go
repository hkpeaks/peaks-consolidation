package webname

import (
	"fmt"	
	"strconv"
	"strings"
)

type Task struct {
	block         string
	command       string
	current_table string
}

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
	} else {
		RunTaskByCommand(rule)
	}
}

func RunTaskByCommand(rule Rule) {

	var cell_name, cell_value, command, current_command, current_table, current_file string
	var error_message, message, message2, upper_command, return_file_name, return_table_name string
	var column_count, precision int
	var task Task
	var web WebNameTable
	cell_store := make(map[string]string)
	table_store := make(map[string]WebNameTable)
	run := make(map[string](func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule)))
	is_enable := map[string]bool{"MESSAGE2SCREEN": true, "MESSAGE2FILE": true}

	AddColumn := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name = AddColumn(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["COMPUTECOLUMN"] = AddColumn
	run["DC2NEGATIVEPOSITIVE"] = AddColumn
	run["DC2POSITIVENEGATIVE"] = AddColumn
	run["POSITIVENEGATIVE2DC"] = AddColumn
	run["NEGATIVEPOSITIVE2DC"] = AddColumn	

	AddRow := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name, error_message = AddRow(table_store, cell_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["APPENDROW"] = AddRow
	run["COMBINETABLEBYCOMMONCOLUMN"] = AddRow
	run["MERGETABLE"] = AddRow

	AmendColumn := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name, error_message = AmendColumn(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["AMENDCOLUMNNAME"] = AmendColumn
	run["NUMBER2TEXT"] = AmendColumn
	run["REMOVECOLUMN"] = AmendColumn
	run["REVERSENUMBER"] = AmendColumn
	run["SELECTCOLUMN"] = AmendColumn

	Cell := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		cell_value, cell_name, precision, return_table_name = Cell(table_store, cell_store, task, rule)
		cell_store[strings.ToUpper(cell_name)] = cell_value
		current_table = return_table_name
		current_file = "Nil"

		f, err := strconv.ParseFloat(cell_value, 64)

		if err == nil {
			message2 = "  " + cell_name + " = " + fmt.Sprintf("%."+strconv.Itoa(precision)+"f", f)
		} else {
			message2 = "  " + cell_name + " = " + cell_value
		}
	}

	run["COMPUTECELL"] = Cell
	run["TABLE2CELL"] = Cell

	CopyTable := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name = CopyTable(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["COPYTABLE"] = CopyTable

	CSVFile2Web := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		rule.csv_separator = 44
		web, return_table_name = CSVFile2Web(task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["CSV2WEB"] = CSVFile2Web
	run["ONECOLUMN2WEB"] = CSVFile2Web

	CurrentTable := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		current_table, error_message = CurrentTable(table_store, task, rule)
		current_file = "Nil"
	}

	run["CURRENTTABLE"] = CurrentTable

	EnableDisable := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		is_enable, error_message = EnableDisable(task, rule)
		current_file = "Nil"
	}

	run["ENABLE"] = EnableDisable
	run["DISABLE"] = EnableDisable

	FileList2Web := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name, error_message = FileList2Web(task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["FILELIST2WEB"] = FileList2Web	

	JoinTable := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name = Join(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["JOINTABLE"] = JoinTable

	Rule2Web := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name = Rule2Web(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["RULE2WEB"] = Rule2Web

	Summary := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		web, return_table_name, error_message = Summary(table_store, task, rule)
		table_store[strings.ToUpper(return_table_name)] = web
		current_table = return_table_name
		current_file = "Nil"
	}

	run["DISTINCT"] = Summary
	run["GROUPBY"] = Summary
	run["CROSSTAB"] = Summary

	Web2File := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
		return_file_name, column_count, error_message = Web2File(table_store, task, rule)
		current_file = return_file_name
	}

	run["WEB2CSV"] = Web2File
	run["WEB2HTML"] = Web2File
	run["WEB2JSON"] = Web2File
	run["WEB2XML"] = Web2File
	run["WEB2ONECOLUMN"] = Web2File

	run_block := func(current_block string) {

		for i := 0; i < len(rule.block2command_sequence[current_block]); i++ {

			current_command = rule.block2command_sequence[current_block][i]
			command = RemoveCommandIndex(current_command)
			upper_command = strings.ToUpper(command)

			if upper_command != "REPLACERULE" {

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
					run[upper_command](table_store, cell_store, task, rule)
					if len(error_message) > 0 {
						println("Error: " + error_message)
						break
					}
				}

				if len(current_table) > 0 {

					if current_file == "Nil" {
						message = current_table + "(" + strconv.Itoa(table_store[strings.ToUpper(current_table)].total_column) + " x " + strconv.Itoa(len(table_store[strings.ToUpper(current_table)].fact_table[0])) + ")"
					} else {
						if upper_command == "WEB2CSV" && column_count != -999 {
							message = current_file + "(" + strconv.Itoa(column_count) + " x " + strconv.Itoa(len(table_store[strings.ToUpper(current_table)].fact_table[0])) + ")"
						} else {
							message = current_file + "(" + strconv.Itoa(table_store[strings.ToUpper(current_table)].total_column) + " x " + strconv.Itoa(len(table_store[strings.ToUpper(current_table)].fact_table[0])) + ")"
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

	PROCESS := func(table_store map[string]WebNameTable, cell_store map[string]string, task Task, rule Rule) {
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

			if _, found := run[upper_command]; found {
				run[upper_command](table_store, cell_store, task, rule)
			}
			/*
				if current_file == "Nil" {
					message = current_table + "(" + strconv.Itoa(table_store[current_table].total_column) + " x " + strconv.Itoa(len(table_store[current_table].fact_table[0])) + ")"
				} else {
					if upper_command == "WEB2CSV" && column_count != -999 {
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


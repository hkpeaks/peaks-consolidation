package peaks

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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

func ValidateRule(bytestream []byte) (int, Rule) {

	var rule = BuildRuleMap(bytestream)
	var recognized_rule, recognized_rule_table = RecognizeRule(rule)
	var error_count = CompareRule(rule, recognized_rule, recognized_rule_table)

	if error_count > 0 {
		println("Error found in", error_count, "rules")
	}

	return error_count, rule
}

func Streaming(table_partition_store map[string]map[int]Cache, task Task, rule Rule) *CachePartition {

	ir := CurrentRule(table_partition_store, task, rule)

	var error_message string
	result_table_partition := make(map[int]Cache)
	var input_folder string = "Input/"

	if strings.Contains(strings.ToUpper(ir.source_table_name), ".CSV") {

		if strings.Contains(ir.source_table_name, "/") || strings.Contains(ir.source_table_name, "\\") {
			input_folder = ""
		}

		file, err := os.Open(input_folder + ir.original_source_table_name)

		if err != nil {
			fmt.Println()
			fmt.Println("** File", ir.source_table_name, "not found **")
			os.Exit(0)
		}
		defer file.Close()

		file2, err := os.Stat(input_folder + ir.original_source_table_name)

		if err != nil {
			log.Fatalf("unable to read file: %v", err)
		}

		var total_byte = file2.Size()

		if total_byte < int64(rule.streamMB*1000000) {

			source_table := ReadFile(task, rule)

			for _, column := range ir.validate_all_column_name {

				if _, found := source_table.table_partition[0].upper_column_name2id[strings.ToUpper(column)]; !found {

					if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
						if strings.ToUpper(column) != "$COLUMN" {
							fmt.Println("** Column", column, "not found **")
							os.Exit(0)
						}
					}
				}
			}

			ir.return_table_name = source_table.table_name
			table_partition := *&source_table.table_partition
			table_partition_store[strings.ToUpper(ir.return_table_name)] = *&table_partition

			ir.source_table_name = ir.return_table_name

			result_table := *CurrentStream(1, &table_partition_store, task, rule, ir)

			result_table_partition = *&result_table.table_partition
			error_message = result_table.error_message

			if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

				var f *os.File
				var csv_string strings.Builder

				csv_string.WriteString(result_table.table_partition[0].column_name[0])

				for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
					csv_string.WriteString(",")
					csv_string.WriteString(result_table.table_partition[0].column_name[x])
				}

				csv_string.WriteString("\r\n")

				write_partition := make(map[int]CachePartition)
				write_partition[1] = *&result_table

				CurrentWriteFile(ir, csv_string, 1, write_partition, err, f)
			}

		} else {

			result_table_partition = *ParallelStreaming(table_partition_store, task, rule, ir, total_byte, file)
		}
	} else {

		table_partition_store = *ParallelAddCellAddressToTablePartition(table_partition_store, ir, rule)

		result_table := *CurrentStream(0, &table_partition_store, task, rule, ir)
		result_table_partition = *&result_table.table_partition
		error_message = result_table.error_message

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

			var f *os.File
			var csv_string strings.Builder

			csv_string.WriteString(result_table.table_partition[0].column_name[0])

			for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
				csv_string.WriteString(",")
				csv_string.WriteString(result_table.table_partition[0].column_name[x])
			}

			csv_string.WriteString("\r\n")

			write_partition := make(map[int]CachePartition)
			write_partition[1] = *&result_table

			CurrentWriteFile(ir, csv_string, 1, write_partition, nil, f)
		}
	}

	var total_row int32

	for current_partition := 0; current_partition < len(result_table_partition); current_partition++ {
		total_row += result_table_partition[current_partition].partition_row
	}

	var result_table CachePartition
	result_table.table_partition = *&result_table_partition
	result_table.table_name = ir.return_table_name
	result_table.error_message = error_message
	result_table.total_row = total_row

	return &result_table
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

	var replace_rule_map = String2Map(column, false, rule)
	text := string(rule.bytestream)

	for k, v := range replace_rule_map {
		text = strings.ReplaceAll(text, k, v)
	}

	bytestream := []byte(text)
	return bytestream
}

func CurrentSetting(task Task, rule Rule) (CachePartition, Rule) {

	var parameter, setting string
	var error error
	var error_message string = ""

	for x := 0; x < len(rule.command2parameter_sequence[task.command]); x++ {
		parameter = rule.command2parameter_sequence[task.command][x]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(strings.ToUpper(parameter), "STREAMMB($SETTING)") {
			rule.streamMB, error = strconv.Atoi(setting)
		} else if strings.Contains(strings.ToUpper(parameter), "THREAD($SETTING)") {
			rule.thread, error = strconv.Atoi(setting)
		} else if strings.Contains(strings.ToUpper(parameter), "FORCEINTEGER($SETTING)") {
			if strings.ToUpper(setting) == "TRUE" || strings.ToUpper(setting) == "Y" {
				rule.force_integer = true
			}
		} else if strings.Contains(strings.ToUpper(parameter), "OUTPUTDEBUG($SETTING)") {
			if strings.ToUpper(setting) == "TRUE" || strings.ToUpper(setting) == "Y" {
				rule.output_debug = true
			}
		} else if strings.Contains(strings.ToUpper(parameter), "READCSVDELIMITER($SETTING)") {
			b := []byte(strings.Trim(setting, " "))
			rule.read_csv_delimiter = b[0]
		} else if strings.Contains(strings.ToUpper(parameter), "WRITECSVDELIMITER($SETTING)") {
			b := []byte(strings.Trim(setting, " "))
			rule.write_csv_delimiter = b[0]
		}
	}

	if error == nil {

	} else {
		error_message = "error"
	}

	if rule.read_csv_delimiter == 0 {
		rule.read_csv_delimiter = 44
	}

	if rule.write_csv_delimiter == 0 {
		rule.write_csv_delimiter = 44
	}

	if rule.thread < 1 {
		rule.thread = 10
	} else if rule.thread > 1000 {
		rule.thread = 1000
	}

	if rule.streamMB < 1 {
		rule.streamMB = 500
	} else if rule.streamMB > 10000 {
		rule.streamMB = 10000
	}

	var table_partition CachePartition
	table_partition.error_message = error_message

	return table_partition, rule
}

func CurrentTable(table_store map[string]Cache, task Task, rule Rule) (string, string) {

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

func String2Map(column string, is_upper bool, rule Rule) map[string]string {

	amend_column_name := make(map[string]string)
	var current_upper_column_name string
	var temp_rule strings.Builder
	column_byte := []byte(column)

	for x := 0; x < len(column_byte); x++ {
		if column_byte[x] == 61 { // =
			if is_upper == true {
				current_upper_column_name = strings.TrimSpace(strings.ToUpper(temp_rule.String()))
			} else {
				current_upper_column_name = strings.TrimSpace(temp_rule.String())
			}
			temp_rule.Reset()
		} else if column_byte[x] == 44 {
			amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())
			temp_rule.Reset()
		} else {
			temp_rule.WriteString(string(column_byte[x]))
		}
	}

	amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())

	return amend_column_name
}

func CurrentExtraction(extraction_batch map[int]int, query_batch map[int]int, bytestream_partition map[int][]byte, streaming_count int, rule Rule, file *os.File, partition_address map[int]int64) {

	for batch := 1; batch <= streaming_count; batch++ {

		for len(bytestream_partition)/rule.thread > 2 {
			time.Sleep(100 * time.Millisecond)
		}

		for current_partition := (batch - 1) * rule.thread; current_partition < rule.thread*batch; current_partition++ {
			partition.Lock()
			bytestream_partition[current_partition] = *ReadByte(current_partition, partition_address, file)
			partition.Unlock()
		}

		extraction_batch[batch] = 1
	}
}

func ParallelStreaming(table_partition_store map[string]map[int]Cache, task Task, rule Rule, ir InternalRule, total_byte int64, file *os.File) *map[int]Cache {

	rule.byte_per_stream = int64(rule.streamMB * 1000000)
	var streaming_count int = int(total_byte / rule.byte_per_stream)

	if streaming_count < 1 {
		streaming_count = 1
	}

	var total_partition int = streaming_count * rule.thread

	if total_byte > 50000000 {
		rule.thread = total_partition
	} else {
		rule.thread = 10
	}

	ds := *FileStructure(rule, total_byte, file)
	rule.thread = total_partition / streaming_count
	fmt.Println("Total Bytes:", total_byte, "| Total Batches of Stream:", streaming_count)
	partition_address := *PartitionAddress(ds)
	bytestream_partition := make(map[int][]byte)
	extraction_batch := make(map[int]int)
	query_batch := make(map[int]int)
	combine_table_partition := make(map[int]Cache)
	result_table_partition_store := make(map[string]map[int]Cache)
	result_table_partition := make(map[int]Cache)

	go CurrentExtraction(extraction_batch, query_batch, bytestream_partition, streaming_count, rule, file, partition_address)

	var total_row int32
	var total_column int
	var csv_string strings.Builder
	var f *os.File
	var err error
	write_partition := make(map[int]CachePartition)
	var p int

	for batch := 1; batch <= streaming_count; batch++ {
		for current_partition := 0; current_partition < rule.thread*(len(query_batch)-1); current_partition++ {
			partition.Lock()
			if _, found := bytestream_partition[current_partition]; found {
				delete(bytestream_partition, current_partition)
			}
			partition.Unlock()
		}

		for batch > len(extraction_batch) {
			time.Sleep(100 * time.Millisecond)
		}

		table_partition := make(map[int]Cache)

		var mutex2 sync.Mutex
		var parallel2 sync.WaitGroup
		parallel2.Add(rule.thread)
		var i int = 0

		for current_partition := (batch - 1) * rule.thread; current_partition < rule.thread*batch; current_partition++ {
			go func(current_partition int) {
				defer parallel2.Done()
				result_table := *CellAddress2(ds, false, current_partition, rule, ir.source_table_name, bytestream_partition)
				mutex2.Lock()
				if batch == 1 {
					for _, column := range ir.validate_all_column_name {

						if _, found := result_table.upper_column_name2id[strings.ToUpper(column)]; !found {

							if strings.ToUpper(column) != "NULL" && strings.Trim(column, "") != "" {
								if strings.ToUpper(column) != "$COLUMN" {
									fmt.Println("** Column", column, "not found **")
									os.Exit(0)
								}
							}
						}
					}
				}
				table_partition[i] = *&result_table
				i++
				mutex2.Unlock()
			}(current_partition)
		}

		parallel2.Wait()

		table_partition_store[strings.ToUpper(ir.return_table_name)] = *&table_partition
		ir.source_table_name = ir.return_table_name

		result_table := *CurrentStream(batch, &table_partition_store, task, rule, ir)

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {
		} else {
			ir.column_id_seq = result_table.ir.column_id_seq
			ir.column_name = result_table.ir.column_name
			result_table_partition = *&result_table.table_partition

			for i := 0; i < len(result_table_partition); i++ {

				if len(result_table_partition[i].bytestream) > 0 {
					combine_table_partition[p] = result_table_partition[i]
					p++
				}
			}
		}

		query_batch[batch] = 1
		fmt.Print(batch, " ")

		if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

			if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {

				for current_partition := 0; current_partition < len(result_table.table_partition); current_partition++ {
					total_row += result_table.table_partition[current_partition].partition_row
				}

				partition.Lock()
				write_partition[batch] = *&result_table
				partition.Unlock()

				for len(write_partition) > 5 {
					time.Sleep(100 * time.Millisecond)
				}

				if batch == 1 {

					total_column = len(result_table.table_partition[0].column_name)

					csv_string.WriteString(result_table.table_partition[0].column_name[0])

					for x := 1; x < len(result_table.table_partition[0].column_name); x++ {
						csv_string.WriteString(",")
						csv_string.WriteString(result_table.table_partition[0].column_name[x])
					}

					csv_string.WriteString("\r\n")

					go CurrentWriteFile(ir, csv_string, streaming_count, write_partition, err, f)
				}
			}
		}
	}

	if _, found := ir.full_streaming_command[strings.ToUpper(RemoveCommandIndex(task.command))]; found {

		if strings.Contains(strings.ToUpper(ir.return_table_name), ".CSV") {
			var result_table Cache
			result_table.partition_row = total_row
			result_table.total_column = total_column
			combine_table_partition[0] = result_table

			for len(write_partition) > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			return &combine_table_partition

		} else {

			for current_partition := 0; current_partition < len(combine_table_partition); current_partition++ {
				total_row += combine_table_partition[current_partition].partition_row
			}

			total_column = len(combine_table_partition[0].column_name)

			return &combine_table_partition
		}

	} else {

		result_table_partition_store["TEMP"] = *&combine_table_partition

		ir.source_table_name = "TEMP"
		column_id_seq, column_name, upper_column_name2id, data_type := AddColumnmNameWithSeq2(&result_table_partition_store, rule, ir)
		ir.column_id_seq = column_id_seq
		ir.column_name = column_name

		result_table_partition[0] = *CurrentCommand(1, combine_table_partition, 0, len(combine_table_partition), task, ir, rule, upper_column_name2id, data_type)

		return &result_table_partition
	}
}

func CurrentWriteFile(ir InternalRule, csv_string strings.Builder, streaming_count int, result_table map[int]CachePartition, err error, f *os.File) {

	var output_folder = "Output/"

	if strings.Contains(ir.return_table_name, "/") || strings.Contains(ir.return_table_name, "\\") {
		output_folder = ""
	}

	for batch := 1; batch <= streaming_count; batch++ {

		if batch == 1 {
			if output_folder != "" {
				if _, err := os.Stat(output_folder); errors.Is(err, os.ErrNotExist) {
					err := os.Mkdir(output_folder, os.ModePerm)
					if err != nil {
						log.Println(err)
					}
				}
			}

			f, err = os.Create(output_folder + ir.return_table_name)

			if err != nil {
				log.Fatal(err)
			}

			defer f.Close()

			_, err = f.WriteString(csv_string.String())
		}

		if _, found := result_table[batch]; found {

			for current_partition := 0; current_partition < len(result_table[batch].table_partition); current_partition++ {
				_, err = f.Write(result_table[batch].table_partition[current_partition].bytestream)
			}
			partition.Lock()
			delete(result_table, batch)
			partition.Unlock()

		} else {
			time.Sleep(100 * time.Millisecond)
			batch--
		}
	}
}

func Bytestream(bytestream_partition map[int][]byte, current_partition int) *[]byte {
	partition.Lock()
	bytestream := bytestream_partition[current_partition]
	partition.Unlock()
	return &bytestream
}

func CellAddress2(ds DataStructure, is_column_name_exist bool, current_partition int, rule Rule, source string, bytestream_partition map[int][]byte) *Cache {

	bytestream := *Bytestream(bytestream_partition, current_partition)
	partition_address := *PartitionAddress(ds)
	var buffer = partition_address[current_partition+1] - partition_address[current_partition]
	read_csv_delimiter := rule.read_csv_delimiter

	current_partition_address := make(map[int]int64)
	current_partition_address[0] = -1
	current_partition_address[1] = buffer

	var _double_quote_count int = 0

	cell_address := make([]uint32, 0, ds.estimated_cell)

	cell_address = append(cell_address, uint32(current_partition_address[0])+1)

	var start_byte int = int(current_partition_address[0]) + 1

	var end_byte int = int(current_partition_address[1])

	var row int32

	for x := start_byte; x < end_byte; x++ {
		if bytestream[x] == read_csv_delimiter {
			if _double_quote_count != 1 {
				cell_address = append(cell_address, uint32(x+1))
				_double_quote_count = 0
			}
		} else if bytestream[x] == 10 {
			cell_address = append(cell_address, uint32(x+1))
			row++
		} else if bytestream[x] == 13 {
			cell_address = append(cell_address, uint32(x+1))
		} else if bytestream[x] == 34 {
			_double_quote_count += 1
		}
	}

	var result_table Cache

	result_table.total_column = ds.total_column
	result_table.extra_line_br_char = ds.extra_line_br_char
	result_table.column_name = ds.column_name
	result_table.upper_column_name2id = ds.upper_column_name2id
	result_table.data_type = ds.data_type
	result_table.bytestream = bytestream
	result_table.partition_row = row
	result_table.cell_address = cell_address

	return &result_table
}

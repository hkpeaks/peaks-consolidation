package peaks

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func WriteFile(table_partition_store map[string]map[int]Cache, task Task, rule Rule) (string, int32, string) {

	var parameter, setting, source_table_name, source_table_name_upper, return_file_name, error_message string
	source_table_name_upper = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source_table_name_upper = strings.ToUpper(setting)
			source_table_name = setting
		} else if strings.Contains(parameter, "$Column") {
			//column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_file_name = setting
		}
	}

	var csv_string strings.Builder

	if _, found := table_partition_store[source_table_name_upper]; found {
	} else {
		fmt.Println()
		fmt.Println("** Table", source_table_name, "not found **")
		os.Exit(0)
	}

	if _, found := table_partition_store[source_table_name_upper]; found {
		csv_string.WriteString(table_partition_store[source_table_name_upper][0].column_name[0])
	} else {
		fmt.Println()
		fmt.Println("** Table", setting, "not found **")
		os.Exit(0)
	}

	for x := 1; x < len(table_partition_store[source_table_name_upper][0].column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(table_partition_store[source_table_name_upper][0].column_name[x])
	}

	csv_string.WriteString("\r\n")

	var output_folder = "Output/"

	if strings.Contains(return_file_name, "/") || strings.Contains(return_file_name, "\\") {
		output_folder = ""
	}

	if output_folder != "" {
		if _, err := os.Stat(output_folder); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(output_folder, os.ModePerm)
			if err != nil {
				log.Println(err)
			}
		}
	}

	f, err := os.Create(output_folder + return_file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err = f.WriteString(csv_string.String())

	var expand_factor int = 1

	if strings.Contains(return_file_name, "%ExpandBy") {
		if strings.Contains(return_file_name, "Time.csv") {
			expand := strings.Replace(return_file_name, "%ExpandBy", "", 1)
			expand = strings.Replace(expand, "Time.csv", "", 1)
			expand_factor, err = strconv.Atoi(expand)
			if err != nil {
				expand_factor = 1
			}
			
			if expand_factor < 1 {
				expand_factor = 1
			} else if expand_factor > 100 {
				expand_factor = 100
			}
		}
	}

	for x := 0; x < expand_factor; x++ {

		if expand_factor > 1 {
			fmt.Print(x+1, " ")
		}

		for current_partition := 0; current_partition < len(table_partition_store[source_table_name_upper]); current_partition++ {
			byte_stream := ByteStreamFromTableStore(table_partition_store, source_table_name_upper, current_partition)
			_, err = f.Write(*byte_stream)
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	var total_row int32 = 0

	for current_partition := 0; current_partition < len(table_partition_store[source_table_name_upper]); current_partition++ {
		total_row += table_partition_store[source_table_name_upper][current_partition].partition_row
	}

	return return_file_name, total_row, error_message
}

func ByteStreamFromTableStore(table_partition_store map[string]map[int]Cache, source_table_name string, current_partition int) *[]byte {

	result_table := table_partition_store[source_table_name][current_partition].bytestream
	return &result_table
}

func WriteCurrentStream(table_partition_store map[string]map[int]Cache, task Task, rule Rule) (string, int32, string) {

	var parameter, setting, source_table_name, return_file_name, error_message string
	//var column string
	source_table_name = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source_table_name = strings.ToUpper(setting)
		} else if strings.Contains(parameter, "$Column") {
			//column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_file_name = setting
		}
	}

	var csv_string strings.Builder

	csv_string.WriteString(table_partition_store[source_table_name][0].column_name[0])

	for x := 1; x < len(table_partition_store[source_table_name][0].column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(table_partition_store[source_table_name][0].column_name[x])
	}

	csv_string.WriteString("\r\n")

	if _, err := os.Stat("Output/"); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir("Output/", os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	f, err := os.Create("Output/" + return_file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err = f.WriteString(csv_string.String())

	for x := 0; x < 1; x++ {
		for current_partition := 0; current_partition < len(table_partition_store[source_table_name]); current_partition++ {
			byte_stream := ByteStreamFromTableStore(table_partition_store, source_table_name, current_partition)
			_, err = f.Write(*byte_stream)
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	var total_row int32 = 0

	for current_partition := 0; current_partition < len(table_partition_store[source_table_name]); current_partition++ {
		total_row += table_partition_store[source_table_name][current_partition].partition_row
	}

	return return_file_name, total_row, error_message
}

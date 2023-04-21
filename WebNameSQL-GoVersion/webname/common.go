package webname

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
)

func GetParameter(command string) string {
	var parameter string
	if strings.Contains(command, "[") {
		parameter = command[0:strings.Index(command, "[")]
	} else if strings.Contains(command, "(") {
		parameter = command[0:strings.Index(command, "(")]
	}
	parameter = strings.ReplaceAll(parameter, "{", "")
	return parameter
}

func GetPrecision(command string) int {
	var precision = command[(strings.Index(command, ".") + 1):(len(command))]
	int, err := strconv.Atoi(precision)

	if err != nil {
		int = 0
	}
	return int
}

func RemoveCommandIndex(command string) string {
	return command[(strings.Index(command, ":") + 1):(len(command))]
}

func String2File(file_name string, text string) {

	f, err := os.Create(file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	_, err2 := f.WriteString(text)

	if err2 != nil {
		log.Fatal(err2)
	}
}

func String2Folder(folder_path string, file_name string, text string) {

	if _, err := os.Stat("Output\\"); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(folder_path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	f, err := os.Create(folder_path + file_name)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	_, err2 := f.WriteString(text)

	if err2 != nil {
		log.Fatal(err2)
	}
}

func String2Map(column string, is_upper bool) map[string]string {

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
		} else if column_byte[x] == 44 { // ,
			amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())
			temp_rule.Reset()
		} else {
			temp_rule.WriteString(string(column_byte[x]))
		}
	}

	amend_column_name[current_upper_column_name] = strings.TrimSpace(temp_rule.String())

	return amend_column_name
}

func String2Slice(column string, is_upper bool) []string {

	var column_name []string
	var temp_rule strings.Builder
	column_byte := []byte(column)

	for x := 0; x < len(column_byte); x++ {
		if column_byte[x] == 44 { // ,
			if is_upper == true {
				column_name = append(column_name, strings.ToUpper(strings.TrimSpace(temp_rule.String())))
			} else {
				column_name = append(column_name, strings.TrimSpace(temp_rule.String()))
			}
			temp_rule.Reset()
		} else {
			temp_rule.WriteString(string(column_byte[x]))
		}
	}

	if is_upper == true {
		column_name = append(column_name, strings.ToUpper(strings.TrimSpace(temp_rule.String())))
	} else {
		column_name = append(column_name, strings.TrimSpace(temp_rule.String()))
	}

	return column_name
}

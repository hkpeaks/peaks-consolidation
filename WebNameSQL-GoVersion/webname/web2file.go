package webname

import (
	"strconv"
	"strings"
)

func Web2File(table_store map[string]WebNameTable, task Task, rule Rule) (string, int, string) {

	var parameter, setting, column, source, return_file_name, error_message string
	var result_table WebNameTable
	source = strings.ToUpper(task.current_table)

	for i := 0; i < len(rule.command2parameter_sequence[task.command]); i++ {
		parameter = rule.command2parameter_sequence[task.command][i]
		setting = rule.block[task.block][task.command][parameter]

		if strings.Contains(parameter, "$Source") {
			source = strings.ToUpper(setting)
		} else if strings.Contains(parameter, "$Column") {
			column = setting
		} else if strings.Contains(parameter, "$Return") {
			return_file_name = setting
		}
	}	

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "WEB2CSV" || strings.ToUpper(RemoveCommandIndex(task.command)) == "WEB2ONECOLUMN" {
		
		if column == "*" {		
			Web2CSV(table_store[source], return_file_name)
			return return_file_name, -999, error_message
		} else {
			result_table, error_message = SelectColumn(table_store, source, column)
			if len(error_message) == 0 {
				Web2CSV(result_table, return_file_name)
			}
			return return_file_name, result_table.total_column, error_message
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "WEB2HTML" {
		if column == "*" {
			Web2HTML(table_store[source], return_file_name)
			return return_file_name, -999, error_message
		} else {
			result_table, error_message = SelectColumn(table_store, source, column)
			Web2HTML(result_table, return_file_name)
			return return_file_name, result_table.total_column, error_message
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "WEB2JSON" {
		if column == "*" {
			Web2JSON(table_store[source], return_file_name)
			return return_file_name, -999, error_message
		} else {
			result_table, error_message = SelectColumn(table_store, source, column)
			Web2JSON(result_table, return_file_name)
			return return_file_name, result_table.total_column, error_message
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "WEB2XML" {
		if column == "*" {
			Web2XML(table_store[source], return_file_name)
			return return_file_name, -999, error_message
		} else {
			result_table, error_message = SelectColumn(table_store, source, column)
			Web2XML(result_table, return_file_name)
			return return_file_name, result_table.total_column, error_message
		}
	} else {
		return return_file_name, result_table.total_column, error_message
	}
}

func Web2CSV(web WebNameTable, file_name string) {

	var temp float64
	var csv_string strings.Builder		
	csv_string.WriteString(web.column_name[0])

	for x := 1; x < len(web.column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(web.column_name[x])
	}

	csv_string.WriteString("\r\n")

	for y := 0; y < len(web.fact_table[0]); y++ {

		for x := 0; x < len(web.column_name); x++ {
			if x > 0 {
				csv_string.WriteString(",")
			}

			if web.data_type[x] != "Number" {
				temp = web.fact_table[x][y]
				csv_string.WriteString(web.key2value[x][uint64(temp)])
			} else {
				temp = web.fact_table[x][y]
				csv_string.WriteString(strconv.FormatFloat(temp, 'f', -1, 64))
			}
		}
		csv_string.WriteString("\r\n")
	}

	String2Folder("Output\\", file_name, csv_string.String())
}

func Web2JSON(web WebNameTable, file_name string) {

	var json_string strings.Builder
	var total_row int = len(web.fact_table[0])
	var temp float64

	json_string.WriteString("{" + "\r\n")
	json_string.WriteString("    \"" + file_name + "\": [" + "\r\n")

	for y := 0; y < total_row; y++ {

		json_string.WriteString("    {" + "\r\n")
		json_string.WriteString("     \"id\": " + strconv.Itoa(y) + "," + "\r\n")

		for x := 0; x < web.total_column; x++ {
			json_string.WriteString("     \"" + web.column_name[x] + "\": ")
			temp = web.fact_table[x][y]

			if web.data_type[x] == "Number" {
				if x != web.total_column-1 {
					json_string.WriteString(strconv.FormatFloat(temp, 'f', -1, 64) + "," + "\r\n")
				} else {
					json_string.WriteString(strconv.FormatFloat(temp, 'f', -1, 64) + "\r\n")
				}
			} else {
				if x != web.total_column-1 {
					json_string.WriteString("\"" + web.key2value[x][uint64(temp)] + "\"," + "\r\n")
				} else {
					json_string.WriteString("\"" + web.key2value[x][uint64(temp)] + "\"" + "\r\n")
				}
			}
		}

		if y != total_row-1 {
			json_string.WriteString("    }," + "\r\n")
		} else {
			json_string.WriteString("    }" + "\r\n")
		}
	}

	json_string.WriteString("  ]" + "\r\n")
	json_string.WriteString("}" + "\r\n")

	String2Folder("Output\\", file_name, json_string.String())
}

func Web2HTML(web WebNameTable, file_name string) {

	var html_string strings.Builder
	var total_row int = len(web.fact_table[0])
	var temp float64

	html_string.WriteString("<table class=\"table\">" + "\r\n")
	html_string.WriteString("    <thead>" + "\r\n")
	html_string.WriteString("      <tr>" + "\r\n")

	for x := 0; x < web.total_column; x++ {
		html_string.WriteString("        <th>" + web.column_name[x] + "</th>" + "\r\n")
	}

	html_string.WriteString("      </tr>" + "\r\n")
	html_string.WriteString("    </thead>" + "\r\n")
	html_string.WriteString("    <tbody>" + "\r\n")

	for y := 0; y < total_row; y++ {
		html_string.WriteString("        <tr>" + "\r\n")

		for x := 0; x < web.total_column; x++ {
			temp = web.fact_table[x][y]

			if web.data_type[x] == "Number" {
				html_string.WriteString("          <td>" + strconv.FormatFloat(temp, 'f', -1, 64) + "</td>" + "\r\n")
			} else {
				html_string.WriteString("          <td>" + web.key2value[x][uint64(temp)] + "</td>" + "\r\n")
			}
		}
		html_string.WriteString("        </tr>" + "\r\n")
	}

	html_string.WriteString("    </tbody>" + "\r\n")
	html_string.WriteString("</table>" + "\r\n")

	String2Folder("Output\\", file_name, html_string.String())
}

func Web2XML(web WebNameTable, file_name string) {

	var xml_string strings.Builder
	var total_row int = len(web.fact_table[0])
	var temp_column_name string
	var xml_column_name []string
	var temp float64

	for x := 0; x < web.total_column; x++ {
		temp_column_name = strings.ReplaceAll(web.column_name[x], " ", "_x0020_")
		temp_column_name = strings.ReplaceAll(temp_column_name, "/", "_x002F_")
		xml_column_name = append(xml_column_name, temp_column_name)
	}
	xml_string.WriteString("<?xml version=\"1.0\" standalone=\"yes\"?>" + "\r\n")
	xml_string.WriteString("<NewDataSet>" + "\r\n")

	for y := 0; y < total_row; y++ {
		xml_string.WriteString("   <" + file_name + ">" + "\r\n")

		for x := 0; x < web.total_column; x++ {
			temp = web.fact_table[x][y]

			if web.data_type[x] == "Number" {
				xml_string.WriteString("     <" + xml_column_name[x] + ">" + strconv.FormatFloat(temp, 'f', -1, 64) + "</" + xml_column_name[x] + ">" + "\r\n")
			} else {
				xml_string.WriteString("     <" + xml_column_name[x] + ">" + web.key2value[x][uint64(temp)] + "</" + xml_column_name[x] + ">" + "\r\n")
			}
		}
		xml_string.WriteString("   </" + file_name + ">" + "\r\n")
	}
	xml_string.WriteString("</NewDataSet>" + "\r\n")

	String2Folder("Output\\", file_name, xml_string.String())
}

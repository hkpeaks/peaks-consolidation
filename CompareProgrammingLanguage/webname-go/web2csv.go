package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

func web2csv(web WebNameTable) {
	var csv_string strings.Builder

	csv_string.WriteString(web.column_name[0])

	for x := 1; x < len(web.column_name); x++ {
		csv_string.WriteString(",")
		csv_string.WriteString(web.column_name[x])
	}

	csv_string.WriteString("\n")

	for y := 1; y < len(web.fact_table[0]); y++ {
		for x := 0; x < len(web.column_name); x++ {
			if x > 0 {
				csv_string.WriteString(",")
			}

			if web.data_type[x] != "Number" {
				csv_string.WriteString(web.key2value[x][uint64(web.fact_table[x][y])])
			} else {
				csv_string.WriteString(strconv.FormatFloat(web.fact_table[x][y], 'f', -1, 64))
			}
		}
		csv_string.WriteString("\n")
	}

	f, err := os.Create("data.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	_, err2 := f.WriteString(csv_string.String())

	if err2 != nil {
		log.Fatal(err2)
	}
}

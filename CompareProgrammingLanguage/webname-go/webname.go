package main

import (
	"fmt"
	//"strings"
	"time"
)

type WebNameTable struct {
	total_column         int
	extra_line_br_char   int
	column_name          []string
	upper_column_name2id map[string]int
	data_type            []string
	fact_table           map[int][]float64
	key2value            map[int]map[uint64]string
	value2key            map[int]map[string]uint64
}

func webname(filename string) {
	start := time.Now()

	var bytestream = file2bytestream(filename)
	var web = csvbyte2web(bytestream)	

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("CSV File to Webname Table: ", elapsed)

	fmt.Println("total column= ", web.total_column, "total row= ", len(web.fact_table[0]))

	start2 := time.Now()
      /*
	for x := 0; x < len(web.column_name); x++ {
		fmt.Println("column: ", x, web.column_name[x], web.data_type[x], web.upper_column_name2id[strings.ToUpper(web.column_name[x])])
	}

	for k, v := range web.key2value[3] {
		fmt.Println("key2value ", k, v)
	}

	for k, v := range web.value2key[3] {
		fmt.Println("value2key ", k, v)
	}

	
		for current_column := 0; current_column < web.total_column; current_column++ {
			for i := 0; i < len(web.fact_table[current_column]); i++ {
				fmt.Println("current column", current_column, " value ", web.fact_table[current_column][i])
			}
		}
	*/
	
	web2csv(web);

	t2 := time.Now()
	elapsed2 := t2.Sub(start2)
	fmt.Println("Webname Table to CSV File: ", elapsed2)
}

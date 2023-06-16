package peaks

import (	
	"strings"
)

func CurrentCommand(batch int, result_table_partition map[int]Cache, current_partition int, total_partition int, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

	var result_table Cache

	if strings.ToUpper(RemoveCommandIndex(task.command)) == "JOINKEYVALUE" {

		if total_partition == 1 {
			result_table = *JoinKeyValue(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECT" || strings.ToUpper(RemoveCommandIndex(task.command)) == "SELECTUNMATCH" {

		if total_partition == 1 {
			result_table = *Select(result_table_partition, current_partition, total_partition, task, ir, rule, upper_column_name2id, data_type)
		}
	} else if strings.ToUpper(RemoveCommandIndex(task.command)) == "BUILDKEYVALUE" {

		if batch == 0 && total_partition == 1 {
			result_table = *BuildKeyValue(result_table_partition, current_partition, total_partition, ir, rule, upper_column_name2id, data_type)
		} else {

			result_keyvalue_table := make(map[string][]byte)
			var source_table Cache

			for p := 0; p < total_partition; p++ {

				source_table = *CurrentPartition(result_table_partition, p)
				for key, value := range source_table.keyvalue_table {

					if _, found := result_keyvalue_table[key]; found {
						result_keyvalue_table[key] = append(result_keyvalue_table[key], value...)
					} else {
						result_keyvalue_table[key] = value
					}
				}
			}

			result_table.total_column = result_table_partition[0].total_column
			result_table.extra_line_br_char = result_table_partition[0].extra_line_br_char
			result_table.column_name = result_table_partition[0].column_name
			result_table.upper_column_name2id = result_table_partition[0].upper_column_name2id
			result_table.data_type = result_table_partition[0].data_type
			result_table.bytestream = nil
			result_table.partition_row = int32(len(result_keyvalue_table))
			result_table.cell_address = result_table_partition[0].cell_address
			result_table.keyvalue_table = result_keyvalue_table
			result_table.value_column_name = source_table.value_column_name
		}

	} else if rule.filter2groupby == true && total_partition == 1 {
		result_table = *Filter2GroupBy(result_table_partition, current_partition, total_partition, task, ir, rule, upper_column_name2id, data_type)
	} else {
		result_table = *DistinctGroupBy(result_table_partition, current_partition, total_partition, task, ir, rule, upper_column_name2id, data_type)
	}

	return &result_table
}

func JoinKeyValue(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {
	
        // This databending function is not open source
	var result_table Cache
	return &result_table
}

func BuildKeyValue(result_table_partition map[int]Cache, current_partition int, total_partition int, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

        // This databending function is not open source
	var result_table Cache
	return &result_table
}

func Select(result_table_partition map[int]Cache, current_partition int, total_partition int, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {	

        // This databending function is not open source
	var result_table Cache
	return &result_table
}

func Filter2GroupBy(result_table_partition map[int]Cache, current_partition int, total_partition int, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {

        // This databending function is not open source
	var result_table Cache
	return &result_table
}

func DistinctGroupBy(result_table_partition map[int]Cache, current_partition int, total_partition int, task Task, ir InternalRule, rule Rule, upper_column_name2id map[string]int, data_type []string) *Cache {   
	
        // This databending function is not open source
	var result_table Cache
	return &result_table
}

func AddColumnmNameWithSeq(table_partition_store *map[string]map[int]Cache, rule Rule, ir InternalRule) ([]int, []int, []string, map[string]int, []string) {

	source_table := CurrentTableFirstPartition(*table_partition_store, ir)
	var column_name = String2Slice(ir.column, false, rule)
	var alt_column_name = String2Slice(ir.column, false, rule)

	var data_type []string
	upper_column_name2id := make(map[string]int)
	var column_id_seq []int

	if len(ir.column) > 0 {

		for x := 0; x < len(ir.group_by_function); x++ {
			if strings.ToUpper(ir.calc_column_name[x]) == "NULL" {

				column_name = append(column_name, "Count()")

				alt_column_name = append(alt_column_name, "Count()")
			} else {
				column_name = append(column_name, ir.calc_column_name[x])
				alt_column_name = append(alt_column_name, ir.group_by_function[x]+"("+ir.calc_column_name[x]+")")
			}
		}

		for x := 0; x < len(column_name); x++ {

			if strings.ToUpper(column_name[x]) == "COUNT()" {
				column_id_seq = append(column_id_seq, -1)
				data_type = append(data_type, "Text")
				upper_column_name2id["COUNT()"] = -1

			} else {
				column_id_seq = append(column_id_seq, source_table.upper_column_name2id[strings.ToUpper(column_name[x])])
				data_type = append(data_type, "Text")
				upper_column_name2id[strings.ToUpper(alt_column_name[x])] = x
			}
		}
	}

	var filter_column_id_seq []int

	if len(ir.filter_column) > 0 {

		var filter_column_name = String2Slice(ir.filter_column, false, rule)

		for x := 0; x < len(filter_column_name); x++ {
			filter_column_id_seq = append(filter_column_id_seq, source_table.upper_column_name2id[strings.ToUpper(filter_column_name[x])])
		}
	}

	return filter_column_id_seq, column_id_seq, alt_column_name, upper_column_name2id, data_type
}

func AddColumnmNameWithSeq2(table_partition_store *map[string]map[int]Cache, rule Rule, ir InternalRule) ([]int, []string, map[string]int, []string) {

	source_table := CurrentTableFirstPartition(*table_partition_store, ir)

	var data_type []string
	upper_column_name2id := make(map[string]int)
	var column_id_seq []int	

	for x := 0; x < len(ir.column_name); x++ {

		column_id_seq = append(column_id_seq, source_table.upper_column_name2id[strings.ToUpper(ir.column_name[x])])
		data_type = append(data_type, "Text")
		upper_column_name2id[strings.ToUpper(ir.column_name[x])] = x
	}

	return column_id_seq, ir.column_name, upper_column_name2id, data_type
}

func CurrentPartition(source_table_partition map[int]Cache, current_partition int) *Cache {
	result_table := source_table_partition[current_partition]
	return &result_table
}

func CurrentBytestream(source_table Cache) *[]byte {
	return &source_table.bytestream
}

func CurrentCellAddress(source_table Cache) *[]uint32 {
	return &source_table.cell_address
}

func CurrentTableFirstPartition(table_partition_store map[string]map[int]Cache, ir InternalRule) *Cache {
	var result_table = table_partition_store[strings.ToUpper(ir.source_table_name)][0]
	return &result_table
}



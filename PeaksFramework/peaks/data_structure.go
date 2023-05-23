package peaks

type Task struct {
	block         string
	command       string
	current_table string
}

type Rule struct {
	thread                     int
	streamMB                   int
	read_csv_delimiter         byte
	write_csv_delimiter        byte
	force_integer              bool
	output_debug               bool
	byte_per_stream            int64
	bytestream                 []byte
	original_rule              []string
	command2rule               map[string]string
	block_sequence             []string
	command_sequence           []string
	parameter_sequence         []string
	block2command_sequence     map[string][]string
	command2parameter_sequence map[string][]string
	block                      map[string]map[string]map[string]string
}

type InternalRule struct {
	source_table_name                     string
	original_source_table_name            string
	return_table_name                     string
	column                                string
	filter_column                         string
	column_name                           []string
	column_id                             map[int]int
	column_id_seq                         []int
	filter_column_id_seq                  []int
	group_by_function                     []string
	join_table_function                   string
	calc_column_name                      []string
	x_column                              string
	y_column                              string
	keyvalue_table_name                   string
	table                                 Cache
	upper_column_name2operator            map[string][]string
	upper_column_name2data_type           map[string][]string
	upper_column_name2compare_value       map[string][]string
	upper_column_name2compare_alt_value   map[string][]string
	upper_column_name2compare_float64     map[string][]float64
	upper_column_name2compare_alt_float64 map[string][]float64
	full_streaming_command                map[string]int
	validate_all_column_name              []string
}

type Cache struct {
	total_column         int
	total_row            int32
	partition_row        int32
	extra_line_br_char   int
	column_name          []string
	upper_column_name2id map[string]int
	data_type            []string
	bytestream           []byte
	cell_address         []uint32
	keyvalue_table       map[string][]byte
	value_column_name    []string
}

type CachePartition struct {
	error_message   string
	total_row       int32
	table_name      string
	table_partition map[int]Cache
	ir              InternalRule
}

type DataStructure struct {
	total_column         int
	extra_line_br_char   int
	column_name          []string
	upper_column_name2id map[string]int
	data_type            []string
	partition_address    map[int]int64
	estimated_cell       int64
}

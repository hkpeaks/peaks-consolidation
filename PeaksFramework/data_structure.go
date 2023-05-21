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

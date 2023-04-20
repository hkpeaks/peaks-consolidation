using System.Text;

namespace WebName
{
    public class WebNameTable
    {
        public int total_column { get; set; }
        public int extra_line_br_char { get; set; }
        public List<string> column_name = new List<string>();
        public Dictionary<string, int> upper_column_name2id = new Dictionary<string, int>();
        public List<string> data_type = new List<string>();
        public Dictionary<int, List<double>> fact_table = new Dictionary<int, List<double>>();
        public Dictionary<int, Dictionary<long, string>> key2value = new Dictionary<int, Dictionary<long, string>>();
        public Dictionary<int, Dictionary<string, long>> value2key = new Dictionary<int, Dictionary<string, long>>();
    }

    public class CreateWebNameTable
    {
        public WebNameTable csvbyte2web(byte[] bytestream) {       
            (int total_column, int extra_line_br_char, List<long> address) = cell_address(bytestream);
            (List<string> column_name, Dictionary<string, int> upper_column_name2id, List<string> data_type) = data_schema(total_column, extra_line_br_char, address, bytestream);

            Dictionary<int, List<double>> fact_table = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<long, string>> key2value = new Dictionary<int, Dictionary<long, string>>();
            Dictionary<int, Dictionary<string, long>> value2key = new Dictionary<int, Dictionary<string, long>>();

            for (int current_column = 0; current_column < total_column; current_column++)
            {
                if (data_type[current_column] == "Text" || data_type[current_column] == "Date")
                {
                    (List<double> ft, Dictionary<long, string> k2v, Dictionary<string, long> v2k) = cell_text(total_column, extra_line_br_char, current_column, address, bytestream);
                    fact_table.Add(current_column, ft);
                    key2value.Add(current_column, k2v);
                    value2key.Add(current_column, v2k);
                }
                else
                {
                    (List<double> ft, Dictionary<long, string> k2v, Dictionary<string, long> v2k) = cell_number(total_column, extra_line_br_char, current_column, address, bytestream);
                    fact_table.Add(current_column, ft);
                    key2value.Add(current_column, k2v);
                    value2key.Add(current_column, v2k);
                }
            }

            WebNameTable result_table = new WebNameTable();
            result_table.total_column = total_column;
            result_table.extra_line_br_char = extra_line_br_char;
            result_table.column_name = column_name;
            result_table.upper_column_name2id = upper_column_name2id;
            result_table.data_type = data_type;
            result_table.fact_table = fact_table;
            result_table.key2value = key2value;
            result_table.value2key = value2key;

            return result_table;
        }


        public (int, int, List<long>) cell_address(byte[] bytestream) {
            int total_column = 1;
            int extra_line_br_char = 0;
            int _double_quote_count = 0;
            
            for (long x = 0; x < bytestream.Length; x++)
            {
                if (bytestream[x] == 44 && _double_quote_count % 2 == 0) {                
                    total_column += 1;                   
                }
                else if (bytestream[x] == 13 || bytestream[x] == 10) {
                    if (bytestream[x + 1] == 13 || bytestream[x + 1] == 10) {                    
                        extra_line_br_char += 1;
                    }
                    break;
                }
                if (bytestream[x] == 34) {                
                    _double_quote_count += 1;
                }
            }

            List<long> address = new List<long>();

            address.Add(0);

            for (long x = 0; x < bytestream.Length; x++) {
                if (bytestream[x] == 44 && _double_quote_count % 2 == 0) {               
                    address.Add(x + 1);
                }
                else if (bytestream[x] == 13 || bytestream[x] == 10) {                
                    address.Add(x + 1);
                }

                if (bytestream[x] == 34) {                
                    _double_quote_count += 1;                  
                }
            }
            return (total_column, extra_line_br_char, address);
        }

        public (List<string>, Dictionary<string, int>, List<string>) data_schema(int total_column, int extra_line_br_char, List<long> cell_address, byte[] _bytestream)
        {
            string current_text;
            List<string> data_type = new List<string>();
            StringBuilder temp_cell_address = new StringBuilder();
            int valiate_row = cell_address.Count - 1;

            if (valiate_row > 100) {            
                valiate_row = 100;
            }

            for (int _current_column = 0; _current_column < total_column; _current_column++)
            {
                data_type.Add("Text");
                int n = _current_column + total_column + extra_line_br_char;

                while (n < valiate_row) {
                    for (long x = cell_address[n]; x < (cell_address[n + 1] - 1); x++) {                    
                        temp_cell_address.Append((char)_bytestream[x]);
                    }

                    current_text = temp_cell_address.ToString();                   
                    var is_num = double.TryParse(current_text, out double current_number);

                    if (is_num == false) {                    
                        data_type[_current_column] = "Text";
                        n = cell_address.Count - 1;
                    }
                    else {                    
                        data_type[_current_column] = "Number";
                    }
                    temp_cell_address.Clear();
                    n += total_column + extra_line_br_char;
                }
            }

            List<string> column_name = new List<string>();
            Dictionary<string, int> upper_column_name2id = new Dictionary<string, int>();

            List<string> text_column = new List<string> { "A/C", "NUMBER", "INVOICE", "ACCOUNT", "DOCUMENT" };

            for (int _current_column = 0; _current_column < total_column; _current_column++) {
                for (long x = cell_address[_current_column]; x < cell_address[_current_column + 1] - 1; x++) {                
                    temp_cell_address.Append((char)_bytestream[x]);
                }

                current_text = temp_cell_address.ToString();

                if (current_text.Length == 0) {                
                    current_text = "Column" + _current_column;
                }

                if (current_text.ToUpper().Contains("DATE")) {                
                    data_type[_current_column] = "Date";
                }

                for (int i = 0; i < text_column.Count; i++) {                
                    if (current_text.ToUpper().Contains(text_column[i])) {                    
                        data_type[_current_column] = "Text";
                    }
                }
                column_name.Add(current_text);
                upper_column_name2id.Add(current_text.ToUpper(), _current_column);
                temp_cell_address.Clear();
            }

            return (column_name, upper_column_name2id, data_type);
        }

        public (List<double>, Dictionary<long, string>, Dictionary<string, long>) cell_text(int total_column, int extra_line_br_char, int current_column, List<long> cell_address, byte[] bytestream) {        
            string current_text;
            List<double> fact_table = new List<double>();
            StringBuilder temp_cell_address = new StringBuilder();
            int n = current_column;
            Dictionary<long, string> key2value = new Dictionary<long, string>();
            Dictionary<string, long> value2key = new Dictionary<string, long>();
            int key = 0;

            while (n < cell_address.Count - 1) {            
                for (long x = cell_address[n]; x < cell_address[n + 1] - 1; x++) {                
                    temp_cell_address.Append((char)bytestream[x]);
                }

                current_text = temp_cell_address.ToString();

                if (!value2key.ContainsKey(current_text)) {                
                    value2key.Add(current_text, key);
                    key2value.Add(key, current_text);
                    fact_table.Add(key);
                    key += 1;
                }
                else {                
                    fact_table.Add(value2key[current_text]);
                }
                temp_cell_address.Clear();
                n += total_column + extra_line_br_char;
            }
            fact_table[0] = current_column;
            return (fact_table, key2value, value2key);
        }

        public (List<double>, Dictionary<long, string>, Dictionary<string, long>) cell_number(int total_column, int extra_line_br_char, int current_column, List<long> cell_address, byte[] bytestream)
        {
            string current_text;
            List<double> fact_table = new List<double>();
            StringBuilder temp_cell_address = new StringBuilder();
            int n = current_column;
            Dictionary<long, string> key2value = new Dictionary<long, string>();
            Dictionary<string, long> value2key = new Dictionary<string, long>();

            for (long x = cell_address[n]; x < cell_address[n + 1] - 1; x++) {            
                temp_cell_address.Append((char)bytestream[x]);
            }

            current_text = temp_cell_address.ToString();
            value2key.Add(current_text, 0);
            key2value.Add(0, current_text);
            fact_table.Add(0);
            temp_cell_address.Clear();
            n += total_column + extra_line_br_char;

            while (n < cell_address.Count - 1) {
                for (long x = cell_address[n]; x < cell_address[n + 1] - 1; x++) {                
                    temp_cell_address.Append((char)bytestream[x]);
                }
                current_text = temp_cell_address.ToString();
                var is_num = double.TryParse(current_text, out double current_number);
                fact_table.Add(current_number);
                temp_cell_address.Clear();
                n += total_column + extra_line_br_char;
            }
            fact_table[0] = current_column;
            return (fact_table, key2value, value2key);
        }
    }
}


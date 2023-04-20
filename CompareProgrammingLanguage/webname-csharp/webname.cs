namespace WebName
{
    public class ProcessFlow {
    
        public void webname(string filename) {           

            DateTime start = DateTime.Now;   
            Conversion currnet_conversion = new Conversion();
            byte[] bytestream = currnet_conversion.file2bytestream(filename);
            CreateWebNameTable current_creation = new CreateWebNameTable();
            WebNameTable web = current_creation.csvbyte2web(bytestream);

            DateTime end = DateTime.Now;   
            Console.WriteLine(string.Format("CSV File to Webname Table = {0:0.000}", (end - start).TotalSeconds) + "s" + Environment.NewLine);

            Console.WriteLine("total_column {0}", web.total_column);

/*
            for (int x = 0; x < web.column_name.Count; x++) {
                Console.WriteLine("column: {0} {1} {2} {3}", x, web.column_name[x], web.data_type[x], web.upper_column_name2id[web.column_name[x].ToUpper()]);                           
            }

            foreach (var item in web.key2value[3]) {
                Console.WriteLine("key2value: {0} {1}", item.Key, item.Value);
            }

            foreach (var item in web.value2key[3]) {
                Console.WriteLine("value2key: {0} {1}", item.Key, item.Value);
            }

            for (int current_column = 0; current_column < web.total_column; current_column++) {
                for (int i = 0; i < web.fact_table[current_column].Count; i++) {
                   Console.WriteLine("current column {0} value {1} ", current_column,  web.fact_table[current_column][i]);
                } 
            }       
*/


            start = DateTime.Now;    
            WriteFile current_file = new WriteFile();
            current_file.web2csv(web);
            end = DateTime.Now;           
            Console.WriteLine(string.Format("Webname Table to CSV file = {0:0.000}", (end - start).TotalSeconds) + "s" + Environment.NewLine);
        }
    }    
}
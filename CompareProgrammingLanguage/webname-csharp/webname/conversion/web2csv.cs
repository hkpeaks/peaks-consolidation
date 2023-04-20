using System.Text;

namespace WebName
{
    public class WriteFile {
        public void web2csv(WebNameTable web) {
            StringBuilder csv_string = new StringBuilder();
            csv_string.Append(web.column_name[0]);

            for (int x = 1; x < web.column_name.Count; x++) {
                 csv_string.Append(",");
                 csv_string.Append(web.column_name[x]);
            }

            csv_string.Append("\n");

            for (int y = 1; y < web.fact_table[0].Count; y++) {
                for (int x = 0; x < web.column_name.Count; x++) {
                    if(x > 0) {
                       csv_string.Append(",");
                    }

                    if(web.data_type[x] != "Number") {                       
                        csv_string.Append(web.key2value[x][Convert.ToInt64(web.fact_table[x][y])]);
                    }
                    else {
                        csv_string.Append(web.fact_table[x][y].ToString());
                    }                   
                }
                csv_string.Append("\n");
            }

            using (StreamWriter toDisk = new StreamWriter("data.csv"))
            {
                toDisk.Write(csv_string);
                toDisk.Close();
            }
        }
    }


}
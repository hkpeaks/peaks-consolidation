use std::io::Read;
use std::io::Write;
use std::fs::File;
use std::fs;

pub mod csv2web;

pub fn web2csv(web: csv2web::WebName) {
    let mut csv_string = String::new();
    csv_string.push_str(&web.column_name[0]);

    for x in 1..web.column_name.len() {         
          csv_string.push_str(",");
          csv_string.push_str(&web.column_name[x]);
    }

    csv_string.push_str("\n");

    for y in 1..web.fact_table[&0].len() {                
        for x in 0..web.column_name.len() {              
            if x > 0 {
               csv_string.push_str(",");               
            }

            if web.data_type[x] != "Number" {            
                csv_string.push_str(&web.key2value[&x][&(web.fact_table[&x][y] as usize)]);     
            }       
            else {                               
               csv_string.push_str(&web.fact_table[&x][y].to_string())
            }
        }
        csv_string.push_str("\n");
    }

    let mut file = std::fs::File::create("data.csv").expect("create failed");
    file.write_all(csv_string.as_bytes()).expect("write failed");
}








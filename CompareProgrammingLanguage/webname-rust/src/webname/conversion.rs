use std::io::Read;
use std::io::Write;
use std::fs::File;
use std::fs;

pub mod csv2web;

pub fn file2bytestream(filename: String) -> Vec<u8> {
    let mut f = File::open(&filename).expect("no file found");
    let metadata = fs::metadata(&filename).expect("unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];    
    f.read(&mut buffer).expect("buffer overflow");    
    return buffer;
}

pub fn web2csvstring(web: csv2web::WebName) -> String {
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
    return csv_string;
}

pub fn csvstring2file(csv_string: String) {
    let mut file = std::fs::File::create("data.csv").expect("create failed");
    file.write_all(csv_string.as_bytes()).expect("write failed");
}








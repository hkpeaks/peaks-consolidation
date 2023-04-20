use std::env;
use std::time::{Instant};

pub mod conversion;

pub fn webname() 
{  
    let start = Instant::now();  
    let args: Vec<String> = env::args().collect();           
   // let web = conversion::csvfile2web(args[1].to_string());              

    let mut bytestream =  conversion::file2bytestream(args[1].to_string());
    let web = conversion::csv2web::csvbyte2web(&mut bytestream);     

    let end = Instant::now();
    println!("CSV File to Webname Table {:?}", end.duration_since(start));        
    
    println!("total_column {}", web.total_column);    


     /*
    for x in 0..web.column_name.len() {
        println!("column: {} {} {} {}", x, web.column_name[x], web.data_type[x], web.upper_column_name2id[&web.column_name[x].to_uppercase()]);    
    }
  
    for (key, value) in &web.key2value[&3] {
        println!("key2value: {} {}", key, value);    
    }

    for (key, value) in &web.value2key[&3] {
        println!("value2key: {} {}", key, value);    
    }    
 
    for current_column in 0..web.total_column {
        for i in 0..web.fact_table[&current_column].len() {
            println!("current column {} value {} ", current_column,  web.fact_table[&current_column][i]);
        }
    }   
*/ 

    let start = Instant::now();  
    let csvstring = conversion::web2csvstring(web);
    conversion::csvstring2file(csvstring);
    let end = Instant::now();
    println!("Webname Table to CSV File {:?}", end.duration_since(start));        
    
}


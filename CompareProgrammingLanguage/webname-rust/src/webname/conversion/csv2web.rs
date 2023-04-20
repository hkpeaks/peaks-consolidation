use std::collections::HashMap;
use std::time::{Instant};

pub struct WebName {
    pub total_column: usize,
    pub extra_line_br_char: usize,
    pub column_name: Vec<String>,
    pub upper_column_name2id: HashMap<String, u64>, 
    pub data_type: Vec<String>,
    pub fact_table: HashMap<usize, Vec<f64>>,
    pub key2value: HashMap<usize, HashMap<usize, String>>,
    pub value2key: HashMap<usize, HashMap<String, usize>>,
}

pub fn csvbyte2web(bytestream: &mut Vec<u8>) -> WebName {

    let start = Instant::now();  
    let (total_column,  extra_line_br_char, mut cell_address) = cell_address(bytestream);       

    let end = Instant::now();
    println!("Cell Address {:?}", end.duration_since(start));        


    let (column_name, upper_column_name2id, data_type) =  data_schema(total_column, extra_line_br_char, &mut cell_address, bytestream);          
    
    let mut fact_table: HashMap<usize,  Vec<f64>> = HashMap::new();
    let mut key2value: HashMap<usize, HashMap<usize,  String>> = HashMap::new();
    let mut value2key: HashMap<usize, HashMap<String, usize>> = HashMap::new();

    for current_column in 0..total_column { 
        if data_type[current_column] == "Text" || data_type[current_column] == "Date" {
            let (ft, k2v, v2k) = cell_text(total_column, extra_line_br_char, current_column, &mut cell_address, bytestream);
            fact_table.insert(current_column, ft);   
            key2value.insert(current_column, k2v);   
            value2key.insert(current_column, v2k);   
        }        
        else {
            let (ft, k2v, v2k) = cell_number(total_column, extra_line_br_char, current_column, &mut cell_address, bytestream);
            fact_table.insert(current_column, ft);   
            key2value.insert(current_column, k2v);   
            value2key.insert(current_column, v2k);  
        }        
    }     

    let result_table:WebName = WebName {total_column: total_column, extra_line_br_char: extra_line_br_char, column_name: column_name, upper_column_name2id: upper_column_name2id, data_type: data_type, fact_table: fact_table, key2value: key2value, value2key: value2key};
    return result_table;
}

pub fn cell_address(bytestream: &mut Vec<u8>) ->  (usize, usize, Vec<u64>) {      
    let mut total_column: usize = 1;    
    let mut extra_line_br_char: usize = 0;
    let mut _double_quote_count: usize = 0;        

    for x in 0usize..bytestream.len() {        
        if bytestream[x] == 44 &&  _double_quote_count % 2 == 0 {                               
            total_column += 1;
        }
        else if bytestream[x] == 13 || bytestream[x] == 10 {                        

            if bytestream[x + 1] == 13 || bytestream[x + 1] == 10 {        
                extra_line_br_char += 1;
            }
            break;       
        }
        if bytestream[x] == 34 {
            _double_quote_count += 1;
        }
    } 
   
    let mut address: Vec<u64> = Vec::new();        
    address.push(0 as u64);  

    for x in 0usize..bytestream.len() {          

        if bytestream[x] == 44 &&  _double_quote_count % 2 == 0 {                               
            address.push((x + 1) as u64);                                            
        } 
        else if bytestream[x] == 13 || bytestream[x] == 10 {                   
            address.push((x + 1) as u64);                                            
        }      

        if bytestream[x] == 34 {
            _double_quote_count += 1;
        }
    } 

    return (total_column, extra_line_br_char, address);
}

pub fn data_schema(total_column: usize, extra_line_br_char: usize, cell_address: &mut Vec<u64>, bytestream: &mut Vec<u8>) ->  (Vec<String>, HashMap<String, u64>, Vec<String>) {        
    let mut data_type: Vec<String> = Vec::new();    
    let mut temp_cell_address: Vec<u8> = Vec::new();    
    let mut valiate_row = cell_address.len() - 1;

    if valiate_row > 100 {
        valiate_row = 100;
    }   

    for _current_column in 0..total_column {
        data_type.push("Text".to_string());
        let mut n = _current_column + total_column + extra_line_br_char;           
     
        while n < valiate_row {           
            for x in cell_address[n]..(cell_address[n + 1] - 1) {
                temp_cell_address.push(bytestream[x as usize]);      
            }

            let current_text = String::from_utf8(temp_cell_address.clone()).expect("Found invalid UTF-8");           
            let is_num = current_text.parse::<f64>().is_ok();               
            
            if is_num == false {                                 
                data_type[_current_column] = "Text".to_string();                    
                n = cell_address.len() - 1;
            }
            else {                   
                data_type[_current_column] = "Number".to_string();                    
            }        
            temp_cell_address.clear();                               
            n += total_column + extra_line_br_char;
        }        
    }   

    let mut column_name: Vec<String> = Vec::new();    
    let mut upper_column_name2id: HashMap<String, u64> = HashMap::new();   

    let text_column = vec!["A/C", "NUMBER", "INVOICE", "ACCOUNT", "DOCUMENT"];     
   
    for _current_column in 0..total_column {
        for x in cell_address[_current_column]..(cell_address[_current_column + 1] - 1) {
            temp_cell_address.push(bytestream[x as usize]);      
        }

        let mut current_text = String::from_utf8(temp_cell_address.clone()).expect("Found invalid UTF-8");            

        if current_text.len() == 0 {          
            current_text = ["Column".to_string(),_current_column.to_string()].join("");
        }

        if current_text.to_uppercase().contains("DATE") {                         
            data_type[_current_column] = "Date".to_string();
        }

        for i in 0..text_column.len() {
            if current_text.to_uppercase().contains(text_column[i]) {                                
                data_type[_current_column] = "Text".to_string();
            }
        }       
     
        column_name.push(current_text.clone());
        upper_column_name2id.insert(current_text.to_uppercase(), _current_column as u64);                    
        temp_cell_address.clear();        
      
    }    

    return (column_name, upper_column_name2id, data_type);
}

pub fn cell_text(total_column: usize, extra_line_br_char: usize, current_column: usize , cell_address: &mut Vec<u64>, bytestream: &mut Vec<u8>) ->  (Vec<f64>, HashMap<usize,  String>, HashMap<String, usize>) {            
    let mut fact_table: Vec<f64> = Vec::new();
    let mut temp_cell_address: Vec<u8> = Vec::new();           
    let mut n = current_column;
    let mut key2value: HashMap<usize,  String> = HashMap::new();
    let mut value2key: HashMap<String, usize> = HashMap::new();
    let mut key = 0;

    while n < cell_address.len() - 1 {
       
        for x in cell_address[n]..(cell_address[n + 1] - 1) {
            temp_cell_address.push(bytestream[x as usize]);      
        }

        let current_text = String::from_utf8(temp_cell_address.clone()).expect("Found invalid UTF-8");           

        if !value2key.contains_key(&current_text) {
            value2key.insert(current_text.clone(), key);
            key2value.insert(key, current_text.clone());               
            fact_table.push(key as f64);            
            key += 1;
        } 
        else {
            fact_table.push(value2key[&current_text] as f64);            
        }

        temp_cell_address.clear();                       
        n += total_column + extra_line_br_char;
    } 
    fact_table[0] = current_column as f64;
    return (fact_table, key2value, value2key);
}

pub fn cell_number(total_column: usize, extra_line_br_char: usize, current_column: usize, cell_address: &mut Vec<u64>, bytestream: &mut Vec<u8>) ->  (Vec<f64>, HashMap<usize,  String>, HashMap<String, usize>) {      
    let mut fact_table: Vec<f64> = Vec::new();
    let mut temp_cell_address: Vec<u8> = Vec::new();           
    let mut n = current_column;
   
    let mut key2value: HashMap<usize,  String> = HashMap::new();
    let mut value2key: HashMap<String, usize> = HashMap::new();
 
    for x in  cell_address[n]..(cell_address[n + 1] - 1) {
        temp_cell_address.push(bytestream[x as usize]);      
    }

    let current_text = String::from_utf8(temp_cell_address.clone()).expect("Found invalid UTF-8");           
    value2key.insert(current_text.clone(), 0);
    key2value.insert(0, current_text);  
    fact_table.push(0 as f64);               
    temp_cell_address.clear();        
    
    n += total_column + extra_line_br_char;

    while n <  cell_address.len() - 1 {
        for x in  cell_address[n]..( cell_address[n + 1] - 1) {
            temp_cell_address.push(bytestream[x as usize]);      
        }
        let current_text = String::from_utf8(temp_cell_address.clone()).expect("Found invalid UTF-8");           
        let number: f64 = current_text.parse().unwrap();                     
        fact_table.push(number);            
        temp_cell_address.clear();                       
        n += total_column + extra_line_br_char;
    } 
    fact_table[0] = current_column as f64;
    return (fact_table, key2value, value2key);
}


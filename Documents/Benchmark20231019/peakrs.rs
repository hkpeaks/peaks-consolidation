extern crate chrono;
extern crate rayon;
extern crate peakrs; 
use peakrs::*;
use chrono::Local;

fn main() {
    
    let start_time = Local::now();     

    let mut df = Dataframe::default();
    df.log_file_name = format!("Outbox/Peakrs-Log-{}.csv", start_time.format("%y%m%d_%H%M%S"));
    df.partition_size_mb = 5;
    df.thread = 100;

    create_log(&df);

    let query1 = query(vec![
       "filter", "Style(=F)",
        "build_key_value", "Product, Style => Table(key_value)"]);

    let master_df = run_batch(df.clone(), "Inbox/Master.csv", query1);

    let query2 = query(vec![
        "filter", "Shop(S77..S78)",
        "join_key_value", "Product, Style => Inner(key_value)",
        "add_column", "Quantity, Unit_Price => Multiply(Amount)",
        "filter", "Amount:Float(>100000)",
        "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)"
      ]);

    let source_file = get_cli_file_path("10-MillionRows.csv");
    let result_file = vec![
        format!("Outbox/Peakrs-Detail-Result-{}", std::path::Path::new(&source_file).file_name().unwrap().to_str().unwrap()),
        format!("Outbox/Peakrs-Summary-Result-{}", std::path::Path::new(&source_file).file_name().unwrap().to_str().unwrap())];

    run_stream(&df, &master_df, &source_file, query2, result_file.clone());

    view_sample(&result_file[0]);
    view_sample(&result_file[1]);

    let end_time = Local::now();
    let duration = end_time - start_time;
    let mut duration_in_second = duration.num_milliseconds() as f64;
    duration_in_second = duration_in_second.round() / 1000.0;
    println!("Peakrs Duration (In Second): {:.3} ", duration_in_second);     
}

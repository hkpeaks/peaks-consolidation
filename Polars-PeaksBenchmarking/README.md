## Polars and Peaks Benchmarking

While Polars is one of the fastest dataframes that can be easily installed and run on desktop PCs, Peaks is not intended to be another Polars or Pandas.

90 performance tests for both Polars and Peaks were completed for ETL functions such as "Distinct", "GroupBy" and JoinTable" using 8-Cores/32GB RAM, with data rows ranging from 10,000 to 1 billion. The time measures cover the starting read csv files to the completion of write csv file. PeaksBenchmark.xlsx uploaded in this repository has documented detail benchmarking results. This benchmark was prepared on April 14th, 2023.

####  Performance Test for Tables with 14 Columns and Varying Numbers of Rows (10,000 ~ 1 Billion)

Usually both very small and very large tables will be disadvantageous in this measure for many software. If the time measures are represented by nominal time, it is meaningless for comparison among different table size scenarios. Duration of billion rows will be very large, 10,000 rows will be very small.

Testing Machine: Intel i9 8-Cores CPU, 32G RAM, 500GB NVMe SSD 

### Distinct

```
ReadFile{FileName.csv ~ Table}
Distinct{Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency}
WriteFile{Table | * ~ OutputFileName.csv}
```

!  It means how many seconds required for each size of table to process from 1 million rows equivalent data size. 

|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|Distinct  |         0.01 |    4.020 |    1.580 |             60.7% |
|          |          0.1 |    0.938 |    0.712 |             24.1% |
|          |            1 |    0.213 |    0.187 |             11.9% |
|          |           10 |    0.123 |    0.115 |              7.0% |
|          |          100 |    0.114 |    0.093 |             18.4% |
|          |         1000 |    0.172 |    0.104 |             39.6% |


![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/Distinct.png)

### GroupBy

```
ReadFile{FileName.csv ~ Table}
GroupBy{Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency 
  =>  Count() Max(Quantity) Min(Quantity) Sum(Quantity)}
WriteFile{Table | * ~ OutputFileName.csv}
```

|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|GroupBy   |         0.01 |    3.420 |    1.700 |             50.3% |
|          |          0.1 |    1.034 |    1.526 |            -32.2% |
|          |            1 |    0.272 |    0.347 |            -21.8% |
|          |           10 |    0.154 |    0.227 |            -32.2% |
|          |          100 |    0.129 |    0.228 |            -43.4% |
|          |         1000 |    0.191 |    0.239 |            -20.1% |


![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/GroupBy.png)

### JoinTable

```
ReadFile{Master.csv ~ Master}
BuildKeyValue{Master | Ledger,Account,Project ~ KeyValue} 
ReadFile{Transaction.csv ~ Transaction}
JoinKeyValue{Ledger,Account,Project => AllMatch(KeyValue)} 
WriteFile{Transaction | * ~ OutputFileName.csv}
```

|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|JoinTable |         0.01 |    3.140 |    6.140 |            -48.9% |
|          |          0.1 |    0.612 |    1.684 |            -63.7% |
|          |            1 |    0.397 |    0.271 |             31.8% |
|          |           10 |    0.388 |    0.159 |             59.1% |
|          |          100 |    0.886 |    0.178 |             79.9% |
|          |         1000 |     Fail |    0.302 |               N/A |

The author has requested the Polars team to provide a real streaming model for JoinTable.
See https://github.com/pola-rs/polars/issues/8231

![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/JoinTable.png)

# Peaks DataFrame
Peaks dataframe is a personal academic project that supports SQL-like processing of billions of rows using Go streaming model. Alternative goal is to achieve real-time processing up to 10 million rows per second to solve the data explosion that came with data capture from IoT devices.

I was amazed when I saw the processing speed of this benchmark in February 2023 (https://h2oai.github.io/db-benchmark/). I was interested in using Go to see if it could handle the billion row level processing of a large table. This test was being run on my desktop PC with 8 cores, 32GB RAM and 500GB NVMe SSD. According to the test of this demo video: https://youtu.be/a23u1rRc4pM, using Go streaming, it’s about 170 seconds, which can extract a 70 GB CSV file with 1 billion rows x 14 columns to output 8 columns of distinct value , i.e. 411 MB/s. After continuing effort to improve the algorithms, the processing time can be reduced to between 90s and 110s depending on the current temperature of my CPU and the number of uncontrollable background windows services running.
 
## Peaks Framework & Library
Peaks framework and library are under development. I am working on an end-user driven command flow that enables working with Peaks and third-party libraries. The Peak Library is a high-performance calculation engine that can be configured in either streaming mode or in-memory mode easily. If you use streaming mode with proper settings, you can process billions of rows on your desktop PC with 16GB or above memory. Both framework and library are written in Golang and are considering interface with Python, R and Node.js. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of my considerations.

## From WebNameSQL to Peaks DataFrame
WebNameSQL is a C# in-memory databending software that supports accountants using a web browser to interactive with accounting rules and tables for databending. However, this project became obsolete and it is replaced by a new project “Peaks DataFrame” to solve issues arising from real-time processing and big data. During a continuing effort in academic research, it is implemented new algorithms by using Golang which resulted in a performance gain of around 5X ~ 10X. Please refer to https://github.com/hkpeaks/peaks-framework

Commands to be re-implemented in the Peaks DataFrame will not be the same as those below. Considering there are too many commands for your learning and practice, further consolidation and improvement is necessary. The use cases are no longer restricted to accounting; for example, some use cases will cover bioinformatics.

## Open source
Peaks framework is considering go open source if there are a demand for this framework.

## Benchmarking
PeaksBenchmark.xlsx documents some benchmarking results. Currently, we are focusing on comparing Polars and Peaks. The next phase will cover DuckDB. For relevant scripts and data, please refer to https://github.com/financialclose/benchmarking.

### Distinct Function

Peaks's Command{Parameters}
```
> In-memory model
ReadFile{10MillionRows.csv ~ Table}
Distinct{Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table2}
WriteFile{Table2 | * ~ Peaks-Distinct10.csv}
WriteFile{Table | Ledger, Account, PartNo,Project,Contact ~ Peaks-Transaction.csv}

> Streaming model (streaming for reading file only)
CurrentSetting{StreamMB(1000)Thread(100)}
Distinct{1000MillionRows.csv | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table}
WriteFile{Table | * ~ Peaks-Distinct1000M.csv}
```

Polar's Python Code

```
import polars as pl
import time
import pathlib
q = (
     pl.scan_csv("Input/1000MillionRows.csv")      
    .select(["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"]).unique()
    )    

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-Distinct1000M.csv"
a.write_csv(path, separator=",")
e = time.time()
print("Polars GroupBy 1000M Time = {}".format(e-s))
```

### GroupBy Function

Peaks's Command{Parameters}
```
CurrentSetting{StreamMB(1000)Thread(100)}
GroupBy{1000MillionRows.CSV | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency 
  =>  Count() Max(Quantity) Min(Quantity) Sum(Quantity) ~ Table}
WriteFile{Table | * ~ Peaks-GroupBy1000M.csv}
```

Polar's Python Code

```
q = (
     pl.scan_csv("Input/1000MillionRows.csv")      
     .groupby(by=["Ledger", "Account", "PartNo", "Contact","Project","Unit Code", "D/C","Currency"])
    .agg([   
        pl.count('Quantity').alias('Quantity(Count)'),
        pl.max('Quantity').alias('Quantity(Max)'),
        pl.min('Quantity').alias('Quantity(Min)'),
        pl.sum('Quantity').alias('Quantity(Sum)'),        
    ])) 

a = q.collect(streaming=True)
path: pathlib.Path = "Output/Polars-GroupBy1000M.csv"
a.write_csv(path, separator=",")

```

### JoinTable Function (Streaming for reading and writing files)

Peaks's Command{Parameters}
```
CurrentSetting{StreamMB(500)Thread(100)}
ReadFile{Master.csv ~ Master}
BuildKeyValue{Master | Ledger,Account,Project ~ KeyValue} 
JoinKeyValue{1000MillionRows.csv | Ledger,Account,Project => AllMatch(KeyValue) ~ Peaks-JoinTable1000M.csv} 
```

Polar's Python Code

```
transaction = pl.read_csv("Input/1000MillionRows.CSV")            
master = pl.read_csv("Input/Master.CSV") 
joined_table = transaction.join(master, on=["Ledger","Account","Project"], how="inner")
joined_table.write_csv("Output/Polars-JoinTable1000M.csv")

```





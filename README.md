# Peaks DataFrame
Peaks DataFrame is a personal academic project that aims to provide an alternative to SQL statements for processing billions of rows using Go streaming model. The project started coding on February 18th, 2023 in Hong Kong SAR with the goal of achieving real-time processing up to 10 million rows per second. One of project aims to solve the data explosion that came with data capture from IoT devices.
 
## Peaks Framework and Library
Peaks DataFrame comprises of Peaks framework and library that are currently under development. I am working on an end-user driven command flow that enables working with Peaks and third-party libraries. The Peak Library is a high-performance calculation engine that can be configured in either streaming mode or in-memory mode easily. If you use streaming mode with proper settings, you can process billions of rows on your desktop PC with 16GB or above memory. Both framework and library are written in Golang and are considering interface with Python, R and Node.js. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of my considerations.

## File Format
Currently, Peaks DataFrame supports tables in CSV file format only. Other table formats such as GZIP(CSV), JSON, HTML, XLSX, Parquet, Lance, HDF5, ORC, Feather and etc are under consideration.

## From WebNameSQL to Peaks DataFrame
WebNameSQL is a C# in-memory databending software that supports accountants using a web browser to interactive with accounting rules and tables for databending. However, this project became obsolete and it is replaced by a new project “Peaks DataFrame” to solve issues arising from real-time processing and big data. During a continuing effort in academic research, it is implemented new algorithms by using Golang which resulted in a performance gain of around 5X ~ 10X.

Commands to be re-implemented in the Peaks DataFrame will not be the same as WebNameSQL. Considering there are too many commands for your learning and practice, further consolidation and improvement is necessary. The use cases are no longer restricted to accounting; for example, some use cases will cover bioinformatics.

## Benchmarking
PeaksBenchmark.xlsx documents some benchmarking results. Currently, we are focusing on comparing Polars and Peaks. The next phase will cover Pandas. For relevant scripts and data, please refer to https://github.com/financialclose/benchmarking.

##  Below Functions has been included in the benchmarking report "PeaksBenchmark.xlsx"

### Distinct Function

Peaks's Script
```
<< Command{Parameters} for Web request, Windows/Linux command line >>

> In-memory model
ReadFile{10MillionRows.csv ~ Table}
Distinct{Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table2}
WriteFile{Table2 | * ~ Peaks-Distinct10.csv}
WriteFile{Table | Ledger, Account, PartNo,Project,Contact ~ Peaks-Transaction.csv}

> Streaming model (streaming for reading file only)
CurrentSetting{StreamMB(1000)Thread(100)}
Distinct{1000MillionRows.csv | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table}
WriteFile{Table | * ~ Peaks-Distinct1000M.csv}

<< Tentative Python Code >>

import peaks as hk

Table = hk.ReadFile("10MillionRows.csv")
Table2 = hk.Distinct("Table | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency")
FilePath = hk.WriteFile("Table2 | * ~ Peaks-Distinct10.csv")
FilePath2 = hk.WriteFile("Table | Ledger, Account, PartNo,Project,Contact ~ Peaks-Transaction.csv")

  where the Python Code (" ") is equvalent to the original syntax {}. 
  And "variable =" is equvalent to the original syntax "~ TableName"
  When output table is a file name instead of in-memory table, the variable  
     will be a string which contain a full file path of the output file.
```

Polar's Python Code

```
> Streaming model
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

Peaks's Script
```
<< Command{Parameters} for Web request, Windows/Linux command line >>

> Streaming model (streaming for reading only)
CurrentSetting{StreamMB(1000)Thread(100)}
GroupBy{1000MillionRows.CSV | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency 
  =>  Count() Max(Quantity) Min(Quantity) Sum(Quantity) ~ Table}
WriteFile{Table | * ~ Peaks-GroupBy1000M.csv}
```

Polar's Python Code

```
> Streaming model
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

### JoinTable Function 

Peaks's Script
```
<< Command{Parameters} for Web request, Windows/Linux command line>>

> Streaming model (streaming for reading and writing only)
CurrentSetting{StreamMB(500)Thread(100)}
ReadFile{Master.csv ~ Master}
BuildKeyValue{Master | Ledger,Account,Project ~ KeyValue} 
JoinKeyValue{1000MillionRows.csv | Ledger,Account,Project => AllMatch(KeyValue) ~ Peaks-JoinTable1000M.csv} 
```

Polar's Python Code

```
> In-memory model (has requested Polars to support a real streaming model for JoinTable)
transaction = pl.read_csv("Input/1000MillionRows.CSV")            
master = pl.read_csv("Input/Master.CSV") 
joined_table = transaction.join(master, on=["Ledger","Account","Project"], how="inner")
joined_table.write_csv("Output/Polars-JoinTable1000M.csv")

```

## Compare Programming Language

Before deciding to develop the Peaks DataFrame, I conducted a study to determine which programming language was most suitable for me. I compared CSharp, Golang, and Rust with Pandas, Peaks, and Polars using a benchmark located in the folder ‘CompareProgrammingLanguage’. You can find a readme.pdf file inside the folder that shows a comparison of these languages with Pandas, Peaks, and Polars. This benchmark was prepared on April 20th, 2023 as I redid testing to cover Pandas, Peaks, and Polars.

Testing Machine: Intel i9 8-Cores CPU, 32G RAM, 500GB NVMe SSD

Further Information: https://www.linkedin.com/posts/max01_benchmarking-pandas-github-activity-7054824689273098241-P3VS?utm_source=share&utm_medium=member_desktop

## High Performance Web Pivot Table

This is my first .net project before I am using Golang. I will consider whether to re-implement this into Peak DataFrame or publish this .net project directly.
https://youtu.be/yfJnYQBJ5ZY

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/HighPerformanceWebPivotTable/WebPivotTable.png)](http://www.youtube.com/watch?v=yfJnYQBJ5ZY "Web Pivot Table")

## Latest News
For latest news about this academic project, please refer to https://www.linkedin.com/in/max01/recent-activity/all/





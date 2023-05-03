# Table of Contents

1. [Introduction](#introduction)
2. [Compare Programming Language](#compare-programming-language)
3. [Trial Version Is Coming Soon](#trial-version-is-coming-soon)
4. [Resource Utilization Does Matter](#resource-utilization-does-matter)
5. [Polars and Peaks Benchmarking](#polars-and-peaks-benchmarking)
6. [From WebNameSQL to Peaks DataFrame](#from-webNamesql-to-peaks-dataFrame)
7. [Peaks Roadmap](#peaks-roadmap)
8. [latest news](#latest-news)

## Introduction
Peaks DataFrame is a personal academic project that aims to provide an alternative to SQL statements for processing billions of rows using streaming or in-memory model to accelerate dataframe. The project began on February 18th, 2023 in Hong Kong SAR and aims to achieve real-time processing of up to 10 million rows per second on a single computing device and also saving your investment in cloud computing.

Currently, Peaks DataFrame have been innovating and testing a set of algorithms and data structures to support profound acceleration of the dataframe with limited memory. One of the project’s expected outcomes is to solve the data explosion that came with data capture from IoT devices, ERP, internet and data lake. By using a proper script settings, it can support streaming and in-memory models.

## Compare Programming Language

Folder: https://github.com/hkpeaks/peaks-framework/tree/main/CompareProgrammingLanguage

Before deciding to develop the Peaks DataFrame, it is conducted a study to determine which programming language was most suitable for this project. The author had compared CSharp, Golang, and Rust with Pandas, Peaks, and Polars using a benchmark located in the folder ‘CompareProgrammingLanguage’. You can find a readme.pdf file inside the folder that shows a comparison of these languages with Pandas, Peaks, and Polars. This benchmark was prepared on April 20th, 2023.

Testing Machine: Intel i9 8-Cores CPU, 32G RAM, 500GB NVMe SSD

Processing Time (In Second) of Read and Write CSV File

|               | 1 Million Rows |10 Million Rows |
| ------------- | -------------- |--------------- |
| Basic Programming                               |
| C Sharp       |          3.269 |         37.482 |
| Golang        |          2.743 |         27.351 |
| Rust          |          3.154 |         32.132 |
| Advanced Programming                            |
| Pandas        |          4.798 |         52.799 |
| Peaks         |          0.177 |          0.917 |
| Polars        |          0.406 |          3.625 |

The data structure implemented for the basic programming in a way that is similar to Parquet file format. It is extensively used key-value pairs, for example, use 1, 2, and 3 to represent unique values for each column. However, this extensive use of CPU and memory resources made Peaks DataFrame avoid using it again.

When it comes to data structures, bytearray is one of the most useful and memory-efficient. As for algorithms, parallel streaming for reading/writing files and querying is very powerful and can handle billions of rows even on a desktop PC with only 16GB RAM.

## Trial Version Is Coming Soon

<p align="center">
<img src="https://github.com/hkpeaks/peaks-framework/blob/main/InitialRelease.png" width=50% height=50%>
</p>

https://github.com/do-account  is a platform where you can publish free and trial versions of Peaks DataFrame with use cases. The tentative date for publishing is by the end of June 2023.

If you’re running a script with initial release of the Peaks DataFrame, you’ll have a 100-second timeout which should be more than enough for the 100-day trial period. If you’re not working with large datasets, a second should allow you to perform many steps of databending.

After the trial period ends, you can still use the app but there will be a 100MB file read/write limit. This is to encourage you to download the latest version again and again.

The special arrangement of after trial period also removes your concern that the latest version may not be available in the future. It’s worth noting that rows of a 100MB file is more than what an Excel worksheet can handle.

About monthly or bi-monthly, new commands, enhancements and bug fixs will be added to subsequent trial versions. However, it will not include commands which involve complex
implementation. Ready-to-use command scripts with sample data will be included in the distribution. For any reported critical bugs, it will be fixed and published as soon as practical.

The initial version will cover the following command groups and commands:-

| Command Group  | Command                        | Remark                                                 |                  
|----------------|------------------------------- |------------------------------------------------------- |
| CurrentSetting | CurrentSetting                 | adjust the size of the partition of your large file    |
|                |                                | and the number threads to match your data and machine  |
| IO             | ReadFile, WriteFile, SplitFile |                                                        | 
| Unique         | Distinct, GroupBy              |                                                        |
| JoinTable      | BuildKeyValue, JoinKeyValue    | two commands must be configured together               |
| Filter         | Select, SelectUnmatch          |                                                        |
| Append         | Append                         | for row/column with adding value/formula               |

The first release version will not include sorting because it requires more research works to solve the root problem of sorting billions of rows by many sorting columns (A/D).


## Resource Utilization Does Matter
JoinTable is an ETL function that is frequently used. However, it has been reported that JoinTable can be problematic when processing tables with billions of rows. 

Golang is a simple and beautiful programming language that allows JoinTable to implement ultra-performance streaming. For instance, it can process a 67.2GB CSV file and output 91GB. 

According to a performance chart, Peaks demonstrate high efficiency in resource utilization during the processing of billions of rows for JoinTable. Every peak of CPU utilization is due to data being loaded into memory for the current partition of a file. You can continue to enjoy YouTube during this intensive processing for less than 5 minutes long.

![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/JoinTableResourceUtilization.jpg)

Apart from JoinTable, this url https://youtu.be/9nxIDi2t1Bg is a demo video which apply a query statement "Select{1000MillionRows.csv | Ledger(L10…L15,L50…L55,L82…L88) Account(12222…12888,15555…16888) Project(>B28,<B22) ~ Peaks-Filter1000M.csv}" to select 15,110,000 rows from the 1 billion rows file. The whole processing time is 124 seconds running on a 3-year-old desktop PC with only 32GB RAM. Utilization of memory resources throughout the process is near half. Less resource demanding if comparing a JoinTable test.

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/FilterDemo.png)](http://www.youtube.com/watch?v=9nxIDi2t1Bg "Filter 15,110,000 Rows from 1 Billion Rows")

## Peaks Framework and Library
Peaks DataFrame comprises of Peaks framework and library that are currently under active development. 

FRAMEWORK: It is an open-source project that aims to promote an alternative standard of ETL expression. It provides a user-friendly command flow that enables working with Peaks and third-party libraries.

LIBRARY: It will be provided a free version of Go library which allows to be operated by the framework. It is a high-performance calculation engine that can be configured by the framework in either streaming mode or in-memory mode easily. 

RELEASES: It will be provided an all-in-one executable runtime for both Windows and Linux. The gRPC version of Peaks Framework which supports Python/Node.js/Java/Rust/.Net will be available in the next stage.

Both framework and library are written in Golang. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of considerations.

## File Format
Currently, Peaks DataFrame supports tables in CSV file format only and will support other popular table formats such as XLSX, JSON, PARQUET and FASTA.

XLSX is a popular file format for accounting. It’s likely that Peaks will support this format by considering the Excelize library for Golang. 

PARQUET is a format that is built to handle flat columnar storage data formats. It is designed for efficient data storage and retrieval. The format stores data in “row group” blocks that are divided into “column chunks” and then further divided into “data pages” .

FASTA format is a text-based format that supports bioinformatics. It is used for representing either nucleotide sequences or amino acid (protein) sequences. In this format, nucleotides or amino acids are represented using single-letter codes for five nucleobases— adenine (A), cytosine (C), guanine (G), thymine (T), and uracil (U). Unknown bases are represented by the letter N. The human genome is a diploid genome that contains 3.2 billion nucleotides. These nucleotides are packed into 23 pairs of chromosomes.

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

##  Examples of Script for Peaks and Polars

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

### Filter Function 

Peaks's Script
```
<< Command{Parameters} for Web request, Windows/Linux command line>>

> Streaming model (streaming for reading and writing only)
CurrentSetting{StreamMB(1000)Thread(100)}
Select{1000MillionRows.csv | Ledger(L10..L15,L50..L55,L82..L88) Account(12222..12888,15555..16888) 
       Project(>B28,<B22) ~ Peaks-Filter1000M.csv}
```

Polar's Python Code

```
> Streaming model 
df = pl.scan_csv('Input/1000MillionRows.csv')

filter = df.filter((((pl.col('Ledger') >= "L10") & (pl.col('Ledger') <= "L15")) | 
((pl.col('Ledger') >= "L50") & (pl.col('Ledger') <= "L55")) | ((pl.col('Ledger') >= "L82")
& (pl.col('Ledger') <= "L88"))) & (((pl.col('Account') >= 12222) & (pl.col('Account') <= 12888))
| ((pl.col('Account') >= 15555) & (pl.col('Account') <= 16888))) & ((pl.col('Project') > "B28")
| (pl.col('Project') < "B22")))

a = filter.collect(streaming=True)
print("Number of selected rows: {}", filter.select(pl.count()).collect());
path: pathlib.Path = "Output/Polars-Filter1000M.csv"
a.write_csv(path)

```

## From WebNameSQL to Peaks DataFrame

Peaks framework is derived by a .net project WebNameSQL. You can see the full specification of "WebNameSQL.pdf" from the repository. Peaks framework will have an improvement version based on WebNameSQL. Any software can implement this framework to standardise ETL expression similar to HTML5, which benefits for end-users. The author have over 10 years of experience in designing ETL expression covers 4 different designs. WebNameSQL is the best design, so Peaks framework will adopt this design with some of improvement, particularly to adapt Python code.

WebNameSQL is a C# in-memory databending software that supports accountants using a web browser to interactive with accounting rules and tables for databending. However, this project became obsolete and it is replaced by a new project “Peaks DataFrame” to solve issues arising from real-time processing and big data. During a continuing effort in academic research, it is implemented new algorithms by using Golang which resulted in a performance gain of around 5X ~ 10X.

WebNameSQL-Go Version is a prototype that uses Golang to rewrite some of the functions using similar algorithms and data structures as WebNameSQL-Csharp Version. The author aims to prove that Golang is more suitable for the next programming language.

Commands to be re-implemented in the Peaks DataFrame will not be the same as WebNameSQL. Considering there are too many commands for your learning and practice, further consolidation and improvement is necessary. The use cases are no longer restricted to accounting; for example, some use cases will cover bioinformatics. Very high performance is essential for this project, so algorithms and data structure of Peaks will be a significant different from WebNameSQL.

Further Information: https://www.linkedin.com/posts/max01_benchmarking-pandas-github-activity-7054824689273098241-P3VS?utm_source=share&utm_medium=member_desktop

## High Performance Web Pivot Table

Folder: https://github.com/hkpeaks/peaks-framework/tree/main/WebPivotTable

The author created a .NET project called “WebPivotTable” before using Golang. He is considering whether to re-implement this visual into Peaks DataFrame. The original project’s source code can be found above. The last bug fix was made on August 3rd, 2020. On April 30th, 2023, the author published this project again. After downloading and building the runtime using Visual Studio 2022 Community Version, a folder called “youFast” was generated. Clicking a youFast which will start a websocket server and open your browser with default data. The app supports csv file only. The websocket runs on local host “ws://127.0.0.1:5000/”. The websocket is an open source and can be downloaded from https://github.com/statianzo/Fleck.

https://youtu.be/yfJnYQBJ5ZY

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/WebPivotTable/WebPivotTable.png)](http://www.youtube.com/watch?v=yfJnYQBJ5ZY "Web Pivot Table")

## Author's Experience in Dataframe Development
The author had developed dataframe software five times during past 13 years, gaining experience in designing better data structures and algorithms that require fewer CPU and memory resources. He developed the software three times while employed by FlexSystem, the fourth time for YouFast Desktop - a high-performance Web Pivot Table, and the fifth time for WebNameSQL. 

Peaks DataFrame which is expected to be the final research and development in dataframe software as its algorithms and data structures have been proven successful. It will be designing as a next-generation accounting software that specializes in management accounting and consolidation. Peaks DataFrame will also cover some special topics in machine learning and bioinformatics. So, it is obviously not to replace Polars, Pandas and Pytorch but rather complement them. 

When it comes to data structures, bytearray is one of the most useful and memory-efficient. As for algorithms, parallel streaming for reading/writing files and querying is very powerful and can handle billions of rows even on a desktop PC with only 8 cores and 32GB RAM. The author had conducted some research in bioinformatics and had learned that RNA polymerase is responsible for transcribing DNA into RNA while ribosomes are responsible for translating RNA into proteins. The author was impressed by the high efficiency of protein production from transcription to translation, so the data model of Peaks is somewhat similar to these biological operations.

## Peaks Roadmap 

Based on user responses after releasing the first beta version, the next possible scenario is to release a new software called ‘Peaks Consolidation’ next year. This constitutes a redevelopment of the DotNet WebNameSQL accounting module into the Golang Peaks. This software will help with statutory compliance and management ad-hoc reporting for financial consolidation with thousands of business units and legal entities. 

Peaks Consolidation can help you solve massive data of account reconciliation, foreign currency translation, elimination of inter-company transactions, re-alignment of different year-end dates with different consolidation methods (i.e., proportional accounting, equity accounting, and full consolidation with calculation of minority interest). It standardizes your dataset and is empowered by a conditional mapping engine with effective date of change calculation rules. This engine can be used to solve your ever-changing incentive plans for different stakeholder management, account allocation and sharing holdings in investment in subsidiaries, joint ventures and associates.

Peaks DataFrame can help you with basic accounting tasks such as amortization, voucher generation, and calculation of segmental multi-currencies account periodical balance. Peaks Consolidation will extend to support your advanced accounting. The mission of Peaks Accounting development roadmap is to empower you to achieve advanced accounting automation anywhere, progressing towards real-time accounting automation.

## Latest News
For latest news about this academic project, please refer to https://www.linkedin.com/in/max01/recent-activity/all/




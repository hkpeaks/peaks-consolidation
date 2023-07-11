## Introduction
Peaks Consolidation has been successfully innovated a set of algorithms that leverage the power of desktop computing. One of the project’s intended goals is to solve the data explosion that resulted from data gathering from IoT devices, ERP, the internet, and data lakes. It supports auto-switching between streaming and in-memory models. 

On July 8, 2023, the Peaks project achieved a new milestone in processing giant files. The below video demonstrated four different scenarios:-

Demo 1. Previewing a 250GB CSV file within a few milliseconds.

Demo 2. Previewing CSV files with many columns.

Demo 3. Previewing CSV files with proper alignment fit for text, integer, negative and decimal.

Demo 4. Previewing the 250GB CSV file using a script and getting batch of random samples for 1,000 times less than half second.

The 2nd pre-release will include the new functions. The video URL: https://youtu.be/lX2HKLDOfwk

[![ReadSample](https://github.com/hkpeaks/peaks-framework/blob/main/Documents/Benchmark20230708/ReadSample.png)](https://youtu.be/lX2HKLDOfwk "ReadSample")

## Ultra Speed for Query Billion Rows

In comparison to other consolidation solutions, such as Oracle HFM, SAP BPC, IBM Cognos Controller, and TM1, Peaks Consolidation's rule creation is extraordinarily straightforward for any user. Furthermore, CurrentSetting enables you to use your computing device to deal with billions of rows of queries, whether it's a single file or a folder comprising numerous files. It can also serve as an interface to your database. Here's an example of rule creation:-

1. Select{1000MillionRows.csv | Ledger(L10..L20)Account(15000..16000) ~ Table}
2. Select{Project(>B25,<B23)}
3. GroupBy{Ledger, Account, Project, D/C, Currency 
        => Sum(Quantity) Sum(Original Amount) Sum(Base Amount)}
4. WriteFile{Table ~ FilterResults.csv}

The complete processing time takes only 85 seconds on a desktop PC with 8 cores and 32GB of memory with a file size of 67.2GB. We are constantly working to enhance the algorithm, which results in higher performance while using fewer resources.

```
## If your file is small e.g. less than 10 million rows, normally it is no need to configure below CurrentSetting.

CurrentSetting{StreamMB(1000)Thread(100)} 

## StreamMB(1000) allows you to adjust the partition size of data streaming to fit your hardware configuration. The author found that 1000 (e.g., 1GB) is suitable for their computer with 32GB of memory.
## Thread(100) allows you to maximize the usage of multi-core CPU. The author found that 100 threads is suitable for their computer with 8 cores.

## Scenario A: if configure file name as data source, Peaks will auto-detect to use streaming/in-memory model automatically

1. Select{1000MillionRows.csv | Ledger(L10..L20)Account(15000..16000) ~ Table}

## Scenario B: if user want to ensure the use in-memory model when their machine has sufficient memory

## 1b1. ReadFile{1000MillionRows.csv ~ Table}
## 1b2. Select{Ledger(L10..L20)Account(15000..16000) ~ Table}

## Scenario C: if user want to output a large proportion of data from source files

1c. Select{1000MillionRows.csv | Ledger(L10..L98) ~ LargeFile.csv}

## Streaming will be implemented throughout the read file, filter and write file in parallel.

```

## Download Pre-release of Peaks v23.05.18

You can download a free and trial versions of runtime for Windows/Linux with use cases from https://github.com/hkpeaks/peaks-consolidation/releases. If you have any problem during your testing of the software, please report us in the issues section.

About monthly or bi-monthly, new commands, enhancements and bug fixs will be added to subsequent trial versions. However, it will not include commands which involve complex implementation. Ready-to-use command scripts with sample data will be included in the distribution. For any reported critical bugs, it will be fixed and published as soon as practical.

The first version publised on May 19, 2023 which cover the following command groups and commands:-

| Command Group  | Command                          | Remark                                                 |                  
|----------------|--------------------------------- |------------------------------------------------------- |
| CurrentSetting | CurrentSetting                   | adjust the size of the partition of your large file    |
|                |                                  | and the number threads to match your data and machine  |
| IO             | ReadFile, WriteFile, SplitFile   |                                                        | 
| Unique         | Distinct, GroupBy                |                                                        |
| JoinTable      | BuildKeyValue, JoinKeyValue      | two commands must be configured together               |
| Filter         | Select, SelectUnmatch            |                                                        |

## Peaks Consolidation
It comprises the following elements:-

### Peaks Framework
It is an open-source project that aims to promote an alternative standard of ETL expression. It provides a user-friendly command flow that enables working with dataFrame and third-party softwares. When it comes to data structures, bytearray is one of the most useful and memory-efficient. 

Included in the framework, it cover a workflow management system that helps you manage your table partitions. Parallel streaming is a powerful technique for reading/writing files and querying algorithms. It can handle billions of rows on a desktop PC with only 8 cores and 32GB RAM. Instead of using arrow as an intermediate of in-memory dataset, parallel streaming performs byte-to-byte conversion of input bytearray directly throughout ETL processes to output bytearray. 

Currently, the framework offers the following open source ETL commands:-

- CurrentSetting{}
- ReadFile{}
- WriteFile{}
- SplitFile{}
- ExpandFile{}
- CombineFile{} (This command is developed after the publishing of the pre-release version 23.05.18)

### Peaks Databending
Databending is an add-on module that supports the manipulation of your data using different calculation rules. Currently, this module offers the following commands:- 

- Distinct{}
- GroupBy{}
- BuildKeyValue{}
- JoinKeyValue{}
- Select{}
- SelectUnmatch{}

### Peaks Releases
It provided an all-in-one executable runtime for both Windows and Linux. https://github.com/hkpeaks/peaks-consolidation/releases
The gRPC version of Peaks Framework which supports Python/Node.js/Java/Rust/.Net will be available in the next stage.

Both framework and dataframe are written in Golang. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of considerations.

### Peaks Use Cases
From time to time use cases will be published in the Github source code section, Pre-release and YouTube channel https://www.youtube.com/@hkpeaks/videos

## Completed Development for Next Pre-release

It is planned to update the above source code files and publish 2nd pre-release runtime during Aug ~ Sep 2023. New functions have been built:-

### The CLI Command: Do 
- Previously "DO" can support DO + script file name 
- Now support DO + filename.csv to display meta data and sample rows of your file

### New Commands:
- AddColumn{}: Add new column by math function e.g. Add, Subtract, Multiply and Divide.
- OrderBy{}: supports to sort billions of rows using 16GB+ memory (1st pre-release supports billion-row distinct, group by, filter and join table)
- Display{}: it is used to print few rows to screen with auto-alignment for text and real number
- ReadSample{}: supports to get fix or random sample of rows from csv file instantly, it is very useful for very large file e.g. >100GB.
- SplitFile2Folder{}: allows to filter a big CSV file or a folder which contains many CSV file to a folder/sub-folder which results many table partitions

### Amend Commands:
- ReadFile{} and WriteFile{} will be changed to Read{} and Write{}
- Select{} and SelectUnmatch{} will be split into Select{}/Filter{} and SelectUnmatch{}/FilterUnmatch{}.
  Select{} is used to select columns while filter{} is used to select rows.
  If you need to select columns and filter rows at the same time, 2 commands are interchange.
  But SelectUnmatch{} and FilterUnmatch{} are not interchange.

### New Features of Current Commands:
- Supports to read all CSV files from a folders which support in-memory or streaming model
- Auto-detect to combine different queries such as filter + group by, filter current columns + join table + filter for join columns
- Filter function supports to read data from particular folders generated by the command "SplitFile2Folder" which can speed-up filter time significantly
- JoinKeyValue{} supports Filter() and FilterUnmatch() in addition to AllMatch().
  Not only it supports join new columns, but also it supports to filter rows by a table which contain multi-dimensional distinct values.

### Bug Fix:
- Polars created csv is a little bit smaller size than Peaks of same dataset. Peaks has adapted to it to read propertly.  

Based on some use cases, coming pre-release is likely to be significantly faster than the top 2 fastest dataframe software DuckDB 0.8.0 and Polars 0.18.0. These 2 are most recent benchmark of the Peaks https://youtu.be/ctxX1O1-OKk & https://youtu.be/bzess7_pKoc

[BillionRowsTestingLog](https://github.com/hkpeaks/peaks-consolidation/tree/main/Documents/Peaks230518Pre-Release/BillionRowsTestingLog) is a set of 
processing logs included in the 1st pre-release delivery. These are foucs on billion-row databending exercises.

Based on a recent test case, it able to handle 7 billion-rows achieving processing speed 1 billion-row / minute. https://youtu.be/1NV0wkGjwoQ

## Under Research Stage

- Develop a data simulator that uses probability distribution.
- Supports CLI interactive mode, so you can maintain the in-memory table for subsequent queries  
- Write HTML5 table with bootstrap 5 and send it through websocket to local browser. This apply to view your local giant files instantly e.g. > 100GB.
- Pyeaks: Peaks for Python. Pyeaks covers some of the current Peaks functions and new functions. It will support “pip install Pyeaks”. If user downloads the Peaks runtime "DO" subsequently, Pyeaks can call most of the DO functions in your Python scripts.   
- Native data store which partitioning your table by a foldertree and support data amendment by selecting distinct column names.
- Implement gRPC with websocket to support connection over the internet or different local machines.
- Support read/write Parquet, JSON table and XLSX file formats.   
- Parallel query with SQL server.
- Supports composite queries in a single statement that can be executed within an inner loop to minimize hardware resource consumption.

    ReturnTable = SourceTable;Filter{};JoinKey2Value{};AddColumn{};Filter{};GroupBy{}

    ReturnTable = SourceTable.csv;Filter{};JoinKey2Value{};AddColumn{};Filter{};GroupBy{}

    ReturnValue = SourceTable.csv;Filter{};JoinKey2Value{};AddColumn{};Filter{};GroupBy{}.Write{Result.csv}

    Where ReturnTable is an in-memory table and ReturnValue is processing staus of writing csv file to disk.

    First filter you may use to filter transactions for JoinKey2Value{}

    Second filter you may use to filter new column after JoinKey2Value{} and AddColumn{}
  
- New Peaks query functions (these were done in C# WebNameSQL except GroupBy virtual column):-
  - Converting different date formats
  - Group By virtual columns - supports time series data table
  - Reconciliation - compares differences of 2 tables by selecting distinct column names
  - Table2Cell - summarizes cells of a table to a number or text by maths/statistics
  - Conditional Action By Cell Value 
  - Build Balance - enables crosstab results which can have monthly year-to-date balance
  - Crosstab and reverse crosstab


## CLI Interactive Mode

This new function will be inclued in the 2nd pre-release distribution. To enter interactive mode in CLI and chat with the Peaks for the purpose of operating disk files and in-memory tables, type “do”. In interactive mode, you can use commands such as “help”, “memory”, and “disk”. Other commands can be also run by a script file. The “memory” command lists which table is currently resident in in-memory while the “disk” command lists data files in your input and output folder. You can type “help” to get a usage for each simplified SQL statement1.
  
  do>>help

  AddColumn{Column, Column => Math(NewColName)} where Math includes Add, Subtract, Multiply & Divide
      
  BuildKeyValue{Column, Column ~ KeyValueTableName} 
  
  CurrentSetting{StreamMB(Number) Thread(Number)}
    
  Distinct{Column, Column}
  
  Filter{Column(CompareOperator Value) Column(CompareOperator Value)}
 
  FilterUnmatch{Column(CompareOperator Value) Column(CompareOperator Value)}
 
  GroupBy{Column, Column => Count() Sum(Column) Max(Column) Min(Column)}
  
  JoinKeyValue{Column, Column => JoinType(KeyValueTableName)} where JoinType includes AllMatch, Filter & FilterUnmatch
     
  OrderBy{PrimaryCol(A or D) SecondaryCol(A or D)}
  
  OrderBy{SecondaryCol => SplitFileByFolder(PrimaryCol) ~ FolderName or FileName.csv}  
  
  Read{FileName.csv ~ TableName}
    
  ReadSample{StartPosition%(Number) ByteLength(Number)}
  
  ReadSample{Repeat(Number) ByteLength(Number)}
  
  Select{Column, Column}
  
  SelectUnmatch{Column, Column}  
  
  SplitFile{FileName.csv ~ NumberOfSplit}
    
  SplitFileByFolder{Column, Column ~ SplitFolderName}
  
  View{TableName}
    
  Write{TableName ~ FileName.csv or %ExpandBy100Time.csv}

While interacting with the interactive mode, the system not only runs your command on demand but also records it in a script file automatically. This allows you to amend and run the script subsequently.
    
### Additional Query Command Setting 
    
  QueryCommand{SourceTable Or FileName.csv Or FilePath/*.csv | QuerySetting}
  
  QueryCommand{QuerySetting ~ ReturnTable Or FileName.csv} except OrderBy & SplitFileByFolder
  
  Compare operator includes >, <, >=, <=, =, != & Range e.g. 100..200
  
  Compare integer or float e.g. Float > Number, Float100..200





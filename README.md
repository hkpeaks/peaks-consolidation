## In-memory Data Transformation

Peaks Query is fast, simple and flexible. Here's an example of a script file:-

Read{Master.csv ~ Master} 

Read{Fact.csv ~ Table}

Write{Table ~ %ExpandBy10Time.csv}

#### Test 1: JoinTable to Add 2 Column and Select Column
JoinTable{Outbox/%ExpandBy10Time.csv | Quantity, Unit_Price => InnerJoin(Master)Multiply(Amount) ~ Result-JoinTable}

Select{Result-JoinTable | Date,Shop,Product,Quantity,Amount ~ Result-SelectColumn}

#### Test 2: BuildKeyKeyValue + JoinKeyValue + AddColumn = JoinTable of Test 1
BuildKeyValue{Master | Product, Style ~ MasterTableKeyValue}

JoinKeyValue{Outbox/%ExpandBy10Time.csv | Product, Style => AllMatch(MasterTableKeyValue) ~ Result-BuildKeyValue}

AddColumn{Result-BuildKeyValue | Quantity, Unit_Price => Multiply(Amount) ~ Result-AddColumn}

#### Test 3: Filter and FilterUnmatch
Filter{Result-AddColumn | Amount(Float > 50000) ~ Result-Filter}

FilterUnmatch{Result-AddColumn | Amount(Float > 50000) ~ Result-FilterUnmatch}


#### Test 4: Distinct and OrderBy
Distinct{Result-Filter | Date, Shop, Product, Style ~ Result-Distinct-Match}

Distinct{Result-FilterUnmatch |  Date, Shop, Product, Style ~ Result-Distinct-Unmatch}

OrderBy{Result-Distinct-Unmatch | Shop(A)Product(A)Date(D) ~ Result-Distinct-Unmatch-OrderAAD}


#### Test 5: GroupBy 
GroupBy{Result-Filter | Product, Style => Count() Sum(Quantity) Sum(Amount) ~ Result-GroupBy-Match}

GroupBy{Result-FilterUnmatch | Product, Style => Count() Sum(Quantity) Sum(Amount) ~ Result-GroupBy-Unmatch}

#### Test 6: Write to Disk
Write{Result-JoinTable ~ Result-JoinTable.csv}

Write{Result-SelectColumn ~ Result-SelectColumn.csv}

Write{MasterTableKeyValue ~ MasterTableKeyValue.csv}

Write{Result-AddColumn ~ Result-AddColumn.csv}

Write{Result-Filter ~ Result-Filter.csv}

Write{Result-FilterUnmatch ~ Result-FilterUnmatch.csv}

Write{Result-Distinct-Match ~ Result-Distinct-Match.csv}

Write{Result-Distinct-Unmatch ~ Result-Distinct-Unmatch.csv}

Write{Result-Distinct-Unmatch-OrderAAD ~ Result-Distinct-Unmatch-OrderAAD.csv}

Write{Result-GroupBy-Match ~ Result-GroupBy-Match.csv}

Write{Result-GroupBy-Unmatch ~ Result-GroupBy-Unmatch.csv}

Note: Command{Extraction | Transformation ~ Load}    

Demo Video: https://youtu.be/5Jhd1WwgfYg

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
| Filter         | Filter, FilterUnmatch            |                                                        |

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

### Peaks Query
Query is an add-on module that supports the manipulation of your data using different calculation rules. Currently, this module offers the following commands:- 

- Distinct{}
- GroupBy{}
- BuildKeyValue{}
- JoinKeyValue{}
- Filter{}
- FilterUnmatch{}

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
- Supports CLI interactive mode, so you can maintain the in-memory table for subsequent queries  

### New Commands:
- AddColumn{}: Add new column by math function e.g. Add, Subtract, Multiply and Divide.
- OrderBy{}: supports to sort billions of rows using 16GB+ memory (1st pre-release supports billion-row distinct, group by, filter and join table)
- Display{}: it is used to print few rows to screen with auto-alignment for text and real number
- ReadSample{}: supports to get fix or random sample of rows from csv file instantly, it is very useful for very large file e.g. >100GB.
- SplitFile2Folder{}: allows to filter a big CSV file or a folder which contains many CSV file to a folder/sub-folder which results many table partitions
- JoinTable{}: allows to join fact table with a master table and to add new column by refer columns from the fact and master table.

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


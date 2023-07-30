## In-memory ETL 
## Command{Extraction | Transformation ~ Load}    

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


## Command List
  * AddColumn{Column, Column => Math(NewColName)}
      where Math includes Add, Subtract, Multiply & Divide
  * BuildKeyValue{Column, Column ~ KeyValueTableName}
    CurrentSetting{StreamMB(Number) Thread(Number)}
  * Distinct{Column, Column}
 1* Filter{Column(CompareOperator Value) Column(CompareOperator Value)}
 1* FilterUnmatch{Column(CompareOperator Value) Column(CompareOperator Value)}
  * GroupBy{Column, Column => Count() Sum(Column) Max(Column) Min(Column)}
 2* JoinKeyValue{Column, Column => JoinType(KeyValueTableName)}
     where JoinType includes AllMatch, Filter & FilterUnmatch
    JoinTable{Column, Column => JoinType(KeyValueTableName) Math(NewColName)}
 3* OrderBy{PrimaryCol(Sorting Order) SecondaryCol(Sorting Order)}
  * OrderBy{SecondaryCol(Sorting Order) => CreateFolderLake(PrimaryCol) ~ FolderName or FileName.csv}
    Read{FileName.csv ~ TableName}
  * ReadSample{StartPosition%(Number) ByteLength(Number)}
  * ReadSample{Repeat(Number) ByteLength(Number)}
    Resume{FileName}
  * Select{Column, Column}
  * SelectUnmatch{Column, Column}
    SplitFile{FileName.csv ~ NumberOfSplit}
  * CreateFolderLake{Column, Column ~ SplitFolderName}
    View{TableName}
    Write{TableName ~ FileName.csv or %ExpandBy100Time.csv}

    Additional Query Command Setting:
  * QueryCommand{SourceTable Or FileName.csv Or FilePath/*.csv| QuerySetting}
  * QueryCommand{QuerySetting ~ ReturnTable Or FileName.csv} except OrderBy & CreateFolderLake
  1 Compare operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
  1 Compare integer or float e.g. Float > Number, Float100..200
  2 Use BuildKeyValue & JoinKeyValue supports JoinTable result
  2 Use JoinTable supports JoinTable & AddColumn result
  3 To sort text, use either A or D, to sort real numbers, use either FloatA or FloatD

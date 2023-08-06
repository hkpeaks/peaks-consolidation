## New Query Statement for File, In-memory Table and Network Stream

Note: Use of "." to indicate it is member of your defined function is optional. 
First line is to define data extraction and data load. Below are 3 possible scenarios:-

UserDefineFunctionName = from Extraction to Load

Or 

UserDefineFunctionName = from Extraction, Extraction, Extraction to Load

Or

UserDefineFunctionName = from Extraction to Load, Load, Load

You can define query/data transformation function from second line and after.

Examples:

#### ExpandFile = from Fact.csv to 1BillionRows.csv

.ExpandFactor: 123

#### JoinScenario1 = from 1BillionRows.csv to Test1Results.csv

.JoinTable: Quantity, Unit_Price => InnerJoin(Master)Multiply(Amount)

.OrderBy: Date(D) => CreateFolderLake(Shop)

.Select: Date,Shop,Style,Product,Quantity,Amount

#### JoinScenario2 = from 1BillionRows.csv to Test2AResults.csv

.JoinTable: Product, Style => AllMatch(Master.csv)

.AddColumn: Quantity, Unit_Price => Multiply(Amount)

.Filter: Amount(Float > 50000)

.GroupBy: Product, Style => Count() Sum(Quantity) Sum(Amount)

.OrderBy: Shop(A)Product(A)Date(D)

#### SplitFile = from Test1Results.csv to FolderLake

.CreateFolderLake: Shop

#### FilterFolder = from Outbox/FolderLake/S15/*.csv to Result-FilterFolderLake.csv

.Filter: Product(222..888) Style(=F)

#### ReadSample2View = from Outbox/Result-FilterFolderLake.csv to SampleTable

.ReadSample: StartPosition%(0) ByteLength(100000)

.View




## Command List

   AddColumn{Column, Column => Math(NewColName)} 
   
        where Math includes Add, Subtract, Multiply & Divide
    
   BuildKeyValue{Column, Column ~ KeyValueTableName}
   
   CurrentSetting{StreamMB(Number) Thread(Number)}
  
   Distinct{Column, Column}
 
   Filter{Column(CompareOperator Value) Column(CompareOperator Value)}
 
   FilterUnmatch{Column(CompareOperator Value) Column(CompareOperator Value)}

        where Compare operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
              Compare integer or float e.g. Float > Number, Float100..200
   
   GroupBy{Column, Column => Count() Sum(Column) Max(Column) Min(Column)}
   
   JoinKeyValue{Column, Column => JoinType(KeyValueTableName)} 
        
        where JoinType includes AllMatch, Filter & FilterUnmatch
   
   JoinTable{Column, Column => JoinType(KeyValueTableName) Math(NewColName)}

        where JoinType includes AllMatch & InnerJoin
   
   OrderBy{PrimaryCol(Sorting Order) SecondaryCol(Sorting Order)}       
  
   OrderBy{SecondaryCol(Sorting Order) => CreateFolderLake(PrimaryCol) ~ FolderName or FileName.csv}

        where Sorting Order represents by A or D, to sort real numbers, use either FloatA or FloatD
 
   Read{FileName.csv ~ TableName}
   
   ReadSample{StartPosition%(Number) ByteLength(Number)}
   
   ReadSample{Repeat(Number) ByteLength(Number)}   
   
   Select{Column, Column}
   
   SelectUnmatch{Column, Column}
   
   SplitFile{FileName.csv ~ NumberOfSplit}
   
   CreateFolderLake{Column, Column ~ SplitFolderName}
   
   View{TableName}

   Write{TableName ~ FileName.csv or %ExpandBy100Time.csv} 


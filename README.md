## New Query Statement for File, In-memory Table and Network Stream

Note: Use of "." to indicate it is member of your defined function is optional. 
However use of  "~" is mandatory to identify first line is "UserDefineFunctionName = Extraction ~ Load".

#### UserDefineFunctionName = Extraction ~ Load

. Transformation

#### UserDefineFunctionName = SourceFile/Table ~ ResultFile/Table

. Command: Setting

#### ExpandFile = Fact.csv ~ 1BillionRows.csv

.ExpandFactor: 123

#### JoinScenario1 = 1BillionRows.csv ~ Test1Results.csv

.JoinTable: Quantity, Unit_Price => InnerJoin(Master)Multiply(Amount)

.OrderBy: Date(D) => CreateFolderLake(Shop)

.Select: Date,Shop,Style,Product,Quantity,Amount

#### BuildKeyValueTable = Master.csv ~ KeyValueTable

.BuildKeyValue: Product, Style

#### JoinScenario2 = 1BillionRows.csv ~ Test2AResults.csv

.JoinKeyValue: Product, Style => AllMatch(KeyValueTable)

.AddColumn: Quantity, Unit_Price => Multiply(Amount)

.Filter: Amount(Float > 50000)

.GroupBy: Product, Style => Count() Sum(Quantity) Sum(Amount)

.OrderBy: Shop(A)Product(A)Date(D)

#### SplitFile = Test1Results.csv ~ FolderLake

.CreateFolderLake: Shop

#### FilterFolder = Outbox/FolderLake/S15/*.csv ~ Result-FilterFolderLake.csv

.Filter: Product(222..888) Style(=F)

#### ReadSample2View = Outbox/Result-FilterFolderLake.csv ~ SampleTable

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


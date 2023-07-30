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


## Command List

   AddColumn{Column, Column => Math(NewColName)} 
   
        where Math includes Add, Subtract, Multiply & Divide
    
   BuildKeyValue{Column, Column ~ KeyValueTableName}
   
   CurrentSetting{StreamMB(Number) Thread(Number)}
  
   Distinct{Column, Column}
 
   Filter{Column(CompareOperator Value) Column(CompareOperator Value)}
 
   FilterUnmatch{Column(CompareOperator Value) Column(CompareOperator Value)}

        where Compare operator includes >,<,>=,<=,=,!= & Range e.g. 100..200
   
   GroupBy{Column, Column => Count() Sum(Column) Max(Column) Min(Column)}
   
   JoinKeyValue{Column, Column => JoinType(KeyValueTableName)} 
        
        where JoinType includes AllMatch, Filter & FilterUnmatch
   
   JoinTable{Column, Column => JoinType(KeyValueTableName) Math(NewColName)}

        where JoinType includes AllMatch & InnerJoin
   
   OrderBy{PrimaryCol(Sorting Order) SecondaryCol(Sorting Order)}       
  
   OrderBy{SecondaryCol(Sorting Order) => CreateFolderLake(PrimaryCol) ~ FolderName or FileName.csv}

        where sorting order represents by A or D, to sort real numbers, use either FloatA or FloatD
 
   Read{FileName.csv ~ TableName}
   
   ReadSample{StartPosition%(Number) ByteLength(Number)}
   
   ReadSample{Repeat(Number) ByteLength(Number)}   
   
   Select{Column, Column}
   
   SelectUnmatch{Column, Column}
   
   SplitFile{FileName.csv ~ NumberOfSplit}
   
   CreateFolderLake{Column, Column ~ SplitFolderName}
   
   View{TableName}

   Write{TableName ~ FileName.csv or %ExpandBy100Time.csv} 

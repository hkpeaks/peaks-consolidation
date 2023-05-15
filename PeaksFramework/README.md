##  Examples of Script for Peaks and Polars

### Distinct Function

Peaks's Script
```
<< Command{Parameters} for Web request, Windows/Linux command line >>

> In-memory model
ReadFile{10MillionRows.csv ~ Table}
Distinct{Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table2}
WriteFile{Table2 ~ Peaks-Distinct10.csv}
WriteFile{Table ~ Peaks-Transaction.csv}

> Streaming model (streaming for reading file only)
CurrentSetting{StreamMB(1000)Thread(100)}
Distinct{1000MillionRows.csv | Ledger, Account, PartNo,Project,Contact,Unit Code, D/C,Currency ~ Table}
WriteFile{Table ~ Peaks-Distinct1000M.csv}

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
WriteFile{Table ~ Peaks-GroupBy1000M.csv}
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

### An Example of Databending Exercise
YouTube Demo Video: https://youtu.be/2MG1e41gloQ
```
## 67.2GB, 1 Billion Rows x 14 Columns 
## Filter Data from One Billion Rows CSV File
## Each Batch of Stream Read 1GB File
## Each Batch Is Subdivided by 100 Partitions and They Are Processed in Parallel

CurrentSetting{StreamMB(1000)Thread(100)}
Select{1000MillionRows.csv | Project(>B28,<B22)Ledger(L10..L12,L53..L55,L82..L85)D/C(=D) ~ FilteredTable}
Select{Contact(=C39,C32..C34)}
### All Columns Are Deem as Text, Config It as Float to Compare Real Number
Select{Quantity(Float500..600)Base Amount(Float>30000) ~ FilteredTable}

## Join "FilteredTable" from "MasterTable" to Append New Columns "Company" & "Cost Centre"

ReadFile{Master.csv ~ MasterTable}
BuildKeyValue{MasterTable | Ledger,Account,Project ~ KeyValue}
JoinKeyValue{FilteredTable | Ledger,Account,Project => AllMatch(KeyValue) ~ JoinedTable} 

## Summarized Data Based on New Columns "Company" & "Cost Centre"

GroupBy{JoinedTable | Cost Centre =>  Count() Max(Quantity) Min(Quantity) Min(Unit Price) Max(Unit Price) Sum(Base Amount) ~ Cost Centre}
GroupBy{JoinedTable | Company =>  Count() Max(Exchange Rate) Min(Exchange Rate) Max(Unit Price) Min(Unit Price) Sum(Base Amount) ~ Company}

## Output In-memory Table Data to CSV File

WriteFile{FilteredTable  ~ Result1000M-FilteredData.csv}
WriteFile{Cost Centre ~ Result1000M-GroupByCostCentre.csv}
WriteFile{Company ~ Result1000M-GroupByCompany.csv}
```

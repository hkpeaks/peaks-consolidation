## Introduction
Peaks Consolidation is currently developing and testing a set of algorithms and data structures to provide significant memory-constrained dataframe acceleration. Solving the data explosion that resulted from data gathering from IoT devices, ERP, the internet, and data lakes is one of the project's intended goals. It supports auto switching between streaming and in-memory models.

### Before you build runtime for the above source code written in Golang, please read the file main.go and view this demo video https://youtu.be/wL1fbY3JZ1Y.

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
| IO             | ReadFile, WriteFile              |                                                        | 
| Unique         | Distinct, GroupBy                |                                                        |
| JoinTable      | BuildKeyValue, JoinKeyValue      | two commands must be configured together               |
| Filter         | Select, SelectUnmatch            |                                                        |

## Peaks Consolidation
It comprises the following elements:-

### Peaks Framework
It is an open-source project that aims to promote an alternative standard of ETL expression. It provides a user-friendly command flow that enables working with dataFrame and third-party softwares. When it comes to data structures, bytearray is one of the most useful and memory-efficient. 
https://github.com/hkpeaks/peaks-consolidation/blob/main/PeaksFramework/data_structure.go

Included in the framework, it cover a workflow management system that helps you manage your table partitions. Parallel streaming is a powerful technique for reading/writing files and querying algorithms. It can handle billions of rows on a desktop PC with only 8 cores and 32GB RAM. Instead of using arrow as an intermediate of in-memory dataset, parallel streaming performs byte-to-byte conversion of input bytearray directly throughout ETL processes to output bytearray. 

### Peaks Databending
Databending is a proprietary software which is repsonsible to provide different ETL commands such as Distinct, GroupBy, BuildKeyValue, JoinKeyValue, Select, SelectUnmatch and more are coming. Working with the Peaks Framework, the unique implementation model has been tested on many billion-row experiments for these ETL  commands and has shown remarkable processing speed.

### Peaks Releases
It provided an all-in-one executable runtime for both Windows and Linux. https://github.com/hkpeaks/peaks-consolidation/releases
The gRPC version of Peaks Framework which supports Python/Node.js/Java/Rust/.Net will be available in the next stage.

Both framework and dataframe are written in Golang. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of considerations.

### Peaks Use Cases
From time to time use cases will be published in the Github source code section, Pre-release and its YouTube channel https://www.youtube.com/@hkpeaks/videos


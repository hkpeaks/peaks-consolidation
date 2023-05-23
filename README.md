## Introduction
Peaks Consolidation is a personal academic project that attempts to offer a cutting-edge framework of ETL expression to make it simpler to process billions of rows utilizing streaming or in-memory model to speed up dataframe. The initiative, which started on February 18th, 2023 in Hong Kong SAR, intends to save your investment in cloud computing while achieving real-time processing of up to 10 million rows per second on a single computer system.

Peaks Consolidation is currently developing and testing a set of algorithms and data structures to provide significant memory-constrained dataframe acceleration. Solving the data explosion that resulted from data gathering from IoT devices, ERP, the internet, and data lakes is one of the project's intended goals. It supports auto switching between streaming and in-memory models.

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

Included in the framework, it cover a workflow management system that helps you manage your databending exercises. The databending process is empowered by a high-performance calculation engine that can be worked in either streaming mode or in-memory mode.

Parallel streaming is a powerful technique for reading/writing files and querying algorithms. It can handle billions of rows on a desktop PC with only 8 cores and 32GB RAM. Instead of using arrow as an intermediate of in-memory dataset, parallel streaming performs byte-to-byte conversion of input bytearray directly throughout ETL processes to output bytearray. 

### Peaks Databending
Databending is a proprietary software which is repsonsible to provide different ETL commands such as Distinct, GroupBy, BuildKeyValue, JoinKeyValue, Select, SelectUnmatch and more are coming. Working with the Peaks Framework, the unique implementation model has been tested on many billion-row experiments for these ETL  commands and has shown remarkable processing speed.

### Peaks Releases
It provided an all-in-one executable runtime for both Windows and Linux. https://github.com/hkpeaks/peaks-consolidation/releases
The gRPC version of Peaks Framework which supports Python/Node.js/Java/Rust/.Net will be available in the next stage.

Both framework and dataframe are written in Golang. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of considerations.

### Peaks Use Cases
From time to time use cases will be published in the Github and its YouTube channel.

## File Format
Currently, Peaks Consolidation supports tables in CSV file format only and will support other popular table formats such as XLSX, JSON, PARQUET and FASTA.

XLSX is a popular file format for accounting. It’s likely that Peaks Consolidation will support this format by considering the Excelize library for Golang. 

PARQUET is a format that is built to handle flat columnar storage data formats. It is designed for efficient data storage and retrieval. The format stores data in “row group” blocks that are divided into “column chunks” and then further divided into “data pages” .

FASTA format is a text-based format that supports bioinformatics. It is used for representing either nucleotide sequences or amino acid (protein) sequences. In this format, nucleotides or amino acids are represented using single-letter codes for five nucleobases— adenine (A), cytosine (C), guanine (G), thymine (T), and uracil (U). Unknown bases are represented by the letter N. The human genome is a diploid genome that contains 3.2 billion nucleotides. These nucleotides are packed into 23 pairs of chromosomes.

## Resource Utilization Does Matter
JoinTable is an ETL function that is frequently used. However, it has been reported that JoinTable can be problematic when processing tables with billions of rows. 

Golang is a simple and beautiful programming language that allows JoinTable to implement ultra-performance streaming. For instance, it can process a 67.2GB CSV file and output 91GB. 

According to the Windows Task Manager for many testing results, Peaks Consolidation demonstrates high efficiency in resource utilization during the processing of billions of rows for JoinTable. You can continue to enjoy YouTube during the intensive data processing.

Apart from JoinTable, this url https://youtu.be/9nxIDi2t1Bg is a demo video which apply a query statement "Select{1000MillionRows.csv | Ledger(L10…L15,L50…L55,L82…L88) Account(12222…12888,15555…16888) Project(>B28,<B22) ~ Peaks-Filter1000M.csv}" to select 15,110,000 rows from the 1 billion rows file. The whole processing time is 124 seconds running on a 3-year-old desktop PC with only 32GB RAM. Utilization of memory resources throughout the process is near half. Less resource demanding if comparing a JoinTable test.

## Consolidation Rules for Financial Close and Budgeting

Peaks Consolidation is a software that supports both consolidation of financial data and non-financial data. It helps with statutory compliance and management ad-hoc reporting with thousands of business units and legal entities with implementing one or more of the following consolidation rules:-

1. Voucher generation e.g. reversal, amortization, acquisition, displosal, fair value adjustment
2. Segmental multi-currency account periodical balance e.g. Yearly, Weekly, Daily
3. Account allocation
4. Account reconciliation
5. Foreign currency

   a. Revaluation at entity level   
  
   b. Translation at consolidation level  
   
5. Elimination of inter-company transactions
6. Re-alignment of different year-end dates with different consolidation methods

   a. Proportional accounting
  
   b. Equity accounting
   
   c. Full consolidation with calculation of minority interest
   
7. Standardization of dataset

Your dataset can be standardized using a conditional mapping engine that is equipped with effective date of change calculation rules. This engine can help you address your ever-changing incentive plans for different stakeholder management, account allocation, and sharing holdings in investment in subsidiaries, joint ventures, and associates.

The mission of Peaks Consolidation development roadmap is to empower you to achieve advanced accounting automation anywhere, progressing towards real-time accounting automation.

## Possible Integration

The author loves using Golang because it is a high-performance programming language with a beautiful syntax design. It is exceptionally easy to learn and has strong support from the community. As a result, the author is considering integrating Peaks Consolidation with other software written in Golang. Below are some of projects why the author finds Golang is very attractive: -

### TiDB
TiDB is a distributed SQL database that supports Hybrid Transactional and Analytical Processing (HTAP) workloads.

### CloudQuery
CloudQuery is a serverless SQL query engine that can be used to query data from various cloud storage services.

### Docker
Docker is a containerization platform and runtime that allows developers to build, package, and deploy applications as containers.

### Kubernetes 
Kubernetes is an open-source container orchestration platform that automates the deployment, scaling, and management of containerized applications.

### gRPC
gRPC is a modern open-source high-performance Remote Procedure Call (RPC) framework that can run in any environment. It has an official Go implementation called grpc-go.

Since Golang doesn’t have a hyper-performance dataframe library similar to Polars for Rust, the author was motivated to create a better one for the Golang community.

## Introduction
Peaks DataFrame is a personal academic project that aims to provide an alternative to SQL statements for processing billions of rows using streaming or in-memory model to accelerate dataframe. The project began on February 18th, 2023 in Hong Kong SAR and aims to achieve real-time processing of up to 10 million rows per second on a single computing device and also saving your investment in cloud computing.

Currently, Peaks DataFrame have been innovating and testing a set of algorithms and data structures to support profound acceleration of the dataframe with limited memory. One of the project’s expected outcomes is to solve the data explosion that came with data capture from IoT devices, ERP, internet and data lake. By using a proper script settings, it can support streaming and in-memory models.

When it comes to data structures, bytearray is one of the most useful and memory-efficient. As for algorithms, parallel streaming for reading/writing files and querying is very powerful and can handle billions of rows even on a desktop PC with only 8 cores and 32GB RAM. The author had conducted some research in bioinformatics and had learned that RNA polymerase is responsible for transcribing DNA into RNA while ribosomes are responsible for translating RNA into proteins. The author was impressed by the high efficiency of protein production from transcription to translation, so the data model of Peaks is somewhat similar to these biological operations.

## High Performance Query for Billion Rows Dataset

Compared to other expensive ETL solutions, the rule setting of Peaks Framework is exceptionally simple for any user. Additionally, CurrentSetting{} allows you to leverage your computing device to deal with billions of rows of queries at your fingertips, whether it’s a single file or a folder containing many files.

```
CurrentSetting{StreamMB(1000)Thread(100)}

Select{1000MillionRows.csv | Ledger(L10..L20)Account(15000..16000) ~ Table}

Select{Project(>B25,<B23)}

GroupBy{Ledger, Account, PartNo,Project,D/C,Currency => Sum(Quantity) Sum(Original Amount) Sum(Base Amount)}

WriteFile{Table | * ~ FilterResults.csv}
```

The entire process running on a desktop PC with 8 cores and 32GB of memory takes only 85 seconds. We are continuously working to improve the algorithm, resulting in better performance


## Trial Version Is Coming Soon

<p align="center">
<img src="https://github.com/hkpeaks/peaks-framework/blob/main/InitialRelease.png" width=50% height=50%>
</p>

You can download a free and trial versions of Peaks DataFrame with use cases. If you have any problem during your testing of the software, please report us in the issues section.

About monthly or bi-monthly, new commands, enhancements and bug fixs will be added to subsequent trial versions. However, it will not include commands which involve complex implementation. Ready-to-use command scripts with sample data will be included in the distribution. For any reported critical bugs, it will be fixed and published as soon as practical.

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

## Resource Utilization Does Matter
JoinTable is an ETL function that is frequently used. However, it has been reported that JoinTable can be problematic when processing tables with billions of rows. 

Golang is a simple and beautiful programming language that allows JoinTable to implement ultra-performance streaming. For instance, it can process a 67.2GB CSV file and output 91GB. 

According to a performance chart, Peaks demonstrate high efficiency in resource utilization during the processing of billions of rows for JoinTable. Every peak of CPU utilization is due to data being loaded into memory for the current partition of a file. You can continue to enjoy YouTube during this intensive processing for less than 5 minutes long.

![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/JoinTableResourceUtilization.jpg)

Apart from JoinTable, this url https://youtu.be/9nxIDi2t1Bg is a demo video which apply a query statement "Select{1000MillionRows.csv | Ledger(L10…L15,L50…L55,L82…L88) Account(12222…12888,15555…16888) Project(>B28,<B22) ~ Peaks-Filter1000M.csv}" to select 15,110,000 rows from the 1 billion rows file. The whole processing time is 124 seconds running on a 3-year-old desktop PC with only 32GB RAM. Utilization of memory resources throughout the process is near half. Less resource demanding if comparing a JoinTable test.

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/Polars-PeaksBenchmarking/Chart/FilterDemo.png)](http://www.youtube.com/watch?v=9nxIDi2t1Bg "Filter 15,110,000 Rows from 1 Billion Rows")

## From WebNameSQL to Peaks DataFrame

Peaks framework is derived by a .net project WebNameSQL. You can see the full specification of "WebNameSQL.pdf" from the repository. Peaks framework will have an improvement version based on WebNameSQL. Any software can implement this framework to standardise ETL expression similar to HTML5, which benefits for end-users. The author have over 10 years of experience in designing ETL expression covers 4 different designs. WebNameSQL is the best design, so Peaks framework will adopt this design with some of improvement, particularly to adapt Python code.

WebNameSQL is a C# in-memory databending software that supports accountants using a web browser to interactive with accounting rules and tables for databending. However, this project became obsolete and it is replaced by a new project “Peaks DataFrame” to solve issues arising from real-time processing and big data. During a continuing effort in academic research, it is implemented new algorithms by using Golang which resulted in a performance gain of around 5X ~ 10X.

WebNameSQL-Go Version is a prototype that uses Golang to rewrite some of the functions using similar algorithms and data structures as WebNameSQL-Csharp Version. The author aims to prove that Golang is more suitable for the next programming language.

Commands to be re-implemented in the Peaks DataFrame will not be the same as WebNameSQL. Considering there are too many commands for your learning and practice, further consolidation and improvement is necessary. The use cases are no longer restricted to accounting; for example, some use cases will cover bioinformatics. Very high performance is essential for this project, so algorithms and data structure of Peaks will be a significant different from WebNameSQL.

YouTube Demo Video: https://youtu.be/6hwbQmTXzMc

[![WebNameSQL](https://github.com/hkpeaks/peaks-framework/blob/main/WebNameSQL-GoVersion/WebNameSQLScreen.png)](https://youtu.be/6hwbQmTXzMc "WebNameSQL")

## Author Experience in Dataframe Development

The author is a seasoned accountant and IT professional with over three decades of experience. He qualified as an accountant in 1998 and has worked with some of Hong Kong listed companies, in various capacities.

He was an Assistant Chief Accountant at PYI/Paul Y. Group from 2006 to 2008, where he managed the daily accounting operations of PYI Group and oversaw the implementation of FlexAccount V10 for two listed companies and their subsidiaries.

He was an Assistant Finance Manager at Giordano from 2008 to 2010, where he conducted user requirement analysis and solution sourcing for automating the consolidated financial statements of the listed company. He recommended FlexSystem to customize its draft ledger and query technology for the financial consolidation needs.

He was a consultant at FlexSystem from 2010 to 2020, where he acquired extensive experience with three dataframe software development: FESA Consolidation, LedgerBase and FlexCalc.

He retired in 2020 due to the COVID-19 pandemic, but continued to work on two visual projects using DotNet and HTML5: youFast Desktop (https://lnkd.in/gGB34cH) and WebNameSQL (https://lnkd.in/gkfREsvK).

He evaluated Rust, Golang and C++ and finished a prototype that migrated the WebNameSQL project from DotNet to Golang in the first four months of 2022. Then he took a break from software development and learnt bioinformatics using Bioconductor and machine learning using TensorFlow and Pytorch.

He is currently working on the cross platform Peaks DataFrame project, which uses gRPC to support different programming languages such as Rust, Golang, Node.js, DotNet and Python. He is 55 years old and remain young in mindset. 

Peaks DataFrame which is expected to be the final research and development in dataframe software as its algorithms and data structures have been proven successful. It will be designing as a next-generation accounting software that specializes in management accounting and consolidation. Peaks DataFrame will also cover some special topics in machine learning and bioinformatics. So, it is obviously not to replace Polars, Pandas and Pytorch but rather complement them. 

## Peaks Roadmap 

Based on user responses after releasing the first beta version, the next possible scenario is to release a new software called ‘Peaks Consolidation’ next year. This constitutes a redevelopment of the DotNet WebNameSQL accounting module into the Golang Peaks. This software will help with statutory compliance and management ad-hoc reporting for financial consolidation with thousands of business units and legal entities. 

Peaks Consolidation can help you solve massive data of account reconciliation, foreign currency translation, elimination of inter-company transactions, re-alignment of different year-end dates with different consolidation methods (i.e., proportional accounting, equity accounting, and full consolidation with calculation of minority interest). It standardizes your dataset and is empowered by a conditional mapping engine with effective date of change calculation rules. This engine can be used to solve your ever-changing incentive plans for different stakeholder management, account allocation and sharing holdings in investment in subsidiaries, joint ventures and associates.

Peaks DataFrame can help you with basic accounting tasks such as amortization, voucher generation, and calculation of segmental multi-currencies account periodical balance. Peaks Consolidation will extend to support your advanced accounting. The mission of Peaks Accounting development roadmap is to empower you to achieve advanced accounting automation anywhere, progressing towards real-time accounting automation.

The author loves using Golang because it is a high-performance programming language with a beautiful syntax design. It is exceptionally easy to learn and has strong support from the community. As a result, the author is considering integrating Peaks with other software written in Golang. Below are some of projects why the author finds Golang is very attractive: -

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

## Listening to Your Need

In the past, the author has developed two data visualization tools using DotNet and HTML5 with a focus on front-end development. Currently, he is working on back-end development with a focus on dataframe acceleration that supports billions of rows. He is exploring whether it is justified to do research on frontend (data visualization) and backend (data engineering) at the same time. He think it all depends on whether the data visualization and the data engineering community demands an integrated front-end and back-end stub.

The data visual stub of Peaks DataFrame Viwewe (Previously known as youFast Desktop) is built by DotNet+CSS+Javascript+Websocket without using ASP Web framework. If he works on front-end stub again, he will consider this combination Golang+Python+Javascript(with or without React.js)+gRPC, so it can support Polars, Peaks, and Pandas user groups at the same time.
 
After the delivery of the initial trial version of Peaks DataFrame, the author will make a decision on whether he should work on front-end and back-end research concurrently. It depends on collective views from you.

## Latest News
For latest news about this academic project, please refer to https://www.linkedin.com/in/max01/recent-activity/all/

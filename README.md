# Peaks DataFrame
Peaks DataFrame is a personal academic project that aims to provide an alternative to SQL statements for processing billions of rows using streaming or in-memory model to accelerate dataframe. The project began on February 18th, 2023 in Hong Kong SAR and aims to achieve real-time processing of up to 10 million rows per second on a single computing device. 

Currently, Peaks DataFrame have been innovating and testing a set of algorithms and data structures to support profound acceleration of the dataframe with limited memory. One of the project’s expected outcomes is to solve the data explosion that came with data capture from IoT devices, ERP, internet and data lake. By using a proper script settings, it can support streaming and in-memory models.

## Author's Experience in Dataframe Development
The author had developed dataframe software five times during past 13 years, gaining experience in designing better data structures and algorithms that require fewer CPU and memory resources. He developed the software three times while employed by FlexSystem, the fourth time for YouFast Desktop - a high-performance Web Pivot Table, and the fifth time for WebNameSQL.

The author’s sixth development in dataframe software is Peaks DataFrame, which has achieved a breakthrough point in software design and is expected to be the final one.
 
## Peaks Framework and Library
Peaks DataFrame comprises of Peaks framework and library that are currently under development. It is working on an end-user driven command flow that enables working with Peaks and third-party libraries. The Peaks library is a high-performance calculation engine that can be configured in either streaming mode or in-memory mode easily. If you use streaming mode with proper settings, you can process billions of rows on your desktop PC with 16GB or above memory. 

Both framework and library are written in Golang and are considering interface with Python, R and Node.js. Currently, the development and testing environment is using Windows 11 and AMD x86, and will support Linux. Apart from AMD x86, it will also support ARM CPU. For fast-growing RISC V in IoT applications, which is one of my considerations.

## File Format
Currently, Peaks DataFrame supports tables in CSV file format only. Other table formats such as GZIP(CSV), JSON, HTML, XLSX, Parquet, Lance, HDF5, ORC, Feather and etc are under consideration.

## From WebNameSQL to Peaks DataFrame

Peaks framework is derived by a .net project WebNameSQL. You can see the full specification of "WebNameSQL.pdf" from the repository. Peaks framework will have an improvement version based on WebNameSQL. Any software can implement this framework to standardise ETL expression similar to HTML5, which benefits for end-users. The author have over 10 years of experience in designing ETL expression covers 4 different designs. WebNameSQL is the best design, so Peaks framework will adopt this design with some of improvement, particularly to adapt Python code.

WebNameSQL is a C# in-memory databending software that supports accountants using a web browser to interactive with accounting rules and tables for databending. However, this project became obsolete and it is replaced by a new project “Peaks DataFrame” to solve issues arising from real-time processing and big data. During a continuing effort in academic research, it is implemented new algorithms by using Golang which resulted in a performance gain of around 5X ~ 10X.

WebNameSQL-Go Version is a prototype that uses Golang to rewrite some of the functions using similar algorithms and data structures as WebNameSQL-Csharp Version. The author aims to prove that Golang is more suitable for the next programming language.

Commands to be re-implemented in the Peaks DataFrame will not be the same as WebNameSQL. Considering there are too many commands for your learning and practice, further consolidation and improvement is necessary. The use cases are no longer restricted to accounting; for example, some use cases will cover bioinformatics. Very high performance is essential for this project, so algorithms and data structure of Peaks will be a significant different from WebNameSQL.

## Polars and Peaks Benchmarking

While Polars is one of the fastest dataframes that can be easily installed and run on desktop PCs, Peaks is not intended to be another Polars or Pandas. Instead, it will become a databending software that supports management accounting as well as some special topics of machine learning and bioinformatics.

90 performance tests for both Polars and Peaks were completed using 8-Cores/32GB RAM, with data rows ranging from 10,000 to 1 billion. The time measures cover the starting read csv files to the completion of write csv file. PeaksBenchmark.xlsx uploaded in this repository has documented detail benchmarking results. 

####  Performance Test for Tables with 14 Columns and Varying Numbers of Rows (10,000 ~ 1 Billion)

Usually both very small and very large tables will be disadvantageous in this measure for many software. If the time measures are represented by nominal time, it is meaningless for comparison among different table size scenarios. Duration of billion rows will be very large, 10,000 rows will be very small.

Testing Machine: Intel i9 8-Cores CPU, 32G RAM, 500GB NVMe SSD

Processing time covers read and write csv file.
 
 !  It means how many seconds required for each size of table to process from 1 million rows equivalent data size. 
 
 @  Filter and Orderby functions are under development.

|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|Distinct  |         0.01 |    4.020 |    1.580 |             60.7% |
|          |          0.1 |    0.938 |    0.712 |             24.1% |
|          |            1 |    0.213 |    0.187 |             11.9% |
|          |           10 |    0.123 |    0.115 |              7.0% |
|          |          100 |    0.114 |    0.093 |             18.4% |
|          |         1000 |    0.172 |    0.104 |             39.6% |

![alt text](https://github.com/hkpeaks/peaks-framework/tree/main/Polars%26PeaksBenchmarking/Chart/Distinct.png)



|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|GroupBy   |         0.01 |    3.420 |    1.700 |             50.3% |
|          |          0.1 |    1.034 |    1.526 |            -32.2% |
|          |            1 |    0.272 |    0.347 |            -21.8% |
|          |           10 |    0.154 |    0.227 |            -32.2% |
|          |          100 |    0.129 |    0.228 |            -43.4% |
|          |         1000 |    0.191 |    0.239 |            -20.1% |




|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|JoinTable |         0.01 |    3.140 |    6.140 |            -48.9% |
|          |          0.1 |    0.612 |    1.684 |            -63.7% |
|          |            1 |    0.397 |    0.271 |             31.8% |
|          |           10 |    0.388 |    0.159 |             59.1% |
|          |          100 |    0.886 |    0.178 |             79.9% |
|          |         1000 |     Fail |    0.302 |               N/A |



|          | Million Rows |  Polars  |  Peaks   | Faster / -Slower  |
|----------|------------- |----------|--------- | ----------------- |
|          |              |     !    |    !     |                   |
|Filter @  |         0.01 |          |          |                   |
|          |          0.1 |          |          |                   |
|          |            1 |          |          |                   |
|          |           10 |          |          |                   |
|          |          100 |          |          |                   |
|          |         1000 |          |          |                   |


## Compare Programming Language

Folder: https://github.com/hkpeaks/peaks-framework/tree/main/CompareProgrammingLanguage

Before deciding to develop the Peaks DataFrame, it is conducted a study to determine which programming language was most suitable for this project. The author had compared CSharp, Golang, and Rust with Pandas, Peaks, and Polars using a benchmark located in the folder ‘CompareProgrammingLanguage’. You can find a readme.pdf file inside the folder that shows a comparison of these languages with Pandas, Peaks, and Polars. This benchmark was prepared on April 20th, 2023.

Testing Machine: Intel i9 8-Cores CPU, 32G RAM, 500GB NVMe SSD

Processing Time (In Second) of Read and Write CSV File

|               | 1 Million Rows |10 Million Rows |
| ------------- | -------------- |--------------- |
| Basic Programming                               |
| C Sharp       |          3.269 |         37.482 |
| Golang        |          2.743 |         27.351 |
| Rust          |          3.154 |         32.132 |
| Advanced Programming                            |
| Pandas        |          4.798 |        52.799 @| 
| Peaks         |          0.177 |          0.917 |
| Polars        |          0.406 |          3.625 |

The data structure implemented for the basic programming in a way that is similar to Parquet file format. It is extensively used key-value pairs, for example, use 1, 2, and 3 to represent unique values for each column. However, this extensive use of CPU and memory resources made Peaks DataFrame avoid using it again.

When it comes to data structures, bytearray is one of the most useful and memory-efficient. As for algorithms, parallel streaming for reading/writing files and querying is very powerful and can handle billions of rows even on a desktop PC with only 16GB RAM.

@ If read only, Pandas read 10 million rows is 8s, if use engine="pyarrow", it is 2.6s

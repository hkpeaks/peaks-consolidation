## Success in Billion-Row Sorting Using My Desktop PC after Pervious Success in JoinTable


It is incredible my desktop PC can support to sort one billion rows (by shop and by date) in less than 9 minutes.

Let’s explain this setting OrderBy{1000-MillionRows.csv | Date(D)=> SplitFileByFolder(Shop)~ SortedByShop&Date.csv}

OrderBy is a sorting command
1000-MillionRows.csv is a data source with size 58.5GB
Date(D) is to sort data in descending order
SplitFileByFolder(Shop) is splitting the data source into many files, each file contain one shop
SortedByShop&Date.csv is the output file with size 58.5GB

I will record a series of demo videos to compare this sorting app with Polars, DuckDB and Pandas.

This is a first query function I use temp file and folder to support data source larger than memory. For other functions such as JoinTable, Filter, Select, Distinct, GroupBy, I do not use temp file to handle data source larger than memory as it is certainly no need using temp files.

Next month I will publish the 2nd pre-release of the app on my Github repo. In the meantime, I will create one or more PowerPoint presentation with recorded demo videos to explain how does this work and will be published on my YouTube channel.

It is appreciate if you would inform me your software can sort billion-row faster than 9 minutes.

Testing Machine: 8-core & 32GB
Programming Language: Golang
Data: 58.5GB 1 Billion Rows x 10 Columns

====================================================

D:\Test>do orderby

OrderBy{1000-MillionRows.csv | Date(D)=> SplitFileByFolder(Shop)~ SortedByShop&Date.csv}

Total Bytes: 62,820,354,096 | Total Batches of Stream: 125

1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 ReadPartition2Sorting: 10 S10 10 S11 10 S12 10 S13 10 S14 10 S15 10 S16 10 S17 10 S18 10 S19 10 S20 10 S21 10 S22 10 S23 10 S24 10 S25 10 S26 10 S27 10 S28 10 S29 10 S30 10 S31 10 S32 10 S33 10 S34 10 S35 10 S36 10 S37 10 S38 10 S39 10 S40 10 S41 10 S42 10 S43 10 S44 10 S45 10 S46 10 S47 10 S48 10 S49 10 S50 10 S51 10 S52 10 S53 10 S54 10 S55 10 S56 10 S57 10 S58 10 S59 10 S60 10 S61 10 S62 10 S63 10 S64 10 S65 10 S66 10 S67 10 S68 10 S69 10 S70 10 S71 10 S72 10 S73 10 S74 10 S75 10 S76 10 S77 10 S78 10 S79 10 S80 10 S81 10 S82 10 S83 10 S84 10 S85 10 S86 10 S87 10 S88 10 S89 10 S90 10 S91 10 S92 10 S93 10 S94 10 S95 10 S96 10 S97 10 S98 10 S99 The empty temp folder SortedByShop&Date-csv was removed

SortedByShop&Date.csv(10 x 1,000,000,000)

Duration: 522.414 seconds

D:\Test>
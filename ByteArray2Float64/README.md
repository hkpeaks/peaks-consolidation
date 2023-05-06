## Convert Byte Array to 64-Bit Floating Point Directly

### Performance Issue
This function is derived from the development of GroupBy function in Peaks DataFrame. The author believes that this function is critical to support calculating periodical analysis account balance with different base and functional currencies for advanced accounting automation. It shall be the strongest function of Peaks DataFrame. However, from the very beginning, Polars has a very strong GroupBy in relatively small datasets e.g. <= 10 million rows and numerical calculation. Peaks is very strong in most of commands for very large datasets e.g >= 1 billion rows but weak in numerical calculation.

### Empowering Customisation
On May 4th 2023, the author made a big discovery on how to solve the root issues of numerical calculation. He come an idea how to design code to address this issues. Now Peaks can convert your byte extracted from file to in-memory float64 number to support calculation directly. Previously it must be converted from bytearray to string and then to float64. Extra benefits include being able to recognize (123.45) as -123.45 and recognize 23apple223.77 as 0 rather than crash or blank results. It can support further customization to suit user requirements e.g. to recognize (123,456.789) as -123456.789 directly.

### Data Cleansing
Other software developers may prefer cleaning data before processing it. The disadvantage is significant when processing large datasets which results in performance issues when dealing with reading large datasets twice.

### Bing Chat
On May 6th 2023, the author has completed testing of the "bytearray to float64" for the Go version. When Peaks will move to cross-platform by gRPC in a cluster computing environment, he try using Bing Chat to convert the Golang code to other programming languages and see how it change in coding using the same code design and logic in order to get experience dealing with many programming languages.

<p align="center">
<img src="(https://github.com/hkpeaks/peaks-framework/blob/main/ByteArray2Float64/20-Languages.png)" width=50% height=50%>
</p>




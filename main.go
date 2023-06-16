package main

import (
	"do/src"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

/*

Peaks Consolidation = Peaks Framework (Open Source) + Peaks Databending (Proprietary)
You can use the publised pre-release https://github.com/hkpeaks/peaks-consolidation/releases to test the software.

Fully function script of the Peaks Framework (Open Source):
- ReadWriteFile.txt
- SplitFile.txt
- ExpandFile.txt
- Please unzip 1MillionRows.zip you can find in the Input folder before you run the above scripts
- 29-May-2023: New function of ReadFile which supports to read many csv files (same data schema) within same folder 
     ReadFile{Drive:\FolerName\*.csv ~ Table}
- 16-Jun-2023: Restructing and refactoring of the whole project

The following functions in query_data.go are not open source.
- BuildKeyValue() => Supports Build Keys for Table B
- JoinKeyValue() => Supports Table A Join Table B By Keys
- Select() => Supports Filter Data by Conditions
- Filter2GroupBy() => Support Combine Filter and GroupBy
- DistinctGroupBy() => Support Distinct or GroupBy

In the same file directory of this main.go, please type "go build", the binary file "do.exe" will be generated. For linux, 
"do" will be generated instead and you may need to type chmod +x do to authorize the use of this runtime in your Linux O/S.

Please unzip 1MillionRows.zip in the Input folder before you run script.

Run script:

do script_file_name or ./do script_file_name 
do "script file name" or ./do "script file name" 

No need to type file extension. For any new script file you create, the script file extension must be ".txt". 
After confirm to run the script, you can see the results in your screen and the output folder.

*/

func main() {

	start := time.Now()

	var bytestream, combine, repalce_rule_byte []byte

	if strings.Contains(os.Args[1], ".txt") {
		bytestream = File2Bytestream(os.Args[1])
	} else {
		bytestream = File2Bytestream(os.Args[1] + ".txt")
	}

	var replace_rule string

	if len(os.Args) == 3 {
		replace_rule = "ReplaceRule{" + os.Args[2] + "}"
	}

	repalce_rule_byte = []byte(replace_rule)
	combine = append(repalce_rule_byte, bytestream...)

	var error_count, rule = peaks.ValidateRule(combine)
	if error_count == 0 {
		peaks.RunTask(rule)
	}

	end := time.Now()
	elapsed := end.Sub(start)
	//fmt.Println("Duration:", elapsed)
	if elapsed.Seconds() <= 1 {
		fmt.Println("Duration:", math.Round((elapsed.Seconds())*1000)/1000, "second")
	} else {
		fmt.Println("Duration:", math.Round((elapsed.Seconds())*1000)/1000, "seconds")
	}
}

func File2Bytestream(filename string) []byte {

	bytestream, err := os.ReadFile(filename)

	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	return bytestream
}

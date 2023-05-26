package main

import (
	"do/peaks"
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

The following functions in data_bending.go are empty and are reserved for Peaks Databending (Proprietary).
- JoinKeyValue() => Support JoinTable
- BuildKeyValue() => Support JoinTable
- CurrentSelect() => Support Filter
- CurrentDistinctGroupByFloat64() => Support Distinct and GroupBy
- CurrentDistinctGroupByInteger() => Support Distinct and GroupBy

Original number of line is 1882  which covers about proprietary sections
For open source purpose, it is reducted to 707 line of code

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

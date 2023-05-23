package main

import (
	"fmt"
	"math"
	"os"	
	"do/peaks"
	"strings"
	"time"	
)

// Peaks Consolidation = Peaks Framework (Open Source) + Peaks Databending (Proprietary)
// Since using "Go Build" fail to build databending.go as a library, so this open source project cannot be built an executable runtime.
// You can use the publised pre-release https://github.com/hkpeaks/peaks-consolidation/releases to test the software.


func main() {

	t := time.Now()	

	if t.Format("2006-01-02") > "2023-08-31" {
		fmt.Println("This software has expired. Please download the latest version from github.com/hkpeaks/peaks-consolidation.")
	} else {

		fmt.Println("Development runtime for testing only")
		fmt.Println("Build Date: 23-05-18 | Expiry Date: 23-08-31")		
		fmt.Println("Report Comment: github.com/hkpeaks/peaks-consolidation")
		fmt.Println("")

		start := time.Now()

		var bytestream, combine, repalce_rule_byte []byte

		if strings.Contains(os.Args[1], ".txt") {
			bytestream = peaks.File2Bytestream(os.Args[1])
		} else {
			bytestream = peaks.File2Bytestream(os.Args[1] + ".txt")
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

}

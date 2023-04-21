package main

import (
	"fmt"
	//"io/ioutil"
	"path/filepath"
	//"log"
	"os"
	"run/webname"
	"strings"
	"time"
)

func VisitFile(fp string, fi os.FileInfo, err error) error {
	if err != nil {
		fmt.Println(err) // can't walk here,
		return nil       // but continue walking elsewhere
	}
	if fi.IsDir() {
		return nil // not a file.  ignore.
	}
	matched, err := filepath.Match("*.mp3", fi.Name())
	if err != nil {
		fmt.Println(err) // malformed pattern
		return err       // this is fatal.
	}
	if matched {
		fmt.Println(fp)
	}
	return nil
}

func main() {

	start := time.Now()

	var bytestream, combine, repalce_rule_byte []byte

	if strings.Contains(os.Args[1], ".txt") {
		bytestream = webname.File2Bytestream(os.Args[1])
	} else {
		bytestream = webname.File2Bytestream(os.Args[1] + ".txt")
	}

	var replace_rule string

	if len(os.Args) == 3 {
		replace_rule = "ReplaceRule{" + os.Args[2] + "}"
	}

	repalce_rule_byte = []byte(replace_rule)
	combine = append(repalce_rule_byte, bytestream...)

	var error_count, rule = webname.ValidateRule(combine)
	if error_count == 0 {
		webname.RunTask(rule)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("Duration:", elapsed)
}

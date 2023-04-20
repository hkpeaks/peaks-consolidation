package main

import (	
	"log"
	"io/ioutil"
)

func file2bytestream(filename string) ([]byte){
	bytestream, err := ioutil.ReadFile(filename)
	
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}

	return bytestream;
}

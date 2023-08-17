package main

import (
	"log"
	"time"
	_ "time/tzdata"
)

func main() {

	_, err := time.LoadLocation("US/blah")
	if err != nil {
		log.Fatalf("error loading location :: %v", err)
	}

}

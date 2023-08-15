package main

import (
	"fmt"
	"log"
	"time"
	_ "time/tzdata"
)

var (
	layout     = "2006-01-02T15:04:05.999999 US/Mountain"
	sampleTime = "2012-02-05T13:12:15.123456 US/Pacific"
	// timeZone   = "US/Eastern"

	// layout     = "2006-01-02T15:04:05Z07:00"
	// sampleTime = "2012-02-05T13:12:15+07:00"
)

func main() {
	t, err := time.Parse(layout, sampleTime)
	if err != nil {
		log.Fatalf("error parsing time :: %v", err)
	}

	// loc, err := time.LoadLocation(timeZone)
	// if err != nil {
	// 	log.Fatalf("error loading location :: %v", err)
	// }

	// t = t.In(loc)

	fmt.Println(t.Zone())
	fmt.Println(t.Format(time.RFC1123Z))
}

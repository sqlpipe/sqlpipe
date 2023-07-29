package main

import (
	"fmt"
	"time"
)

func FormatDate(t time.Time) string {
	return t.Format("2006-01-02")
}

func main() {
	t := time.Now()
	fmt.Println(FormatDate(t))
}

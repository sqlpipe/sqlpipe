package main

import (
	"fmt"
	"os"
)

func myfunc() {
	file, err := os.CreateTemp("", "AAA")
	if err != nil {
		panic("couldn't create a")
	}
	defer fmt.Println(file.Name())

	file.Close()

	file, err = os.CreateTemp("", "BBB")
	if err != nil {
		panic("couldn't create b")
	}
	defer fmt.Println(file.Name())
}

func main() {
	myfunc()
	fmt.Println("Still here!")
	fmt.Println(len("\u0000"))
}

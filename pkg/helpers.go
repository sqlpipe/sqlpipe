package pkg

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Confirm(action string) bool {
	reader := bufio.NewReader(os.Stdin)
	var answer bool

	for {
		fmt.Printf("\nAre you sure you want to %s\n\n**************************************************\n\nRespnd Y or N -> ", action)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare("Y", strings.ToUpper(text)) == 0 {
			answer = true
			break
		} else if strings.Compare("N", strings.ToUpper(text)) == 0 {
			answer = false
			break
		} else {
			fmt.Println("Respond Y or N")
		}
	}

	return answer
}

type TableAndSchema struct {
	HasSchema  bool
	TableName  string
	SchemaName string
}

func GetTableAndSchema(
	table string,
) (
	tableAndSchema TableAndSchema,
) {
	split := strings.Split(table, ".")
	switch len(split) {
	case 1:
		tableAndSchema.TableName = table
		tableAndSchema.HasSchema = false
	case 2:
		tableAndSchema.SchemaName = split[0]
		tableAndSchema.TableName = split[1]
		tableAndSchema.HasSchema = true
	}
	return tableAndSchema
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func Background(fn func()) {
	go func() {
		fn()
	}()
}

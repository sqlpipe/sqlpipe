package xsql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/shomali11/util/xstrings"
	"strings"
)

const (
	empty               = ""
	space               = " "
	plus                = "+"
	minus               = "-"
	pipe                = "|"
	newLine             = "\n"
	intString           = "int"
	floatString         = "float"
	leftJustification   = "LEFT"
	rightJustification  = "RIGHT"
	sqlRowFooter        = "(1 row)"
	sqlRowsFooterFormat = "(%d rows)"
)

// Pretty returns a pretty sql string
func Pretty(rows *sql.Rows) (string, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return empty, err
	}

	if len(columnNames) == 0 {
		return getFooter(0), nil
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return empty, err
	}

	values, err := getValues(rows)
	if err != nil {
		return empty, err
	}

	columnSizes := getColumnSizes(columnNames, values)
	columnJustifications := getColumnJustifications(columnTypes)
	header := getHeader(columnTypes, columnSizes)
	body := getBody(values, columnSizes, columnJustifications)
	footer := getFooter(len(values))

	var results bytes.Buffer
	results.WriteString(header)
	results.WriteString(body)
	results.WriteString(footer)
	return results.String(), nil
}

func getValues(rows *sql.Rows) ([][]string, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := [][]string{}

	dataBytes := make([][]byte, len(columnTypes))
	pointers := make([]interface{}, len(columnTypes))
	for i := range dataBytes {
		pointers[i] = &dataBytes[i]
	}

	for rows.Next() {
		err = rows.Scan(pointers...)
		if err != nil {
			return nil, err
		}

		rowValues := make([]string, len(columnTypes))

		for i, data := range dataBytes {
			if data == nil {
				rowValues[i] = empty
			} else {
				rowValues[i] = string(data)
			}
		}

		values = append(values, rowValues)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return values, nil
}

func getColumnSizes(columnNames []string, values [][]string) []int {
	columnMaxSizes := make([]int, len(columnNames))
	for i, columnName := range columnNames {
		columnMaxSizes[i] = len(columnName)
	}

	for rowIndex := 0; rowIndex < len(values); rowIndex++ {
		for columnIndex := 0; columnIndex < len(values[rowIndex]); columnIndex++ {
			if columnMaxSizes[columnIndex] < len(values[rowIndex][columnIndex]) {
				columnMaxSizes[columnIndex] = len(values[rowIndex][columnIndex])
			}
		}
	}
	return columnMaxSizes
}

func getColumnJustifications(columnTypes []*sql.ColumnType) []string {
	columnJustifications := make([]string, len(columnTypes))
	for i, columnType := range columnTypes {
		kindString := columnType.ScanType().Kind().String()
		if strings.Contains(kindString, intString) || strings.Contains(kindString, floatString) {
			columnJustifications[i] = rightJustification
		} else {
			columnJustifications[i] = leftJustification
		}
	}
	return columnJustifications
}

func getHeader(columnTypes []*sql.ColumnType, columnSizes []int) string {
	var header bytes.Buffer
	header.WriteString(space)
	header.WriteString(xstrings.Center(columnTypes[0].Name(), columnSizes[0]))
	header.WriteString(space)

	for i := 1; i < len(columnTypes); i++ {
		header.WriteString(pipe)
		header.WriteString(space)
		header.WriteString(xstrings.Center(columnTypes[i].Name(), columnSizes[i]))
		header.WriteString(space)
	}

	header.WriteString(newLine)
	for i := 0; i < 2+columnSizes[0]; i++ {
		header.WriteString(minus)
	}
	for i := 1; i < len(columnTypes); i++ {
		header.WriteString(plus)
		for j := 0; j < 2+columnSizes[i]; j++ {
			header.WriteString(minus)
		}
	}
	header.WriteString(newLine)
	return header.String()
}

func getBody(values [][]string, columnSizes []int, columnJustifications []string) string {
	var body bytes.Buffer
	for rowIndex := 0; rowIndex < len(values); rowIndex++ {
		body.WriteString(space)
		switch columnJustifications[0] {
		case leftJustification:
			body.WriteString(xstrings.Left(values[rowIndex][0], columnSizes[0]))
		case rightJustification:
			body.WriteString(xstrings.Right(values[rowIndex][0], columnSizes[0]))
		default:
			body.WriteString(xstrings.Center(values[rowIndex][0], columnSizes[0]))
		}
		body.WriteString(space)

		for columnIndex := 1; columnIndex < len(values[rowIndex]); columnIndex++ {
			body.WriteString(pipe)
			body.WriteString(space)
			switch columnJustifications[columnIndex] {
			case leftJustification:
				body.WriteString(xstrings.Left(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			case rightJustification:
				body.WriteString(xstrings.Right(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			default:
				body.WriteString(xstrings.Center(values[rowIndex][columnIndex], columnSizes[columnIndex]))
			}
			body.WriteString(space)
		}
		body.WriteString(newLine)
	}
	return body.String()
}

func getFooter(rowCount int) string {
	if rowCount == 1 {
		return sqlRowFooter
	}
	return fmt.Sprintf(sqlRowsFooterFormat, rowCount)
}

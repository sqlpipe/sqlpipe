package engine

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/writers"
)

func RunTransfer(ctx context.Context, transfer data.Transfer) (map[string]any, int, error) {
	rows, err := transfer.Source.Db.QueryContext(ctx, transfer.Query)
	if err != nil {
		return map[string]any{"": ""}, http.StatusBadRequest, err
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return map[string]any{"": ""}, http.StatusBadRequest, err
	}

	numCols := len(columnNames)

	vals := make([]interface{}, numCols)
	valPtrs := make([]interface{}, numCols)

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}
	colDbTypes := []string{}
	for _, colType := range colTypes {
		colDbTypes = append(colDbTypes, colType.DatabaseTypeName())
	}

	var valWriters map[string]func(value interface{}, terminator string) (string, error)

	switch transfer.Target.Writers {
	case "csv":
		valWriters = writers.CsvValWriters
	default:
		valWriters = writers.GeneralValWriters
		// createWriters := writers.GeneralCreateWriters
	}

	var fileBuilder strings.Builder

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	schemaSpecifier := ""
	switch transfer.Target.Schema {
	case "":
	default:
		schemaSpecifier = fmt.Sprintf("%v.", transfer.Target.Schema)
	}
	columnNamesString := strings.Join(columnNames, ",")

	isFirst := true

	for i := 1; rows.Next(); i++ {
		rows.Scan(valPtrs...)
		switch isFirst {
		case true:
			fileBuilder.WriteString(fmt.Sprintf("insert into %v%v (%v) values (", schemaSpecifier, transfer.Target.Table, columnNamesString))
			isFirst = false
		default:
			fileBuilder.WriteString(",(")
		}
		for j := 0; j < numCols-1; j++ {
			valToWrite, err := valWriters[colDbTypes[j]](vals[j], ",")
			if err != nil {
				return map[string]any{"": ""}, http.StatusInternalServerError, err
			}
			fileBuilder.WriteString(valToWrite)
		}
		valToWrite, err := valWriters[colDbTypes[numCols-1]](vals[numCols-1], ")")
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
		fileBuilder.WriteString(valToWrite)
		if i%transfer.Target.RowsPerInsertQuery == 0 {
			fmt.Println(fileBuilder.String())
			fileBuilder.Reset()
			isFirst = true
		}
	}

	return map[string]any{"message": "success"}, http.StatusOK, nil
}

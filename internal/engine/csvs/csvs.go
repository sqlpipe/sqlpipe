package csvs

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunCsvSaveOnServer(ctx context.Context, export data.Export) (map[string]any, int, error) {
	rows, err := export.Source.Db.QueryContext(ctx, export.Query)
	if err != nil {
		return map[string]any{"": ""}, http.StatusBadRequest, err
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return map[string]any{"": ""}, http.StatusBadRequest, err
	}

	numCols := len(columnNames)

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}
	colDbTypes := []string{}
	for _, colType := range colTypes {
		colDbTypes = append(colDbTypes, colType.DatabaseTypeName())
	}

	var f *os.File

	_, err = os.Stat(export.CsvTarget.CsvWriteLocation)
	if err == nil {
		err = os.Remove(export.CsvTarget.CsvWriteLocation)
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
	}

	f, err = os.OpenFile(export.CsvTarget.CsvWriteLocation, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}

	var batchBuilder strings.Builder

	for i := 0; i < numCols-1; i++ {
		fmt.Fprintf(&batchBuilder, `"%v",`, columnNames[i])
	}
	fmt.Fprintf(&batchBuilder, `"%v"`, columnNames[numCols-1])
	fmt.Fprintf(&batchBuilder, "\n")
	if _, err = f.WriteString(batchBuilder.String()); err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}
	batchBuilder.Reset()

	vals := make([]interface{}, numCols)
	valPtrs := make([]interface{}, numCols)

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	for i := 1; rows.Next(); i++ {
		rows.Scan(valPtrs...)

		for j := 0; j < numCols-1; j++ {
			valToWrite, err := formatters[colDbTypes[j]](vals[j], ",")
			if err != nil {
				return map[string]any{"": ""}, http.StatusInternalServerError, err
			}
			fmt.Fprint(&batchBuilder, valToWrite)
		}
		valToWrite, err := formatters[colDbTypes[numCols-1]](vals[numCols-1], "\n")
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
		fmt.Fprint(&batchBuilder, valToWrite)
	}

	if _, err = f.WriteString(batchBuilder.String()); err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}

	return map[string]any{"message": "success"}, http.StatusOK, nil
}

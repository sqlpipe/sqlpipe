package csvs

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func CreateCsvFile(
	ctx context.Context,
	csvExport data.CsvExport,
) (
	file *os.File,
	err error,
) {
	rows, err := csvExport.Source.Db.QueryContext(ctx, csvExport.Query)
	if err != nil {
		return nil, fmt.Errorf("error running query on source: %v", err.Error())
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error getting column types: %v", err.Error())
	}
	colDbTypes := []string{}
	for _, colType := range colTypes {
		colDbTypes = append(colDbTypes, colType.DatabaseTypeName())
	}

	if csvExport.WriteLocation == "" {
		file, err = os.CreateTemp("", "")
		if err != nil {
			return nil, fmt.Errorf("error creating temp file: %v", err.Error())
		}
	} else {
		_, err = os.Stat(csvExport.WriteLocation)
		if err == nil {
			err = os.Remove(csvExport.WriteLocation)
			if err != nil {
				return nil, fmt.Errorf("error removing file at write_location: %v", err.Error())
			}
		}

		file, err = os.OpenFile(csvExport.WriteLocation, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("error creating file at write_location: %v", err.Error())
		}
	}

	csvWriter := csv.NewWriter(file)

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names: %v", err.Error())
	}

	err = csvWriter.Write(columnNames)
	if err != nil {
		return nil, fmt.Errorf("error writing header to csv file: %v", err.Error())
	}

	numCols := len(columnNames)
	vals := make([]interface{}, numCols)
	valPtrs := make([]interface{}, numCols)

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	rowVals := make([]string, numCols)
	for i := 1; rows.Next(); i++ {
		rows.Scan(valPtrs...)
		for j := 0; j < numCols; j++ {
			rowVals[j], err = formatters[colDbTypes[j]](vals[j])
			if err != nil {
				return nil, fmt.Errorf("error formatting values for csv file: %v", err.Error())
			}
		}
		err = csvWriter.Write(rowVals)
		if err != nil {
			return nil, fmt.Errorf("error writing values to csv file: %v", err.Error())
		}
	}

	csvWriter.Flush()

	return file, nil
}

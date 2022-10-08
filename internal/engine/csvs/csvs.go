package csvs

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func WriteCsvToFile(
	ctx context.Context,
	csvExport data.CsvSave,
	file *os.File,
) (
	err error,
) {
	rows, err := csvExport.Source.Db.QueryContext(ctx, csvExport.Query)
	if err != nil {
		return fmt.Errorf("error running query on source: %v", err.Error())
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error getting column types: %v", err.Error())
	}
	colDbTypes := []string{}
	for _, colType := range colTypes {
		colDbTypes = append(colDbTypes, colType.DatabaseTypeName())
	}

	csvWriter := csv.NewWriter(file)

	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting column names: %v", err.Error())
	}

	err = csvWriter.Write(columnNames)
	if err != nil {
		return fmt.Errorf("error writing header to csv file: %v", err.Error())
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
				return fmt.Errorf("error formatting values for csv file: %v", err.Error())
			}
		}
		err = csvWriter.Write(rowVals)
		if err != nil {
			return fmt.Errorf("error writing values to csv file: %v", err.Error())
		}
	}

	csvWriter.Flush()
	if csvWriter.Error() != nil {
		return fmt.Errorf("error flushing csv file: %v", err.Error())
	}

	return nil
}

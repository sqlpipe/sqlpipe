package engine

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/systems"
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

	var systemValFormatters map[string]func(value interface{}, terminator string, nullString string) (string, error)
	var batchEnder string

	schemaSpecifier := ""
	switch transfer.Target.Schema {
	case "":
	default:
		schemaSpecifier = fmt.Sprintf("%v.", transfer.Target.Schema)
	}

	systemValFormatters = systems.ValFormatters[transfer.Target.SystemType]
	batchEnder = ")"
	createFormatters := systems.CreateFormatters[transfer.Target.SystemType]
	createQuery := fmt.Sprintf("create table %v%v(", schemaSpecifier, transfer.Target.Table)

	for i := 0; i < numCols-1; i++ {
		columnSpecifier, err := createFormatters[colDbTypes[i]](colTypes[i], ",")
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
		createQuery = createQuery + columnSpecifier
	}
	columnSpecifier, err := createFormatters[colDbTypes[numCols-1]](colTypes[numCols-1], ")")
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}
	createQuery = createQuery + columnSpecifier

	dropTableCommand := fmt.Sprintf(
		"%v %v%v",
		systems.DropTableCommandStarters[transfer.Target.SystemType],
		transfer.Target.Table,
		schemaSpecifier,
	)

	_, err = transfer.Target.Db.ExecContext(ctx, dropTableCommand)
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}

	_, err = transfer.Target.Db.ExecContext(ctx, createQuery)
	if err != nil {
		return map[string]any{"": ""}, http.StatusInternalServerError, err
	}

	var batchBuilder strings.Builder

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	columnNamesString := strings.Join(columnNames, ",")

	isFirstRow := true
	dataRemaining := false
	nullString := systems.NullStrings[transfer.Target.SystemType]

	for i := 1; rows.Next(); i++ {
		dataRemaining = true
		rows.Scan(valPtrs...)

		if isFirstRow {
			batchBuilder.WriteString(fmt.Sprintf("insert into %v%v (%v) values (", schemaSpecifier, transfer.Target.Table, columnNamesString))
		} else {
			batchBuilder.WriteString(",(")
		}
		isFirstRow = false
		for j := 0; j < numCols-1; j++ {
			valToWrite, err := systemValFormatters[colDbTypes[j]](vals[j], ",", nullString)
			if err != nil {
				return map[string]any{"": ""}, http.StatusInternalServerError, err
			}
			batchBuilder.WriteString(valToWrite)
		}
		valToWrite, err := systemValFormatters[colDbTypes[numCols-1]](vals[numCols-1], batchEnder, nullString)
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
		batchBuilder.WriteString(valToWrite)
		if i%transfer.Target.RowsPerWrite == 0 {
			stringToWrite := batchBuilder.String()
			_, err := transfer.Target.Db.ExecContext(ctx, stringToWrite)
			if err != nil {
				return map[string]any{"": ""}, http.StatusInternalServerError, err
			}

			batchBuilder.Reset()
			isFirstRow = true
			dataRemaining = false
		}
	}

	if dataRemaining {
		stringToWrite := batchBuilder.String()

		_, err := transfer.Target.Db.ExecContext(ctx, stringToWrite)
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}

		batchBuilder.Reset()
	}

	return map[string]any{"message": "success"}, http.StatusOK, nil
}

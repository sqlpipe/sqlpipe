package transfers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/transfers/formatters"
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

	var midStringValFormatters map[string]func(value interface{}) (string, error)
	var endStringValFormatters map[string]func(value interface{}) (string, error)

	schemaSpecifier := ""
	switch transfer.Target.Schema {
	case "":
	default:
		schemaSpecifier = fmt.Sprintf("%v.", transfer.Target.Schema)
	}

	midStringValFormatters = systemMidStringValFormatters[transfer.Target.SystemType]
	endStringValFormatters = systemEndStringValFormatters[transfer.Target.SystemType]
	createFormatters := createFormatters[transfer.Target.SystemType]
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

	if transfer.DropTargetTable {
		dropTableCommand := fmt.Sprintf(
			"drop table %v%v",
			transfer.Target.Table,
			schemaSpecifier,
		)

		_, err = transfer.Target.Db.ExecContext(ctx, dropTableCommand)
		if err != nil {
			return map[string]any{"": ""}, http.StatusInternalServerError, err
		}
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
			valToWrite, err := midStringValFormatters[colDbTypes[j]](vals[j])
			if err != nil {
				return map[string]any{"": ""}, http.StatusInternalServerError, err
			}
			batchBuilder.WriteString(valToWrite)
		}
		valToWrite, err := endStringValFormatters[colDbTypes[numCols-1]](vals[numCols-1])
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

var createFormatters = map[string]map[string]func(column *sql.ColumnType, terminator string) (string, error){
	"postgresql": formatters.PostgresqlCreateFormatters,
}

var systemMidStringValFormatters = map[string]map[string]func(value interface{}) (string, error){
	"postgresql": formatters.PostgresqlMidStringValFormatters,
}

var systemEndStringValFormatters = map[string]map[string]func(value interface{}) (string, error){
	"postgresql": formatters.PostgresqlEndStringValFormatters,
}

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

	schemaSpecifier := ""
	switch transfer.Target.Schema {
	case "":
	default:
		schemaSpecifier = fmt.Sprintf("%v.", transfer.Target.Schema)
	}

	var midStringValFormatters map[string]func(value interface{}) (string, error)
	var endStringValFormatters map[string]func(value interface{}) (string, error)
	var createFormatters map[string]func(column *sql.ColumnType, terminator string) (string, error)
	var ok bool

	if midStringValFormatters, ok = systemMidStringValFormatters[transfer.Target.SystemType]; !ok {
		return map[string]any{"": ""}, http.StatusBadRequest, fmt.Errorf("unsupported target system type: %v", transfer.Target.SystemType)
	}

	if endStringValFormatters, ok = systemEndStringValFormatters[transfer.Target.SystemType]; !ok {
		return map[string]any{"": ""}, http.StatusBadRequest, fmt.Errorf("unsupported target system type: %v", transfer.Target.SystemType)
	}

	if createFormatters, ok = systemCreateFormatters[transfer.Target.SystemType]; !ok {
		return map[string]any{"": ""}, http.StatusBadRequest, fmt.Errorf("unsupported target system type: %v", transfer.Target.SystemType)
	}

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
			"%v %v%v",
			dropTableCommandStarters[transfer.Target.SystemType],
			schemaSpecifier,
			transfer.Target.Table,
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
	rowsPerWrite := defaultRowsPerWrite[transfer.Target.SystemType]

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
		if i%rowsPerWrite == 0 {
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

var (
	systemCreateFormatters = map[string]map[string]func(column *sql.ColumnType, terminator string) (string, error){
		"postgresql": formatters.PostgresqlCreateFormatters,
		"mssql":      formatters.MssqlCreateFormatters,
	}
	systemMidStringValFormatters = map[string]map[string]func(value interface{}) (string, error){
		"postgresql": formatters.PostgresqlMidStringValFormatters,
		"mssql":      formatters.MssqlMidStringValFormatters,
	}
	systemEndStringValFormatters = map[string]map[string]func(value interface{}) (string, error){
		"postgresql": formatters.PostgresqlEndStringValFormatters,
		"mssql":      formatters.MssqlEndStringValFormatters,
	}
	defaultRowsPerWrite = map[string]int{
		"postgresql": 1000,
		"mssql":      1000,
	}
	dropTableCommandStarters = map[string]string{
		"postgresql": "drop table if exists",
		"mssql":      "drop table if exists",
	}
)

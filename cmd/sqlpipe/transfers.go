package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.DataSystem `json:"source"`
		Target data.DataSystem `json:"target"`
		Query  string          `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	transfer := &data.Transfer{
		Source: input.Source,
		Target: input.Target,
		Query:  input.Query,
	}

	v := validator.New()

	if data.ValidateTransfer(v, transfer); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	sourceDsn := fmt.Sprintf(
		"Driver={%v};Server=%v;Port=%v;Database=%v;Uid=%v;Pwd=%v;",
		transfer.Source.DriverName,
		transfer.Source.Host,
		transfer.Source.Port,
		transfer.Source.DbName,
		transfer.Source.Username,
		transfer.Source.Password,
	)

	sourceDb, err := sql.Open(
		"odbc",
		sourceDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err.Error())
		return
	}

	transfer.Source.Db = *sourceDb
	err = transfer.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	targetDsn := fmt.Sprintf(
		"Driver={%v};Server=%v;Port=%v;Database=%v;Uid=%v;Pwd=%v;",
		transfer.Target.DriverName,
		transfer.Target.Host,
		transfer.Target.Port,
		transfer.Target.DbName,
		transfer.Target.Username,
		transfer.Target.Password,
	)

	targetDb, err := sql.Open(
		"odbc",
		targetDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err.Error())
		return
	}

	transfer.Target.Db = *targetDb
	err = transfer.Target.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	rows, err := transfer.Source.Db.QueryContext(r.Context(), transfer.Query)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	columnNames, err := rows.Columns()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	numCols := len(columnNames)

	vals := make([]interface{}, numCols)
	valPtrs := make([]interface{}, numCols)

	var fileBuilder strings.Builder

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	colTypes, err := rows.ColumnTypes()

	for _, colType := range colTypes {
		fmt.Println(colType.DatabaseTypeName())
	}

	for i := 0; rows.Next(); i++ {
		rows.Scan(valPtrs...)
		for j := 0; j < numCols; j++ {
			switch v := vals[j].(type) {
			case []uint8:
				fileBuilder.Write(v)
			case nil:
				fileBuilder.WriteString("")
			default:
				fileBuilder.WriteString(fmt.Sprint(v))
			}
		}
	}

	headers := make(http.Header)

	// fmt.Println(fileBuilder.String())

	err = app.writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}

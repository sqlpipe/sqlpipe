package main

import (
	"database/sql"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) runTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.Source `json:"source"`
		Target data.Target `json:"target"`
		Query  string      `json:"query"`
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

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		transfer.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transfer.Source.Db = sourceDb
	err = transfer.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	var targetDb *sql.DB
	targetDb, err = sql.Open(
		"odbc",
		transfer.Target.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}
	transfer.Target.Db = *targetDb
	err = transfer.Target.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	result, statusCode, err := engine.RunTransfer(r.Context(), *transfer)
	if err != nil {
		app.errorResponse(w, r, statusCode, err)
		return
	}

	headers := make(http.Header)

	err = app.writeJSON(w, http.StatusOK, result, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

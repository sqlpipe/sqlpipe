package main

import (
	"database/sql"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) runQueryHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.Source `json:"source"`
		Query  string      `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	query := &data.Query{
		Source: input.Source,
		Query:  input.Query,
	}

	v := validator.New()

	if data.ValidateQuery(v, query); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	sourceDb, err := sql.Open(
		"odbc",
		query.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	query.Source.Db = sourceDb
	err = query.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	result, statusCode, err := engine.RunQuery(r.Context(), *query)
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

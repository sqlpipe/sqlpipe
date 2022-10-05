package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/shomali11/xsql"
	"github.com/sqlpipe/sqlpipe/internal/data"
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

	message := ""

	rows, err := query.Source.Db.QueryContext(r.Context(), query.Query)
	if err != nil {
		switch {
		case err.Error() == "Stmt did not create a result set":
			message = "Query ran successfully but did not product a result set."
		default:
			app.errorResponse(w, r, http.StatusBadRequest, err)
			return
		}
	}
	// defer rows.Close()

	if message == "" {
		message, err = xsql.Pretty(rows)
		if err != nil {
			app.errorResponse(w, r, http.StatusInternalServerError, err)
			return
		}
	}

	fmt.Println(message)

	err = app.writePlaintext(w, http.StatusOK, message, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
	fmt.Println("HERE")
}

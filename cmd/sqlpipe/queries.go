package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) createQueryHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Target data.DataSystem `json:"target"`
		Query  string          `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	query := &data.Query{
		Target: input.Target,
		Query:  input.Query,
	}

	v := validator.New()

	if data.ValidateQuery(v, query); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	dsn := fmt.Sprintf(
		"Driver={%v};Server=%v;Port=%v;Database=%v;Uid=%v;Pwd=%v;",
		query.Target.DriverName,
		query.Target.Host,
		query.Target.Port,
		query.Target.DbName,
		query.Target.Username,
		query.Target.Password,
	)

	db, err := sql.Open(
		"odbc",
		dsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	if err = db.Ping(); err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	headers := make(http.Header)

	err = app.writeJSON(w, http.StatusCreated, envelope{"query": query}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}

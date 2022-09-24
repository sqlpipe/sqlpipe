package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine"
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

	targetDb, err := sql.Open(
		"odbc",
		dsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err.Error())
		return
	}

	query.Target.Db = *targetDb
	err = query.Target.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err.Error())
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
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}

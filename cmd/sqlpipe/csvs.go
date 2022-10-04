package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/csvs"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) runCsvSaveOnServerHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source        data.Source `json:"source"`
		WriteLocation string      `json:"write_location"`
		Query         string      `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	export := &data.CsvExport{
		Source:        input.Source,
		WriteLocation: input.WriteLocation,
		Query:         input.Query,
	}

	v := validator.New()
	if data.ValidateCsvExport(v, export); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		export.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	export.Source.Db = sourceDb
	err = export.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	file, err := csvs.CreateCsvFile(r.Context(), *export)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, map[string]any{"message": fmt.Sprintf("csv file written to %v", file.Name())}, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func (app *application) runCsvDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source data.Source `json:"source"`
		Query  string      `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	export := &data.CsvExport{
		Source: input.Source,
		Query:  input.Query,
	}

	v := validator.New()
	if data.ValidateCsvExport(v, export); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	var sourceDb *sql.DB
	sourceDb, err = sql.Open(
		"odbc",
		export.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	export.Source.Db = sourceDb
	err = export.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	file, err := csvs.CreateCsvFile(r.Context(), *export)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.writeCSV(w, http.StatusOK, file, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

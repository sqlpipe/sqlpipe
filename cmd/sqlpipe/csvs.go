package main

import (
	"database/sql"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/csvs"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) runCsvExportHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source    data.Source    `json:"source"`
		CsvTarget data.CsvTarget `json:"csv_target"`
		Query     string         `json:"query"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	export := &data.Export{
		Source:    input.Source,
		CsvTarget: input.CsvTarget,
		Query:     input.Query,
	}

	v := validator.New()

	if data.ValidateExport(v, export); !v.Valid() {
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

	result, statusCode, err := csvs.RunCsvExport(r.Context(), *export)
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

func (app *application) runCsvDownloadHandler(w http.ResponseWriter, r *http.Request) {
	// todo

	headers := make(http.Header)

	err := app.writeJSON(w, http.StatusOK, map[string]any{}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func (app *application) runS3ExportHandler(w http.ResponseWriter, r *http.Request) {
	// todo

	headers := make(http.Header)

	err := app.writeJSON(w, http.StatusOK, map[string]any{}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

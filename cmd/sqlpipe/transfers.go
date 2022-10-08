package main

import (
	"database/sql"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/transfers"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

func (app *application) runTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source          data.Source `json:"source"`
		Target          data.Target `json:"target"`
		Query           string      `json:"query"`
		DropTargetTable bool        `json:"drop_target_table"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	transfer := &data.Transfer{
		Source:          input.Source,
		Target:          input.Target,
		Query:           input.Query,
		DropTargetTable: input.DropTargetTable,
	}

	v := validator.New()
	if data.ValidateTransfer(v, transfer); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	transfer.Source.Db, err = sql.Open(
		"odbc",
		transfer.Source.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = transfer.Source.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	transfer.Target.Db, err = sql.Open(
		"odbc",
		transfer.Target.OdbcDsn,
	)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = transfer.Target.Db.Ping()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = transfers.RunTransfer(r.Context(), *transfer)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, err)
		return
	}

	err = app.respondWithJSON(w, http.StatusOK, map[string]any{"message": "success"}, make(http.Header))
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

package main

import (
	"net/http"

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

	headers := make(http.Header)

	err = app.writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}

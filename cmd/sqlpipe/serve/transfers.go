package serve

import (
	"errors"
	"net/http"
	"reflect"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type listTransfersInput struct {
	data.Filters
}

func (app *application) getListTransfersInput(r *http.Request) (input listTransfersInput, err map[string]string) {
	v := validator.New()

	qs := r.URL.Query()

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 20, v)

	input.Filters.Sort = app.readString(qs, "sort", "id")
	input.Filters.SortSafelist = []string{"id", "created_at", "-id", "-created_at"}

	data.ValidateFilters(v, input.Filters)

	return input, v.Errors
}

func (app *application) listTransfersApiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListTransfersInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
	}

	transfers, metadata, err := app.models.Transfers.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"transfers": transfers, "metadata": metadata}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) createTransferApiHandler(w http.ResponseWriter, r *http.Request) {

	var input struct {
		SourceID     int64  `json:"sourceID"`
		TargetID     int64  `json:"targetID"`
		Query        string `json:"query"`
		TargetSchema string `json:"targetSchema"`
		TargetTable  string `json:"targetTable"`
		Overwrite    bool   `json:"overwrite"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	transfer := &data.Transfer{
		SourceID:     input.SourceID,
		TargetID:     input.TargetID,
		Query:        input.Query,
		TargetSchema: input.TargetSchema,
		TargetTable:  input.TargetTable,
		Overwrite:    input.Overwrite,
	}

	v := validator.New()

	if data.ValidateTransfer(v, transfer); !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	transfer, err = app.models.Transfers.Insert(transfer)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	err = app.writeJSON(w, http.StatusAccepted, envelope{"transfer": transfer}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) showTransferApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	transfer, err := app.models.Transfers.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

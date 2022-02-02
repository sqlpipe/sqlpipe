package serve

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/forms.go"
	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type listTransfersInput struct {
	CreatedAtStart time.Time
	CreatedAtEnd   time.Time
	CreatedBy      int64
	Status         string
	Source         int64
	Target         int64
	data.Filters
}

func (app *application) getListTransfersInput(r *http.Request) (input listTransfersInput, err map[string]string) {
	v := validator.New()

	qs := r.URL.Query()

	input.CreatedAtStart = app.readDateTime(qs, "created_at_start", time.Now().AddDate(0, 0, -7))
	input.CreatedAtEnd = app.readDateTime(qs, "created_at_end", time.Now())

	input.CreatedBy = int64(app.readInt(qs, "created_by", 0, v))

	input.Status = app.readString(qs, "status", "")

	input.Source = int64(app.readInt(qs, "source_id", 0, v))
	input.Target = int64(app.readInt(qs, "target_id", 0, v))

	input.Filters.Page = app.readInt(qs, "page", 1, v)
	input.Filters.PageSize = app.readInt(qs, "page_size", 100, v)

	input.Filters.Sort = app.readString(qs, "sort", "created_at")
	input.Filters.SortSafelist = []string{"created_at", "-created_at"}

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
		Overwrite    *bool  `json:"overwrite"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	overwrite := false
	if input.Overwrite != nil {
		overwrite = *input.Overwrite
	}

	transfer := &data.Transfer{
		CreatedBy:    app.getAuthenticatedUserId(r),
		SourceID:     input.SourceID,
		TargetID:     input.TargetID,
		Query:        input.Query,
		TargetSchema: input.TargetSchema,
		TargetTable:  input.TargetTable,
		Overwrite:    overwrite,
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

func (app *application) cancelTransferApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	v := validator.New()
	transfer, err := app.models.Transfers.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			v.AddError("id", "not found")
			app.failedValidationResponse(w, r, v.Errors)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	if transfer.Status != "queued" && transfer.Status != "active" {
		v.AddError("status", fmt.Sprintf("cannot cancel a transfer with status of %s", transfer.Status))
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	transfer.Status = "cancelled"
	transfer.StoppedAt = time.Now()

	app.models.Transfers.Update(transfer)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
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

func (app *application) deleteTransferApiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Transfers.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	err = app.writeJSON(w, http.StatusOK, envelope{"message": "transfer successfully deleted"}, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func (app *application) listTransfersUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListTransfersInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	transfers, metadata, err := app.models.Transfers.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	paginationData := getPaginationData(metadata.CurrentPage, int(metadata.TotalRecords), metadata.PageSize, "transfers")

	app.render(w, r, "transfers.page.tmpl", &templateData{Transfers: transfers, Metadata: metadata, PaginationData: &paginationData})
}

func (app *application) createTransferFormUiHandler(w http.ResponseWriter, r *http.Request) {
	input, validationErrors := app.getListConnectionsInput(r)
	if !reflect.DeepEqual(validationErrors, map[string]string{}) {
		app.failedValidationResponse(w, r, validationErrors)
		return
	}

	connections, _, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.render(w, r, "create-transfer.page.tmpl", &templateData{Connections: connections, Form: forms.New(nil)})
}

func (app *application) cancelTransferUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	v := validator.New()
	transfer, err := app.models.Transfers.GetById(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			v.AddError("id", "not found")
			app.failedValidationResponse(w, r, v.Errors)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	if transfer.Status != "queued" && transfer.Status != "active" {
		v.AddError("status", fmt.Sprintf("cannot cancel a transfer with status of %s", transfer.Status))
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	transfer.Status = "cancelled"
	transfer.StoppedAt = time.Now()

	app.models.Transfers.Update(transfer)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrEditConflict):
			app.editConflictResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", "Transfer cancelled")
	http.Redirect(w, r, fmt.Sprintf("/ui/transfers/%d", id), http.StatusSeeOther)
}

func (app *application) showTransferUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil || id < 1 {
		app.notFoundResponse(w, r)
		return
	}

	transfer, err := app.models.Transfers.GetById(id)
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
		} else {
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.render(w, r, "transfer-detail.page.tmpl", &templateData{Transfer: transfer})
}

func (app *application) createTransferUiHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, "unable to parse create transfer form")
		return
	}

	var sourceId int64 = 0
	if r.PostForm.Get("sourceId") != "" {
		sourceId, err = strconv.ParseInt(r.PostForm.Get("sourceId"), 10, 64)
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, "non int value given to sourceId")
			return
		}
	}

	var targetId int64 = 0
	if r.PostForm.Get("targetId") != "" {
		targetId, err = strconv.ParseInt(r.PostForm.Get("targetId"), 10, 64)
		if err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, "non int value given to targetId")
			return
		}
	}

	transfer := &data.Transfer{
		CreatedBy:    app.getAuthenticatedUserId(r),
		SourceID:     sourceId,
		TargetID:     targetId,
		Query:        r.PostForm.Get("query"),
		TargetSchema: r.PostForm.Get("targetSchema"),
		TargetTable:  r.PostForm.Get("targetTable"),
		Overwrite:    r.PostForm.Get("overwrite") == "on",
	}

	form := forms.New(r.PostForm)

	input, _ := app.getListTransfersInput(r)
	connections, _, err := app.models.Connections.GetAll(input.Filters)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	if data.ValidateTransfer(form.Validator, transfer); !form.Validator.Valid() {
		app.render(w, r, "create-transfer.page.tmpl", &templateData{Connections: connections, Form: form})
		return
	}

	transfer, err = app.models.Transfers.Insert(transfer)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Transfer %d created", transfer.ID))
	http.Redirect(w, r, fmt.Sprintf("/ui/transfers/%d", transfer.ID), http.StatusSeeOther)
}

func (app *application) deleteTransferUiHandler(w http.ResponseWriter, r *http.Request) {
	id, err := app.readIDParam(r)
	if err != nil {
		app.notFoundResponse(w, r)
		return
	}

	err = app.models.Transfers.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, data.ErrRecordNotFound):
			app.notFoundResponse(w, r)
		default:
			app.serverErrorResponse(w, r, err)
		}
		return
	}

	app.session.Put(r, "flash", fmt.Sprintf("Transfer %d deleted", id))
	http.Redirect(w, r, "/ui/transfers", http.StatusSeeOther)
}
